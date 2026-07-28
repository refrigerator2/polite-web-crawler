use crate::{
    crawler_error::CrawlerError,
    domain_rate_limiter::DomainRateLimiter,
    html_parser::ParsedPage,
    link_fetcher::LinkFetcher,
    storage::crawler_storage::CrawlerStorage,
    storage::db::{CrawlerDB, UrlAccess},
};
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::mpsc;
use url::Url;

pub struct TaskGuard(Arc<AtomicUsize>);
impl Drop for TaskGuard {
    fn drop(&mut self) {
        &self.0.fetch_sub(1, Ordering::SeqCst);
    }
}
pub struct CrawlerCore {
    keywords: Arc<Option<Vec<String>>>,
    db_name: String,
    tokio_workers: usize,
    agent_name: String,
    pages_crawled: Arc<AtomicUsize>,
    active_tasks: Arc<AtomicUsize>,
}

impl CrawlerCore {
    pub async fn new(
        keywords: Arc<Option<Vec<String>>>,
        db_name: String,
        tokio_workers: usize,
        agent_name: String,
    ) -> Result<CrawlerCore, CrawlerError> {
        Ok(CrawlerCore {
            keywords,
            db_name,
            tokio_workers,
            agent_name,
            pages_crawled: Arc::new(AtomicUsize::new(0)),
            active_tasks: Arc::new(AtomicUsize::new(0)),
        })
    }

    pub async fn run(&self, start_url: Url) -> Result<(), CrawlerError> {
        let (tx, rx) = mpsc::channel::<Url>(self.tokio_workers * 500);
        let rx = Arc::new(tokio::sync::Mutex::new(rx));

        let storage = CrawlerStorage::new(self.db_name.as_str(), self.agent_name.clone()).await?;

        tx.send(start_url.clone()).await.unwrap();
        self.active_tasks.store(1, Ordering::SeqCst);
        self.pages_crawled.store(0, Ordering::SeqCst);
        storage.insert_url_in_seen_urls(&start_url);
        let mut workers = vec![];

        for _ in 0..self.tokio_workers {
            let rx_clone = Arc::clone(&rx);
            let tx_clone = tx.clone();
            let keywords_clone = Arc::clone(&self.keywords);
            let counter_clone = Arc::clone(&self.pages_crawled);
            let active_tasks_clone = Arc::clone(&self.active_tasks);
            let drl = DomainRateLimiter::new(Duration::from_secs(1));
            let storage_clone = storage.clone();

            let handle = tokio::spawn(async move {
                Self::worker_loop(
                    rx_clone,
                    tx_clone,
                    keywords_clone,
                    counter_clone,
                    active_tasks_clone,
                    drl.clone(),
                    storage_clone,
                )
                .await
            });
            workers.push(handle);
        }
        loop {
            tokio::time::sleep(Duration::from_millis(100)).await;
            if self.active_tasks.load(Ordering::SeqCst) == 0 {
                println!(
                    "Ended crawling, urls processed: {}",
                    self.pages_crawled.load(Ordering::SeqCst)
                );
                break;
            }
            println!("PARSED: {}", self.pages_crawled.load(Ordering::SeqCst));
        }
        for worker in workers {
            worker.abort();
        }
        Ok(())
    }

    async fn worker_loop(
        rx: Arc<tokio::sync::Mutex<mpsc::Receiver<Url>>>,
        tx: mpsc::Sender<Url>,
        keywords: Arc<Option<Vec<String>>>,
        counter: Arc<AtomicUsize>,
        active_tasks: Arc<AtomicUsize>,
        drl: DomainRateLimiter,
        storage: CrawlerStorage,
    ) -> Result<(), CrawlerError> {
        loop {
            let mut rx_guard = rx.lock().await;
            let url = match rx_guard.recv().await {
                Some(u) => u,
                None => break,
            };
            drop(rx_guard);
            let domain = match url.domain() {
                Some(d) => d.to_string(),
                None => {
                    println!("Tried to acquire url without domain: {}", url);
                    active_tasks.fetch_sub(1, Ordering::SeqCst);
                    continue;
                }
            };

            if !drl.try_acquire(&domain) {
                if tx.try_send(url).is_err() {
                    eprintln!("Failed to re-queue URL: channel full or closed");
                    active_tasks.fetch_sub(1, Ordering::SeqCst);
                }
                continue;
            }
            let _task_guard = TaskGuard(Arc::clone(&active_tasks));
            match Self::process_single_url(&url, &keywords, &counter, drl.clone(), storage.clone())
                .await
            {
                Ok(mut outbound_links) => {
                    outbound_links.sort();
                    outbound_links.dedup();
                    let mut filtered_urls = Vec::new();

                    for url in outbound_links {
                        let is_in_list = storage.insert_url_in_seen_urls(&url);
                        if !is_in_list {
                            filtered_urls.push(url);
                        }
                    }

                    for next_url in filtered_urls {
                        active_tasks.fetch_add(1, Ordering::SeqCst);
                        if tx.try_send(next_url).is_err() {
                            eprintln!("buffer overflow");
                            active_tasks.fetch_sub(1, Ordering::SeqCst);
                        }
                    }
                }
                Err(e) => {
                    eprintln!("Error while parsing {}: {:?}", url, e);
                }
            }
            println!("active_tasks: {}", active_tasks.load(Ordering::SeqCst));
        }
        Ok(())
    }

    async fn process_single_url(
        url: &Url,
        keywords: &Arc<Option<Vec<String>>>,
        counter: &Arc<AtomicUsize>,
        drl: DomainRateLimiter,
        storage: CrawlerStorage,
    ) -> Result<Vec<Url>, CrawlerError> {
        match storage.check_if_url_allowed(url).await? {
            UrlAccess::UnknownDomain => {
                Self::handle_unknown_domain(url, keywords, counter, drl, storage.clone()).await
            }
            UrlAccess::Allowed => {
                Self::handle_allowed_domain(url, keywords, counter, storage.clone()).await
            }
            UrlAccess::Disallowed => Err(CrawlerError::NotAllowed()),
            UrlAccess::URLWithoutHost => Err(CrawlerError::UrlDoesntContainDomain()),
        }
    }
    async fn handle_unknown_domain(
        url: &Url,
        keywords: &Arc<Option<Vec<String>>>,
        counter: &Arc<AtomicUsize>,
        drl: DomainRateLimiter,
        storage: CrawlerStorage,
    ) -> Result<Vec<Url>, CrawlerError> {
        let mut link_fetcher = LinkFetcher::new(url.clone(), 0.0);

        let dom_data = link_fetcher
            .get_domain_data(storage.agent_name.as_str())
            .await?;
        let delay = dom_data.delay;
        drl.update_delay(&dom_data.domain_string, Duration::from_secs_f32(delay));
        let id = storage.save_domain(&dom_data).await?;
        match storage.check_if_url_allowed(url).await? {
            UrlAccess::Allowed => {
                link_fetcher.delay = delay;
                while !drl.try_acquire(&dom_data.domain_string) {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                Self::download_and_parse_page(link_fetcher, keywords, counter, storage.clone())
                    .await
            }
            UrlAccess::Disallowed => Err(CrawlerError::NotAllowed()),
            _ => Err(CrawlerError::UrlDoesntContainDomain()),
        }
    }

    async fn handle_allowed_domain(
        url: &Url,
        keywords: &Arc<Option<Vec<String>>>,
        counter: &Arc<AtomicUsize>,
        storage: CrawlerStorage,
    ) -> Result<Vec<Url>, CrawlerError> {
        let host = url.domain().ok_or(CrawlerError::UrlDoesntContainDomain())?;

        let delay = storage.get_delay(host).await?;

        let link_fetcher = LinkFetcher::new(url.clone(), delay.as_secs_f32());
        Self::download_and_parse_page(link_fetcher, keywords, counter, storage.clone()).await
    }

    async fn download_and_parse_page(
        link_fetcher: LinkFetcher,
        keywords: &Arc<Option<Vec<String>>>,
        counter: &Arc<AtomicUsize>,
        storage: CrawlerStorage,
    ) -> Result<Vec<Url>, CrawlerError> {
        let url_copy = link_fetcher.url.clone();

        let raw_html_data = link_fetcher.get_page_data().await?;

        let parsed_page = ParsedPage::parse(raw_html_data, Arc::clone(keywords));

        if parsed_page.keywords_in_it {
            Self::save_results_to_db(&url_copy, &parsed_page, counter, storage.clone()).await?;
        }
        Ok(parsed_page.outbound_links)
    }

    async fn save_results_to_db(
        url: &Url,
        parsed_page: &ParsedPage,
        counter: &Arc<AtomicUsize>,
        storage: CrawlerStorage,
    ) -> Result<(), CrawlerError> {
        let host = url.domain().ok_or(CrawlerError::UrlDoesntContainDomain())?;

        let id = storage
            .get_domain_id(host)
            .await?
            .ok_or_else(|| CrawlerError::UrlDoesntContainDomain())?;
        storage.save_parsed_page(id, parsed_page).await?;
        counter.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}
