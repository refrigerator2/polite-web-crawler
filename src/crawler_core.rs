use crate::{
    crawler_error::CrawlerError,
    db::{CrawlerDB, UrlAccess},
    domain_cache::{self, DomainCache},
    domain_rate_limiter::DomainRateLimiter,
    html_parser::ParsedPage,
    link_fetcher::LinkFetcher,
};
use fastbloom::BloomFilter;
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::mpsc;
use url::Url;
const EXPECTED_NUM_OF_URLS: usize = 10_000_000;

pub struct TaskGuard(Arc<AtomicUsize>);
impl Drop for TaskGuard {
    fn drop(&mut self) {
        &self.0.fetch_sub(1, Ordering::SeqCst);
    }
}
pub struct CrawlerCore {
    keywords: Arc<Option<Vec<String>>>,
    db: CrawlerDB,
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
            db: CrawlerDB::new(&db_name).await?,
            tokio_workers,
            agent_name,
            pages_crawled: Arc::new(AtomicUsize::new(0)),
            active_tasks: Arc::new(AtomicUsize::new(0)),
        })
    }

    pub async fn run(&self, start_url: Url) -> Result<(), CrawlerError> {
        let (tx, rx) = mpsc::channel::<Url>(self.tokio_workers * 500);
        let rx = Arc::new(tokio::sync::Mutex::new(rx));
        let seen_urls = Arc::new(Mutex::new(
            BloomFilter::with_false_pos(0.001).expected_items(EXPECTED_NUM_OF_URLS),
        ));
        tx.send(start_url.clone()).await.unwrap();
        self.active_tasks.store(1, Ordering::SeqCst);
        self.pages_crawled.store(0, Ordering::SeqCst);
        let mut guard = seen_urls.lock().unwrap();
        guard.insert(&start_url);
        drop(guard);

        let mut workers = vec![];
        let domain_cache = DomainCache::new(Duration::from_secs(360), 20);
        for _ in 0..self.tokio_workers {
            let rx_clone = Arc::clone(&rx);
            let tx_clone = tx.clone();
            let db_clone = self.db.clone();
            let keywords_clone = Arc::clone(&self.keywords);
            let counter_clone = Arc::clone(&self.pages_crawled);
            let active_tasks_clone = Arc::clone(&self.active_tasks);
            let seen_urls_clone = Arc::clone(&seen_urls);
            let drl = DomainRateLimiter::new(Duration::from_secs(1));
            let agent_name_clone = self.agent_name.clone();
            let domain_cache_clone = domain_cache.clone();

            let handle = tokio::spawn(async move {
                Self::worker_loop(
                    rx_clone,
                    tx_clone,
                    db_clone,
                    keywords_clone,
                    counter_clone,
                    active_tasks_clone,
                    &agent_name_clone,
                    seen_urls_clone,
                    drl.clone(),
                    domain_cache_clone,
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
        }
        for worker in workers {
            worker.abort();
        }
        Ok(())
    }

    async fn worker_loop(
        rx: Arc<tokio::sync::Mutex<mpsc::Receiver<Url>>>,
        tx: mpsc::Sender<Url>,
        db: CrawlerDB,
        keywords: Arc<Option<Vec<String>>>,
        counter: Arc<AtomicUsize>,
        active_tasks: Arc<AtomicUsize>,
        agent_name: &str,
        seen_urls: Arc<Mutex<BloomFilter>>,
        drl: DomainRateLimiter,
        domain_cache: DomainCache,
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
            match Self::process_single_url(
                &url,
                &db,
                &keywords,
                &counter,
                agent_name,
                drl.clone(),
                &domain_cache,
            )
            .await
            {
                Ok(mut outbound_links) => {
                    outbound_links.sort();
                    outbound_links.dedup();
                    let mut filtered_urls_by_seen = Vec::new();

                    for url in outbound_links {
                        let is_in_list = seen_urls.lock().unwrap().insert(&url);
                        if !is_in_list {
                            filtered_urls_by_seen.push(url);
                        }
                    }

                    let mut filtered_urls = Vec::new();
                    for url in filtered_urls_by_seen {
                        if let Ok(false) =
                            db.check_if_url_has_already_been_parsed(url.as_str()).await
                        {
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
        db: &CrawlerDB,
        keywords: &Arc<Option<Vec<String>>>,
        counter: &Arc<AtomicUsize>,
        agent_name: &str,
        drl: DomainRateLimiter,
        domain_cache: &DomainCache,
    ) -> Result<Vec<Url>, CrawlerError> {
        if let Some(rob) = domain_cache.get_domain_robot(url.domain().unwrap()).await {
            //gonna refactor my code real quick
        }
        match db.is_url_allowed(url, agent_name).await? {
            UrlAccess::UnknownDomain => {
                Self::handle_unknown_domain(
                    url,
                    db,
                    keywords,
                    counter,
                    agent_name,
                    drl,
                    domain_cache,
                )
                .await
            }
            UrlAccess::Allowed => {
                Self::handle_allowed_domain(url, db, keywords, counter, domain_cache).await
            }
            UrlAccess::Disallowed => Err(CrawlerError::NotAllowed()),
            UrlAccess::URLWithoutHost => Err(CrawlerError::UrlDoesntContainDomain()),
        }
    }
    async fn handle_unknown_domain(
        url: &Url,
        db: &CrawlerDB,
        keywords: &Arc<Option<Vec<String>>>,
        counter: &Arc<AtomicUsize>,
        agent_name: &str,
        drl: DomainRateLimiter,
        domain_cache: &DomainCache,
    ) -> Result<Vec<Url>, CrawlerError> {
        let mut link_fetcher = LinkFetcher::new(url.clone(), 0.0);

        let dom_data = link_fetcher.get_domain_data(agent_name).await?;
        let delay = dom_data.delay;
        drl.update_delay(&dom_data.domain_string, Duration::from_secs_f32(delay));
        let id = db.save_domain(&dom_data).await?;
        domain_cache
            .add_domain(
                &dom_data.domain_string,
                id,
                dom_data.robots.unwrap_or(String::from("")),
            )
            .await;
        match db.is_url_allowed(url, agent_name).await? {
            UrlAccess::Allowed => {
                link_fetcher.delay = delay;
                while !drl.try_acquire(&dom_data.domain_string) {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                Self::download_and_parse_page(link_fetcher, db, keywords, counter, domain_cache)
                    .await
            }
            UrlAccess::Disallowed => Err(CrawlerError::NotAllowed()),
            _ => Err(CrawlerError::UrlDoesntContainDomain()),
        }
    }

    async fn handle_allowed_domain(
        url: &Url,
        db: &CrawlerDB,
        keywords: &Arc<Option<Vec<String>>>,
        counter: &Arc<AtomicUsize>,
        domain_cache: &DomainCache,
    ) -> Result<Vec<Url>, CrawlerError> {
        let host = url.domain().ok_or(CrawlerError::UrlDoesntContainDomain())?;

        let delay = db.get_delay(host).await?.unwrap_or(0.0);

        let link_fetcher = LinkFetcher::new(url.clone(), delay);
        Self::download_and_parse_page(link_fetcher, db, keywords, counter, domain_cache).await
    }

    async fn download_and_parse_page(
        link_fetcher: LinkFetcher,
        db: &CrawlerDB,
        keywords: &Arc<Option<Vec<String>>>,
        counter: &Arc<AtomicUsize>,
        domain_cache: &DomainCache,
    ) -> Result<Vec<Url>, CrawlerError> {
        let url_copy = link_fetcher.url.clone();

        let raw_html_data = link_fetcher.get_page_data().await?;

        let parsed_page = ParsedPage::parse(raw_html_data, Arc::clone(keywords));

        if parsed_page.keywords_in_it {
            Self::save_results_to_db(&url_copy, &parsed_page, db, counter, domain_cache).await?;
        }
        Ok(parsed_page.outbound_links)
    }

    async fn save_results_to_db(
        url: &Url,
        parsed_page: &ParsedPage,
        db: &CrawlerDB,
        counter: &Arc<AtomicUsize>,
        domain_cache: &DomainCache,
    ) -> Result<(), CrawlerError> {
        let host = url.domain().ok_or(CrawlerError::UrlDoesntContainDomain())?;

        let dom_id = match domain_cache.get_domain_id(host).await {
            Some(id) => id,
            None => db.get_domain_id_by_domain_name(host).await?.unwrap(),
        };

        db.save_parsed_page(dom_id, parsed_page).await?;

        counter.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}
