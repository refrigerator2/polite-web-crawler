use crate::storage_client::client::StorageClient;
use crate::{
    network::{domain_rate_limiter::DomainRateLimiter, link_fetcher::LinkFetcher},
    parsers::{html_parser::ParsedPage, sitemaps_parser::SitemapsParser},
};
use common::DEFAULT_AGENT_NAME;
use common::{
    CRAWLER_TASK_QUEUE_NAME, error::crawler_error::CrawlerError, network::url_info::UrlAccess,
    task_queue::task_queue::TaskQueue,
};
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
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
    limit: Option<u64>,
    pages_crawled: Arc<AtomicUsize>,
    active_tasks: Arc<AtomicUsize>,
}

impl CrawlerCore {
    pub async fn new(
        keywords: Arc<Option<Vec<String>>>,
        db_name: String,
        tokio_workers: usize,
        agent_name: String,
        limit: Option<u64>,
    ) -> Result<CrawlerCore, CrawlerError> {
        Ok(CrawlerCore {
            keywords,
            db_name,
            tokio_workers,
            agent_name,
            limit,
            pages_crawled: Arc::new(AtomicUsize::new(0)),
            active_tasks: Arc::new(AtomicUsize::new(0)),
        })
    }

    pub async fn run(
        &self,
        start_url: Url,
        shutdown: CancellationToken,
    ) -> Result<(), CrawlerError> {
        let redis_addr =
            std::env::var("REDIS_ADDR").unwrap_or("redis://127.0.0.1:6379".to_string());
        let task_queue = TaskQueue::new(&redis_addr, CRAWLER_TASK_QUEUE_NAME, 5.0).await?;

        let storage_grpc_addr =
            std::env::var("STORAGE_GRPC_ADDR").unwrap_or("http://127.0.0.1:50051".to_string());
        let storage = StorageClient::new(storage_grpc_addr, redis_addr).await?;

        task_queue.push(start_url.as_str()).await?;
        self.active_tasks.store(1, Ordering::SeqCst);
        self.pages_crawled.store(0, Ordering::SeqCst);
        let first_url_insert = storage.insert_url(start_url.as_str()).await;
        if let Err(e) = first_url_insert {
            eprintln!("{e}");
            return Err(e);
        }
        let mut workers = vec![];

        for _ in 0..self.tokio_workers {
            let keywords_clone = Arc::clone(&self.keywords);
            let counter_clone = Arc::clone(&self.pages_crawled);
            let active_tasks_clone = Arc::clone(&self.active_tasks);
            let drl = DomainRateLimiter::new(Duration::from_secs(1));
            let storage_clone = storage.clone();
            let task_queue_clone = task_queue.clone();
            let shutdown_clone = shutdown.clone();
            let handle = tokio::spawn(async move {
                Self::worker_loop(
                    task_queue_clone,
                    keywords_clone,
                    counter_clone,
                    active_tasks_clone,
                    drl.clone(),
                    storage_clone,
                    shutdown_clone,
                )
                .await
            });
            workers.push(handle);
        }
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    println!("Shutdowning workers...");
                    break;
                }
                _ = tokio::time::sleep(Duration::from_millis(50)) => {
            if self.active_tasks.load(Ordering::SeqCst) == 0
                || self
                    .limit
                    .is_some_and(|l| l as usize <= self.pages_crawled.load(Ordering::SeqCst))
            {
                println!(
                    "Ended crawling, urls processed: {}",
                    self.pages_crawled.load(Ordering::SeqCst)
                );
                shutdown.cancel();
                break;
            }
            println!("PARSED: {}", self.pages_crawled.load(Ordering::SeqCst));
            }
            }
        }
        for worker in workers {
            if let Err(e) = worker.await {
                eprintln!("Worker finished his tasks with error: {e}");
            }
        }
        println!("All workers finished their tasks");
        Ok(())
    }

    async fn worker_loop(
        task_queue: TaskQueue,
        keywords: Arc<Option<Vec<String>>>,
        counter: Arc<AtomicUsize>,
        active_tasks: Arc<AtomicUsize>,
        drl: DomainRateLimiter,
        storage: StorageClient,
        shutdown: CancellationToken,
    ) -> Result<(), CrawlerError> {
        loop {
            if shutdown.is_cancelled() {
                break;
            }
            let url = match task_queue.pop_front().await? {
                Some(u) => u,
                None => break,
            };
            let domain = match url.domain() {
                Some(d) => d.to_string(),
                None => {
                    println!("Tried to acquire url without domain: {}", url);
                    active_tasks.fetch_sub(1, Ordering::SeqCst);
                    continue;
                }
            };

            if !drl.try_acquire(&domain) {
                if task_queue.push(url.as_str()).await.is_err() {
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
                    for u in outbound_links {
                        match storage.insert_url(u.as_str()).await {
                            Ok(is_duplicate) => {
                                if !is_duplicate {
                                    filtered_urls.push(u.to_string());
                                }
                            }
                            Err(e) => {
                                eprintln!("Failed to check insert_url for {}: {:?}", u, e);
                            }
                        }
                    }
                    if filtered_urls.is_empty() {
                        continue;
                    }
                    for i in 1..=3 {
                        match task_queue.push_bulk(filtered_urls.as_slice()).await {
                            Ok(added_count) => {
                                active_tasks.fetch_add(added_count, Ordering::SeqCst);
                                break;
                            }
                            Err(e) => {
                                eprintln!("Error pushing bulk (attempt {}/3): {}", i, e);
                                if i == 3 {
                                    return Err(e);
                                }
                                tokio::time::sleep(std::time::Duration::from_millis(
                                    200 * i as u64,
                                ))
                                .await;
                            }
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
        storage: StorageClient,
    ) -> Result<Vec<Url>, CrawlerError> {
        match storage.check_if_url_allowed(url.as_str()).await? {
            UrlAccess::UnknownDomain => {
                Self::handle_unknown_domain(url, drl, storage.clone()).await
            }
            UrlAccess::Allowed => {
                let path = url.path().to_lowercase();
                if path.ends_with(".xml") || path.ends_with(".xml.gz") {
                    let domain = url.domain().ok_or(CrawlerError::UrlDoesntContainDomain())?;

                    drl.await_acquiring(domain).await;

                    let delay = storage.get_delay(domain).await?;
                    let sitemaps_parser =
                        SitemapsParser::new(vec![url.as_str().to_string()], delay);

                    return Ok(sitemaps_parser.parse().await);
                }

                Self::handle_allowed_domain(url, keywords, counter, storage.clone()).await
            }
            UrlAccess::Disallowed => Err(CrawlerError::NotAllowed()),
            UrlAccess::URLWithoutHost => Err(CrawlerError::UrlDoesntContainDomain()),
        }
    }
    async fn handle_unknown_domain(
        url: &Url,
        drl: DomainRateLimiter,
        storage: StorageClient,
    ) -> Result<Vec<Url>, CrawlerError> {
        let link_fetcher = LinkFetcher::new(url.clone(), 0.0);

        let dom_data = link_fetcher.get_domain_data(DEFAULT_AGENT_NAME).await?;
        let delay = dom_data.delay;
        drl.update_delay(&dom_data.domain_string, Duration::from_secs_f32(delay));
        storage.save_domain_data(&dom_data).await?;
        Ok(vec![])
    }

    async fn handle_allowed_domain(
        url: &Url,
        keywords: &Arc<Option<Vec<String>>>,
        counter: &Arc<AtomicUsize>,
        storage: StorageClient,
    ) -> Result<Vec<Url>, CrawlerError> {
        let host = url.domain().ok_or(CrawlerError::UrlDoesntContainDomain())?;

        let delay = storage.get_delay(host).await?;

        let link_fetcher = LinkFetcher::new(url.clone(), delay);
        Self::download_and_parse_link(link_fetcher, keywords, counter, storage.clone()).await
    }

    async fn download_and_parse_link(
        link_fetcher: LinkFetcher,
        keywords: &Arc<Option<Vec<String>>>,
        counter: &Arc<AtomicUsize>,
        storage: StorageClient,
    ) -> Result<Vec<Url>, CrawlerError> {
        let raw_html_data = link_fetcher.get_page().await?;

        let (keywords_in_it, parsed_page) = ParsedPage::parse(raw_html_data, Arc::clone(keywords));

        if keywords_in_it {
            Self::save_results_to_db(&parsed_page, counter, storage.clone()).await?;
        }
        Ok(parsed_page.outbound_links)
    }

    async fn save_results_to_db(
        parsed_page: &ParsedPage,
        counter: &Arc<AtomicUsize>,
        storage: StorageClient,
    ) -> Result<(), CrawlerError> {
        storage.save_parsed_page(parsed_page).await?;
        counter.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}
