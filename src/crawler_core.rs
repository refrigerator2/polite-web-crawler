use crate::{
    crawler_error::CrawlerError,
    db::{CrawlerDB, UrlAccess},
    html_parser::ParsedPage,
    link_fetcher::LinkFetcher,
};
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::mpsc;
use url::Url;

const TUSKS_BUF_SIZE: usize = 1000;
const TOKIO_WORKERS: u8 = 8;
const DEFAULT_DB_NAME: &str = "sqlite://crawler.db/";
const AGENT_NAME: &str = "Aah";
const MAX_DB_RECONNECTS: usize = 3;

pub struct CrawlerCore {
    keywords: Arc<Option<Vec<String>>>,
    db: CrawlerDB,
    pages_crawled: Arc<AtomicUsize>,
    active_tasks: Arc<AtomicUsize>,
}

impl CrawlerCore {
    pub async fn new(keywords: Arc<Option<Vec<String>>>) -> Result<CrawlerCore, CrawlerError> {
        Ok(CrawlerCore {
            keywords,
            db: CrawlerDB::new(DEFAULT_DB_NAME).await?,
            pages_crawled: Arc::new(AtomicUsize::new(0)),
            active_tasks: Arc::new(AtomicUsize::new(0)),
        })
    }

    pub async fn run(&self, start_url: Url) -> Result<(), CrawlerError> {
        let (tx, rx) = mpsc::channel::<Url>(TUSKS_BUF_SIZE);
        let rx = Arc::new(tokio::sync::Mutex::new(rx));

        tx.send(start_url).await.unwrap();
        self.active_tasks.store(1, Ordering::SeqCst);
        let mut workers = vec![];

        for _ in 0..TOKIO_WORKERS {
            let rx_clone = Arc::clone(&rx);
            let tx_clone = tx.clone();
            let db_clone = self.db.clone();
            let keywords_clone = Arc::clone(&self.keywords);
            let counter_clone = Arc::clone(&self.pages_crawled);
            let active_tasks_clone = Arc::clone(&self.active_tasks);
            let handle = tokio::spawn(async move {
                Self::worker_loop(
                    rx_clone,
                    tx_clone,
                    db_clone,
                    keywords_clone,
                    counter_clone,
                    active_tasks_clone,
                )
                .await
            });
            workers.push(handle);
        }
        loop {
            tokio::time::sleep(Duration::from_millis(100)).await;
            if self.active_tasks.load(Ordering::SeqCst) == 0 {
                println!("Ended crawling");
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
    ) -> Result<(), CrawlerError> {
        loop {
            let mut rx_guard = rx.lock().await;
            let url = match rx_guard.recv().await {
                Some(u) => u,
                None => break,
            };
            drop(rx_guard);
            match Self::process_single_url(&url, &db, &keywords, &counter).await {
                Ok(outbound_links) => {
                    for next_url in outbound_links {
                        active_tasks.fetch_add(1, Ordering::SeqCst);
                        if tx.try_send(next_url).is_err() {
                            active_tasks.fetch_sub(1, Ordering::SeqCst);
                        }
                    }
                }
                Err(e) => {
                    eprintln!("Error while parsing {}: {:?}", url, e);
                }
            }
            active_tasks.fetch_sub(1, Ordering::SeqCst);
        }
        Ok(())
    }

    async fn process_single_url(
        url: &Url,
        db: &CrawlerDB,
        keywords: &Arc<Option<Vec<String>>>,
        counter: &Arc<AtomicUsize>,
    ) -> Result<Vec<Url>, CrawlerError> {
        match db.is_url_allowed(url, AGENT_NAME).await? {
            UrlAccess::UnknownDomain => {
                Self::handle_unknown_domain(url, db, keywords, counter).await
            }
            UrlAccess::Allowed => Self::handle_allowed_domain(url, db, keywords, counter).await,
            UrlAccess::Disallowed => Err(CrawlerError::NotAllowed()),
            UrlAccess::URLWithoutHost => Err(CrawlerError::UrlDoesntContainDomain()),
        }
    }
    async fn handle_unknown_domain(
        url: &Url,
        db: &CrawlerDB,
        keywords: &Arc<Option<Vec<String>>>,
        counter: &Arc<AtomicUsize>,
    ) -> Result<Vec<Url>, CrawlerError> {
        let mut link_fetcher = LinkFetcher::new(url.clone(), 0.0);

        let dom_data = link_fetcher.get_domain_data(AGENT_NAME).await?;
        let delay = dom_data.delay;

        db.save_domain(dom_data).await?;

        match db.is_url_allowed(url, AGENT_NAME).await? {
            UrlAccess::Allowed => {
                link_fetcher.delay = delay;
                Self::download_and_parse_page(link_fetcher, db, keywords, counter).await
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
    ) -> Result<Vec<Url>, CrawlerError> {
        let host = url.domain().ok_or(CrawlerError::UrlDoesntContainDomain())?;

        let delay = db.get_delay(host).await?.unwrap_or(0.0);

        let link_fetcher = LinkFetcher::new(url.clone(), delay);
        Self::download_and_parse_page(link_fetcher, db, keywords, counter).await
    }

    async fn download_and_parse_page(
        link_fetcher: LinkFetcher,
        db: &CrawlerDB,
        keywords: &Arc<Option<Vec<String>>>,
        counter: &Arc<AtomicUsize>,
    ) -> Result<Vec<Url>, CrawlerError> {
        let url_copy = link_fetcher.url.clone();

        let raw_html_data = link_fetcher.get_page_data().await?;

        let parsed_page = match ParsedPage::parse(raw_html_data, Arc::clone(keywords)) {
            Some(page) => page,
            None => return Ok(vec![]),
        };

        Self::save_results_to_db(&url_copy, &parsed_page, db, counter).await?;

        Ok(parsed_page.outbound_links)
    }

    async fn save_results_to_db(
        url: &Url,
        parsed_page: &ParsedPage,
        db: &CrawlerDB,
        counter: &Arc<AtomicUsize>,
    ) -> Result<(), CrawlerError> {
        let host = url.domain().ok_or(CrawlerError::UrlDoesntContainDomain())?;
        let dom_id = db.get_domain_id_by_domain_name(host).await?.unwrap();

        db.save_parsed_page(dom_id, parsed_page).await?;

        counter.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}
