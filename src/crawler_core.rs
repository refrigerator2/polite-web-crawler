use crate::{
    crawler_error::CrawlerError,
    db::{CrawlerDB, UrlAccess},
    html_parser::ParsedPage,
    link_fetcher::LinkFetcher,
};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::mpsc;
use url::Url;

const TUSKS_BUF_SIZE: usize = 1000;
const TOKIO_WORKERS: u8 = 8;
const DEFAULT_DB_NAME: &str = "sqlite://crawler.db/";
const AGENT_NAME: &str = "Aah";
pub struct CrawlerCore {
    keywords: Arc<Option<Vec<String>>>,
    db: CrawlerDB,
}

impl CrawlerCore {
    pub async fn new(keywords: Arc<Option<Vec<String>>>) -> Result<CrawlerCore, CrawlerError> {
        let temp = CrawlerCore {
            keywords,
            db: CrawlerDB::new(DEFAULT_DB_NAME).await?,
        };
        Ok(temp)
    }
    pub async fn run(&self, start_url: Url) -> Result<(), CrawlerError> {
        let (tx, rx) = mpsc::channel::<Url>(TUSKS_BUF_SIZE);

        let rx = Arc::new(tokio::sync::Mutex::new(rx));

        tx.send(start_url).await.unwrap();

        let db = self.db.clone();
        let mut workers = vec![];
        for _ in 0..TOKIO_WORKERS {
            let rx_clone = Arc::clone(&rx);
            let tx_clone = tx.clone();
            let db_clone = db.clone();

            let handle: tokio::task::JoinHandle<Result<(), CrawlerError>> =
                tokio::spawn(async move {
                    loop {
                        let mut rx_guard = rx_clone.lock().await;

                        if let Some(u) = rx_guard.recv().await {
                            drop(rx_guard);
                            let mut link_fetcher = LinkFetcher::new(u.clone());
                            let if_allowed_res = db_clone.is_url_allowed(&u, AGENT_NAME).await?;
                            match if_allowed_res {
                                UrlAccess::UnknownDomain => {}
                                UrlAccess::Allowed => {}
                                UrlAccess::Disallowed => {}
                                UrlAccess::URLWithoutHost => {}
                            }
                        } else {
                            break;
                        }
                    }
                    Ok(())
                });
            workers.push(handle);
        }
        for worker in workers {
            let _ = worker.await;
        }
        Ok(())
    }
    async fn unknown_domain_case(&self, mut link_fetcher: LinkFetcher) -> Result<(), CrawlerError> {
        let dom_data = link_fetcher.get_domain_data(AGENT_NAME).await?;
        let delay_copy = dom_data.delay;
        self.db.save_domain(dom_data).await?;
        let if_allowed_res_again = self
            .db
            .is_url_allowed(&link_fetcher.url, AGENT_NAME)
            .await?;
        match if_allowed_res_again {
            UrlAccess::UnknownDomain => {
                panic!("Some issues with db");
            }
            UrlAccess::Allowed => {
                tokio::time::sleep(Duration::from_secs_f32(delay_copy)).await;
            }
            UrlAccess::Disallowed => {}
            UrlAccess::URLWithoutHost => {}
        }
        Ok(())
    }
    async fn allowed_case(&self, mut link_fetcher: LinkFetcher) -> Result<Vec<Url>, CrawlerError> {
        let url_copy = link_fetcher.url.clone();
        let not_parsed_data = link_fetcher.get_page_data().await?;
        let parsed_struct = ParsedPage::parse(not_parsed_data, Arc::clone(&self.keywords));
        if parsed_struct.is_none() {
            return Ok(vec![]);
        }
        let parsed_struct = parsed_struct.unwrap();
        let dom_id = self
            .db
            .get_domain_id_by_domain_name(url_copy.domain().unwrap())
            .await?;
        if dom_id.is_none() {
            panic!("Something went wrong with db");
        }
        let dom_id = dom_id.unwrap();
        self.db.save_parsed_page(dom_id, &parsed_struct).await?;
        Ok(parsed_struct.outbound_links)
    }
}
