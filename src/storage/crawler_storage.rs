use std::time::Duration;

use crate::crawler_error::CrawlerError;
use crate::storage::db::CrawlerDB;
use crate::storage::domain_cache::DomainCache;
use crate::storage::seen_urls::{self, SeenUrls};

struct CrawlerStorage {
    seen_urls: SeenUrls,
    db: CrawlerDB,
    domainc_cache: DomainCache,
}
impl CrawlerStorage {
    pub async fn new(db_name: &str) -> Result<Self, CrawlerError> {
        Ok(Self {
            seen_urls: SeenUrls::new(),
            db: CrawlerDB::new(db_name).await?,
            domainc_cache: DomainCache::new(Duration::from_secs(360), 20),
        })
    }
}
