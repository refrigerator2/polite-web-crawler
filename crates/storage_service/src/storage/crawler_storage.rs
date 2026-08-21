use crate::storage::{
    content_deduplicator::ContentDeduplicator, db::CrawlerDB, domain_cache::CachedData,
    domain_cache::DomainCache, seen_urls::SeenUrls,
};
use common::{
    error::crawler_error::CrawlerError,
    network::url_info::{DomainData, UrlAccess},
    parsers::parsed_data::{DomainDataSaveData, ParsedData, ParsedPageSaveData},
};
use std::path::Path;
use std::{sync::Arc, time::Duration};
use texting_robots::Robot;
use url::Url;

#[derive(Clone)]
pub struct CrawlerStorage {
    seen_urls: SeenUrls,
    db: CrawlerDB,
    domain_cache: DomainCache,
    pub agent_name: String,
    dedup: ContentDeduplicator,
}
impl CrawlerStorage {
    pub async fn new(db_name: &str, user_agent: String) -> Result<Self, CrawlerError> {
        let db_exists = Path::new(db_name).exists();
        let db = CrawlerDB::new(db_name).await?;
        let hashes = match db_exists {
            true => db.get_simhashes().await?,
            false => {
                vec![]
            }
        };
        Ok(Self {
            seen_urls: SeenUrls::new(),
            db: CrawlerDB::new(db_name).await?,
            domain_cache: DomainCache::new(Duration::from_secs(360), 20),
            agent_name: user_agent,
            dedup: ContentDeduplicator::init(hashes, 3),
        })
    }
    pub async fn save_parsed_data(&self, data: ParsedData) -> Result<Vec<String>, CrawlerError> {
        match data {
            ParsedData::ParsedPage(pp) => {
                let host = Url::parse(&pp.url).unwrap();

                let host = host
                    .domain()
                    .ok_or(CrawlerError::UrlDoesntContainDomain())?;

                let id = self
                    .get_domain_id(host)
                    .await?
                    .ok_or_else(|| CrawlerError::UrlDoesntContainDomain())?;
                self.save_parsed_page(id, &pp).await?;
                Ok(vec![])
            }
            ParsedData::ParsedDomain(dd) => {
                let res = self.save_domain(&dd).await?;
                Ok(res)
            }
        }
    }
    pub async fn get_domain_id(&self, domain: &str) -> Result<Option<i64>, CrawlerError> {
        let dom_id = match self.domain_cache.get_domain_id(domain).await {
            Some(id) => Some(id),
            None => {
                let temp = self
                    .db
                    .get_cache_info_about_domain(domain, self.agent_name.as_str())
                    .await?;
                if let Some(cd) = temp {
                    self.domain_cache.add_domain(domain, cd.clone()).await;
                    return Ok(Some(cd.id));
                }
                None
            }
        };
        Ok(dom_id)
    }
    pub async fn check_if_url_allowed(&self, url: &Url) -> Result<UrlAccess, CrawlerError> {
        let clone = url.clone();
        let domain = match clone.domain() {
            Some(d) => d,
            None => return Ok(UrlAccess::URLWithoutHost),
        };
        let cached_data = match self.domain_cache.get_cached_domain(domain).await {
            Some(cd) => cd,
            None => {
                let db_data = self
                    .db
                    .get_cache_info_about_domain(domain, self.agent_name.as_str())
                    .await?;

                match db_data {
                    Some(cd) => {
                        self.domain_cache.add_domain(domain, cd.clone()).await;
                        cd
                    }
                    None => return Ok(UrlAccess::UnknownDomain),
                }
            }
        };
        if let Some(ref robot) = cached_data.robot {
            if robot.allowed(url.path()) {
                return Ok(UrlAccess::Allowed);
            } else {
                return Ok(UrlAccess::Disallowed);
            }
        } else {
            return Ok(UrlAccess::Allowed);
        }
        unreachable!("Unreach in check_if_url_allowed")
    }
    pub fn insert_url_in_seen_urls(&self, url: &Url) -> bool {
        self.seen_urls.insert_url(url)
    }
    async fn save_parsed_page(
        &self,
        dom_id: i64,
        page: &ParsedPageSaveData,
    ) -> Result<(), CrawlerError> {
        if let Some(txt) = page.clean_text.clone() {
            if !self.dedup.is_duplicate(&txt) {
                let h = self.dedup.insert(&txt);
                self.db.save_parsed_page(dom_id, page, Some(h)).await?;
            }
        } else {
            self.db.save_parsed_page(dom_id, page, None).await?;
        }
        Ok(())
    }
    async fn save_domain(
        &self,
        domain_data: &DomainDataSaveData,
    ) -> Result<Vec<String>, CrawlerError> {
        let id = self.db.save_domain(domain_data).await?;
        self.domain_cache
            .add_domain(
                &domain_data.domain_string,
                CachedData::new(
                    id,
                    domain_data.robots.clone(),
                    domain_data.delay,
                    self.agent_name.as_str(),
                )?,
            )
            .await;
        Ok(self
            .domain_cache
            .get_sitemaps(&domain_data.domain_string)
            .await)
    }
    pub async fn get_delay(&self, domain: &str) -> Result<Duration, CrawlerError> {
        let delay = self.domain_cache.get_domain_delay(domain).await;
        match delay {
            Some(d) => return Ok(d),
            None => {
                let cd = self
                    .db
                    .get_cache_info_about_domain(domain, self.agent_name.as_str())
                    .await?;
                if let Some(cache) = cd {
                    let temp = cache.delay;
                    self.domain_cache.add_domain(domain, cache).await;
                    return Ok(temp);
                }
                return Ok(Duration::from_secs(1));
            }
        }
    }
}
