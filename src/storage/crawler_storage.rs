use crate::crawler_error::CrawlerError;
use crate::html_parser::ParsedPage;
use crate::link_fetcher::DomainData;
use crate::storage::db::{CrawlerDB, UrlAccess};
use crate::storage::domain_cache::{CachedData, DomainCache};
use crate::storage::seen_urls::{self, SeenUrls};
use std::{sync::Arc, time::Duration};
use texting_robots::Robot;
use url::Url;

#[derive(Clone)]

pub struct CrawlerStorage {
    seen_urls: SeenUrls,
    db: CrawlerDB,
    domainc_cache: DomainCache,
    pub agent_name: String,
}
impl CrawlerStorage {
    pub async fn new(db_name: &str, user_agent: String) -> Result<Self, CrawlerError> {
        Ok(Self {
            seen_urls: SeenUrls::new(),
            db: CrawlerDB::new(db_name).await?,
            domainc_cache: DomainCache::new(Duration::from_secs(360), 20),
            agent_name: user_agent,
        })
    }
    pub async fn get_domain_id(&self, domain: &str) -> Result<Option<i64>, CrawlerError> {
        let dom_id = match self.domainc_cache.get_domain_id(domain).await {
            Some(id) => Some(id),
            None => {
                let temp = self
                    .db
                    .get_cache_info_about_domain(domain, self.agent_name.as_str())
                    .await?;
                if let Some(cd) = temp {
                    self.domainc_cache.add_domain(domain, cd.clone()).await;
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
        let cached_data = match self.domainc_cache.get_cached_domain(domain).await {
            Some(cd) => cd,
            None => {
                let db_data = self
                    .db
                    .get_cache_info_about_domain(domain, self.agent_name.as_str())
                    .await?;

                match db_data {
                    Some(cd) => {
                        self.domainc_cache.add_domain(domain, cd.clone()).await;
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
    pub async fn save_parsed_page(
        &self,
        dom_id: i64,
        page: &ParsedPage,
    ) -> Result<(), CrawlerError> {
        self.db.save_parsed_page(dom_id, page).await
    }
    pub async fn save_domain(&self, domain_data: &DomainData) -> Result<i64, CrawlerError> {
        let id = self.db.save_domain(domain_data).await?;
        self.domainc_cache
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
        Ok(id)
    }
    pub async fn get_delay(&self, domain: &str) -> Result<Duration, CrawlerError> {
        let delay = self.domainc_cache.get_domain_delay(domain).await;
        match delay {
            Some(d) => return Ok(d),
            None => {
                let cd = self
                    .db
                    .get_cache_info_about_domain(domain, self.agent_name.as_str())
                    .await?;
                if let Some(cache) = cd {
                    let temp = cache.delay;
                    self.domainc_cache.add_domain(domain, cache).await;
                    return Ok(temp);
                }
                return Ok(Duration::from_secs(1));
            }
        }
    }
}
