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
}
impl CrawlerStorage {
    pub async fn new(db_name: &str) -> Result<Self, CrawlerError> {
        Ok(Self {
            seen_urls: SeenUrls::new(),
            db: CrawlerDB::new(db_name).await?,
            domainc_cache: DomainCache::new(Duration::from_secs(360), 20),
        })
    }
    pub async fn get_domain_id(&self, domain: &str) -> Result<Option<i64>, CrawlerError> {
        let dom_id = match self.domainc_cache.get_domain_id(domain).await {
            Some(id) => Some(id),
            None => {
                let temp = self.db.get_cache_info_about_domain(domain).await?;
                if let Some(cd) = temp {
                    self.domainc_cache.add_domain(domain, cd.clone()).await;
                    return Ok(Some(cd.id));
                }
                None
            }
        };
        Ok(dom_id)
    }
    pub async fn check_if_url_allowed(
        &self,
        url: &Url,
        user_agent: &str,
    ) -> Result<UrlAccess, CrawlerError> {
        let clone = url.clone();
        let domain = match clone.domain() {
            Some(d) => d,
            None => return Ok(UrlAccess::URLWithoutHost),
        };
        let robot = match self.domainc_cache.get_domain_robot(domain).await {
            Some(rob) => Some(rob),
            None => {
                let temp = self.db.get_cache_info_about_domain(domain).await?;
                if temp.is_none() {
                    return Ok(UrlAccess::UnknownDomain);
                }
                let temp = temp.unwrap();
                self.domainc_cache.add_domain(domain, temp.clone());
                temp.robot
            }
        };
        match robot {
            Some(r) => {
                let path = url.path();
                let robot = Robot::new(user_agent, r.as_bytes())?;

                if robot.allowed(path) {
                    return Ok(UrlAccess::Allowed);
                } else {
                    return Ok(UrlAccess::Disallowed);
                }
            }
            None => return Ok(UrlAccess::Allowed),
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
                CachedData {
                    id,
                    robot: domain_data.robots.clone(),
                    delay: Duration::from_secs_f32(domain_data.delay),
                },
            )
            .await;
        Ok(id)
    }
    pub async fn get_delay(&self, domain: &str) -> Result<Duration, CrawlerError> {
        let delay = self.domainc_cache.get_domain_delay(domain).await;
        match delay {
            Some(d) => return Ok(d),
            None => {
                let cd = self.db.get_cache_info_about_domain(domain).await?;
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
