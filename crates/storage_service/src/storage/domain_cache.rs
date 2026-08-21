use common::error::crawler_error::CrawlerError;
use moka::future::Cache;
use std::sync::Arc;
use std::time::Duration;
use texting_robots::Robot;

#[derive(Clone, Debug)]
pub struct CachedData {
    pub id: i64,
    pub robot: Option<Arc<Robot>>,
    pub delay: Duration,
}
impl CachedData {
    pub fn new(
        id: i64,
        robot: Option<Arc<String>>,
        delay: f32,
        agent: &str,
    ) -> Result<CachedData, CrawlerError> {
        let robot = if let Some(r) = robot {
            let robot_matcher = Robot::new(agent, r.as_bytes())?;
            Some(Arc::new(robot_matcher))
        } else {
            None
        };
        Ok(CachedData {
            id,
            robot,
            delay: Duration::from_secs_f32(delay),
        })
    }
}
#[derive(Clone)]
pub struct DomainCache {
    cache: Cache<String, CachedData>,
}

impl DomainCache {
    pub fn new(tti: Duration, init_cap: usize) -> Self {
        Self {
            cache: Cache::builder()
                .time_to_idle(tti)
                .initial_capacity(init_cap)
                .build(),
        }
    }

    pub async fn add_domain(&self, domain: &str, cd: CachedData) {
        self.cache.insert(domain.to_string(), cd).await
    }

    pub async fn get_domain_id(&self, domain: &str) -> Option<i64> {
        match self.cache.get(domain).await {
            Some(cd) => Some(cd.id),
            None => None,
        }
    }
    pub async fn get_domain_robot(&self, domain: &str) -> Option<Arc<Robot>> {
        self.cache.get(domain).await.and_then(|cd| cd.robot)
    }
    pub async fn get_domain_delay(&self, domain: &str) -> Option<Duration> {
        match self.cache.get(domain).await {
            Some(cd) => Some(cd.delay),
            None => None,
        }
    }
    pub async fn get_sitemaps(&self, domain: &str) -> Vec<String> {
        self.cache
            .get(domain)
            .await
            .and_then(|cd| cd.robot)
            .as_deref()
            .map(|r| r.sitemaps.clone())
            .unwrap_or(Vec::new())
    }
    pub async fn get_cached_domain(&self, domain: &str) -> Option<CachedData> {
        self.cache.get(domain).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_domain_cache_lifecycle() -> Result<(), CrawlerError> {
        let cache = DomainCache::new(Duration::from_millis(100), 10);

        assert_eq!(cache.get_domain_id("smt").await, None);
        assert_eq!(cache.get_domain_delay("smt").await, None);

        let raw_robots = Arc::new("User-agent: *\nDisallow: /admin".to_string());
        let cd_1 = CachedData::new(67, Some(raw_robots), 1.5, "MyBot")?;

        cache.add_domain("new_domain", cd_1).await;

        assert_eq!(cache.get_domain_id("new_domain").await, Some(67));
        assert_eq!(
            cache.get_domain_delay("new_domain").await,
            Some(Duration::from_secs_f32(1.5))
        );

        let robot = cache.get_domain_robot("new_domain").await;
        assert!(robot.is_some());
        assert!(!robot.unwrap().allowed("https://example.com/admin"));

        let cd_2 = CachedData::new(100, None, 0.5, "MyBot")?;
        cache.add_domain("no_robots_domain", cd_2).await;

        let robot = cache.get_domain_robot("new_domain").await;
        assert!(robot.is_some());
        assert!(cache.get_domain_robot("no_robots_domain").await.is_none());

        tokio::time::sleep(Duration::from_millis(150)).await;
        assert_eq!(cache.get_domain_id("new_domain").await, None);

        Ok(())
    }
}
