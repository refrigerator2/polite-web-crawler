use moka::future::Cache;
use std::sync::Arc;
use std::time::Duration;

#[derive(Clone, Debug, PartialEq)]
pub struct CachedData {
    pub id: i64,
    pub robot: Option<Arc<String>>,
    pub delay: Duration,
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
    pub async fn get_domain_robot(&self, domain: &str) -> Option<Arc<String>> {
        self.cache.get(domain).await.and_then(|cd| cd.robot)
    }
    pub async fn get_domain_delay(&self, domain: &str) -> Option<Duration> {
        match self.cache.get(domain).await {
            Some(cd) => Some(cd.delay),
            None => None,
        }
    }
    pub async fn get_cached_domain(&self, domain: &str) -> Option<CachedData> {
        self.cache.get(domain).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn default_situation() {
        let cache = DomainCache::new(Duration::from_millis(50), 1);

        assert_eq!(cache.get_domain_id("smt").await, None);
        assert_eq!(cache.get_domain_delay("smt").await, None);

        let test_robot = Arc::new("User-agent: * Disallow:".to_string());

        let cd_1 = CachedData {
            id: 67,
            robot: Some(Arc::clone(&test_robot)),
            delay: Duration::from_secs_f32(1.5),
        };

        cache.add_domain("new_domain", cd_1).await;

        assert_eq!(cache.get_domain_id("new_domain").await, Some(67));
        assert_eq!(
            cache.get_domain_robot("new_domain").await,
            Some(test_robot.clone())
        );
        assert_eq!(
            cache.get_domain_delay("new_domain").await,
            Some(Duration::from_secs_f32(1.5))
        );

        let cd_2 = CachedData {
            id: 100,
            robot: None,
            delay: Duration::from_secs_f32(0.5),
        };

        cache.add_domain("no_robots_domain", cd_2.clone()).await;

        assert_eq!(cache.get_domain_robot("no_robots_domain").await, None);
        assert_eq!(
            cache.get_cached_domain("no_robots_domain").await,
            Some(cd_2)
        );

        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(cache.get_domain_id("new_domain").await, None);
    }
}
