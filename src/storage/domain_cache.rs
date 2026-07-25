use moka::future::Cache;
use std::sync::Arc;
use std::time::Duration;

#[derive(Clone, Debug, PartialEq)]
struct CachedData {
    pub id: i64,
    pub robot: Arc<String>,
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

    pub async fn add_domain(&self, domain: &str, id: i64, robot: String) {
        self.cache
            .insert(
                domain.to_string(),
                CachedData {
                    id,
                    robot: Arc::new(robot),
                },
            )
            .await;
    }

    pub async fn get_domain_id(&self, domain: &str) -> Option<i64> {
        match self.cache.get(domain).await {
            Some(cd) => Some(cd.id),
            None => None,
        }
    }
    pub async fn get_domain_robot(&self, domain: &str) -> Option<Arc<String>> {
        match self.cache.get(domain).await {
            Some(cd) => Some(Arc::clone(&cd.robot)),
            None => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn default_situation() {
        let cache = DomainCache::new(Duration::from_millis(50), 1);

        let res = cache.get_domain_id("smt").await;
        assert_eq!(res, None);

        let test_robot = "User-agent: * Disallow:".to_string();

        cache.add_domain("new_domain", 67, test_robot.clone()).await;

        let res_id = cache.get_domain_id("new_domain").await;
        assert_eq!(res_id, Some(67));

        let res_robot = cache.get_domain_robot("new_domain").await;
        assert_eq!(res_robot, Some(Arc::new(test_robot)));

        tokio::time::sleep(Duration::from_millis(100)).await;

        let res_after_ttl = cache.get_domain_id("new_domain").await;
        assert_eq!(res_after_ttl, None);
    }
}
