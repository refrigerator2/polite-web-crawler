use moka::future::Cache;
use std::time::Duration;
pub struct DomainCache {
    cache: Cache<String, u32>,
}
impl DomainCache {
    pub fn new() -> Self {
        Self {
            cache: Cache::builder()
                .time_to_idle(Duration::from_secs(360))
                .initial_capacity(20)
                .build(),
        }
    }
    pub async fn add_domain(&mut self, domain: &str, id: u32) {
        self.cache.insert(domain.to_string(), id).await;
    }
    pub async fn get_domain_id(&self, domain: &str) -> Option<u32> {
        self.cache.get(domain).await
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    async fn default_situation() {
        let mut cache = DomainCache::new();
        let res = cache.get_domain_id("smt").await;
        assert_eq!(res, None);
        cache.add_domain("new_domain", 67).await;
        let res = cache.get_domain_id("new_domain").await;
        assert_eq!(Some(67), res);
        std::thread::sleep(Duration::from_secs(360));
        let res = cache.get_domain_id("new_domain").await;
        assert_eq!(res, None);
    }
}
