use dashmap::DashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

pub struct DomainRateLimiter {
    delay: Duration,
    map: Arc<DashMap<String, Instant>>,
}
impl DomainRateLimiter {
    pub fn new(delay: Duration) -> Self {
        Self {
            delay,
            map: Arc::new(DashMap::new()),
        }
    }

    pub fn try_acquire(&mut self, domain: &str) -> bool {
        let now = Instant::now();
        let mut entry = self
            .map
            .entry(domain.to_string())
            .or_insert_with(|| now - self.delay);
        if now.duration_since(*entry.value()) >= self.delay {
            *entry.value_mut() = now;
            true
        } else {
            false
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_after_time_passed() {
        let mut drl = DomainRateLimiter::new(Duration::from_secs(1));
        let res = drl.try_acquire("new_domain");
        assert!(res);
        let res = drl.try_acquire("new_domain");
        assert!(!res);
        std::thread::sleep(Duration::from_secs(1));
        let res = drl.try_acquire("new_domain");
        assert!(res);
    }
}
