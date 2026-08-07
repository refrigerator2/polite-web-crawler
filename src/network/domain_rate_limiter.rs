use dashmap::DashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Clone)]
pub struct DomainRateLimiter {
    map: Arc<DashMap<String, DomainState>>,
    default_delay: Duration,
}

pub struct DomainState {
    pub delay: Duration,
    pub last_accessed: Instant,
}

impl DomainRateLimiter {
    pub fn new(default_delay: Duration) -> Self {
        Self {
            map: Arc::new(DashMap::new()),
            default_delay,
        }
    }

    pub fn try_acquire(&self, domain: &str) -> bool {
        let now = Instant::now();

        let mut entry = self
            .map
            .entry(domain.to_string())
            .or_insert_with(|| DomainState {
                delay: self.default_delay,
                last_accessed: now - self.default_delay,
            });

        let state = entry.value_mut();

        if now.saturating_duration_since(state.last_accessed) >= state.delay {
            state.last_accessed = now;
            true
        } else {
            false
        }
    }

    pub fn update_delay(&self, domain: &str, new_delay: Duration) {
        let now = Instant::now();
        self.map
            .entry(domain.to_string())
            .and_modify(|state| state.delay = new_delay)
            .or_insert_with(|| DomainState {
                delay: new_delay,
                last_accessed: now - new_delay,
            });
    }
    pub async fn await_acquiring(&self, domain: &str) {
        let now = Instant::now();

        let mut entry = self
            .map
            .entry(domain.to_string())
            .or_insert_with(|| DomainState {
                delay: self.default_delay,
                last_accessed: now - self.default_delay,
            });

        let state = entry.value_mut();

        let time = now.saturating_duration_since(state.last_accessed);
        tokio::time::sleep(time).await;
        state.last_accessed = now;
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_try_aquire() {
        let drl = DomainRateLimiter::new(Duration::from_secs(1));
        let res = drl.try_acquire("new_domain");
        assert!(res);
        let res = drl.try_acquire("new_domain");
        assert!(!res);
        std::thread::sleep(Duration::from_secs(1));
        let res = drl.try_acquire("new_domain");
        assert!(res);
    }
    #[test]
    fn test_update_delay() {
        let drl = DomainRateLimiter::new(Duration::from_secs(1));
        let res = drl.try_acquire("new_domain");
        assert!(res);
        drl.update_delay("new_domain", Duration::from_secs(2));
        let res = drl.try_acquire("new_domain");
        assert!(!res);
        std::thread::sleep(Duration::from_secs(1));
        let res = drl.try_acquire("new_domain");
        assert!(!res);
    }
}
