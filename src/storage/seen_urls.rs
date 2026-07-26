use fastbloom::BloomFilter;
use std::sync::{Arc, Mutex};
use url::Url;

const EXPECTED_NUM_OF_URLS: usize = 10_000_000;
const EXPECTED_LOSS: f64 = 0.001;
#[derive(Clone)]
pub struct SeenUrls {
    seen_urls: Arc<Mutex<BloomFilter>>,
}
impl SeenUrls {
    pub fn new() -> Self {
        Self {
            seen_urls: Arc::new(Mutex::new(
                BloomFilter::with_false_pos(EXPECTED_LOSS).expected_items(EXPECTED_NUM_OF_URLS),
            )),
        }
    }
    pub fn insert_url(&self, url: &Url) -> bool {
        let mut guard = self.seen_urls.lock().unwrap();
        guard.insert(url)
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    fn test_creating_and_inserting() {
        let su = SeenUrls::new();
        let url = Url::parse("https://www.google.com/").unwrap();
        let res = su.insert_url(&url);
        assert!(!res);
        let res = su.insert_url(&url);
        assert!(res);
    }
}
