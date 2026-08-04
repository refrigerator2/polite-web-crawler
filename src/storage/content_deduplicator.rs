use std::sync::RwLock;

pub struct ContentDeduplicator {
    hashes: RwLock<Vec<u64>>,
    max_dist: u32,
}
impl ContentDeduplicator {
    pub fn init(hashes: Vec<u64>, dist: u32) -> Self {
        Self {
            hashes: RwLock::new(hashes),
            max_dist: dist,
        }
    }
    fn calculate_hash(text: &str) -> u64 {
        let cleaned_text = text
            .chars()
            .filter(|c| c.is_whitespace() || c.is_alphanumeric())
            .collect::<String>()
            .to_lowercase();
        let bytes = cleaned_text.as_bytes();

        if bytes.len() < 4 {
            return simhash::simhash(text);
        }

        let ngrams = bytes.windows(4).filter_map(|w| std::str::from_utf8(w).ok());

        simhash::simhash_stream(ngrams)
    }
    pub fn is_duplicate(&self, text: &str) -> bool {
        let text_hash = Self::calculate_hash(text);
        let hashes = self.hashes.read().unwrap();
        hashes
            .iter()
            .any(|h| simhash::hamming_distance(*h, text_hash) <= self.max_dist)
    }
    pub fn insert(&self, text: &str) {
        let new_hash = Self::calculate_hash(text);
        let mut guard = self.hashes.write().unwrap();
        guard.push(new_hash);
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deduplicator() {
        let dedup = ContentDeduplicator::init(Vec::new(), 3);

        let text = "Rust is a systems programming language that runs blazingly fast, prevents segfaults, and guarantees thread safety. It enables everyone to build reliable and efficient software.";

        let text1 = "Rust is a systems programming language that runs blazingly quick, prevents segfaults, and guarantees thread safety. It enables everyone to build reliable and efficient software.";

        let text2 = "PostgreSQL is a powerful, open source object-relational database system with over 30 years of active development.";

        assert!(!dedup.is_duplicate(text));
        dedup.insert(text);

        assert!(dedup.is_duplicate(text1));

        assert!(!dedup.is_duplicate(text2));
        dedup.insert(text2);

        assert_eq!(dedup.hashes.read().unwrap().len(), 2);
    }
}
