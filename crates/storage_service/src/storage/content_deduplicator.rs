use std::collections::HashMap;
use std::sync::{Arc, RwLock};

const BAND_NUM: usize = 8;
const BAND_BITS: usize = 8;
#[derive(Clone)]
pub struct ContentDeduplicator {
    bands: Arc<RwLock<Vec<HashMap<u16, Vec<u64>>>>>,
    max_dist: u32,
}
impl ContentDeduplicator {
    pub fn init(hashes: Vec<u64>, dist: u32) -> Self {
        let mut bands: Vec<HashMap<u16, Vec<u64>>> = vec![HashMap::new(); BAND_NUM];
        for h in &hashes {
            for band_id in 0..BAND_NUM {
                let band_value = Self::extract_band(*h, band_id);
                bands[band_id].entry(band_value).or_default().push(*h);
            }
        }
        Self {
            bands: Arc::new(RwLock::new(bands)),
            max_dist: dist,
        }
    }
    fn extract_band(hash: u64, band_id: usize) -> u16 {
        (hash >> (band_id * BAND_BITS)) as u16
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
        let bands = self.bands.read().unwrap();
        for band_id in 0..BAND_NUM {
            let band_value = Self::extract_band(text_hash, band_id);
            if let Some(candidates) = bands[band_id].get(&band_value) {
                if candidates
                    .iter()
                    .any(|h| simhash::hamming_distance(*h, text_hash) <= self.max_dist)
                {
                    return true;
                }
            }
        }
        false
    }
    pub fn insert(&self, text: &str) -> u64 {
        let new_hash = Self::calculate_hash(text);
        let mut guard = self.bands.write().unwrap();
        for band_id in 0..BAND_NUM {
            let band_value = Self::extract_band(new_hash, band_id);
            guard[band_id].entry(band_value).or_default().push(new_hash);
        }
        new_hash
    }
    pub fn debug_max_bucket_size(&self) -> usize {
        let bands = self.bands.read().unwrap();
        bands
            .iter()
            .flat_map(|band| band.values())
            .map(|v| v.len())
            .max()
            .unwrap_or(0)
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

        assert!(dedup.is_duplicate(text2));
    }
}
