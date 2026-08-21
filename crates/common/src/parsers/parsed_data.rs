use crate::network::url_info::DomainData;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
#[derive(Serialize, Deserialize)]
pub struct ParsedPageSaveData {
    pub title: Option<String>,
    pub description: Option<String>,
    pub clean_text: Option<String>,
    pub url: String,
}

#[derive(Serialize, Deserialize)]
pub struct DomainDataSaveData {
    pub domain_string: String,
    pub robots: Option<Arc<String>>,
    pub delay: f32,
}

impl DomainDataSaveData {
    pub fn from_domain_data(data: &DomainData) -> Self {
        Self {
            domain_string: data.domain_string.clone(),
            robots: data.robots.clone(),
            delay: data.delay,
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "event_type")]
pub enum ParsedData {
    ParsedDomain(DomainDataSaveData),
    ParsedPage(ParsedPageSaveData),
}
