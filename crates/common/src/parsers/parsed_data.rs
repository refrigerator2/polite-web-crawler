use crate::{network::link_fetcher::DomainData, parsers::html_parser::ParsedPage};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct ParsedPageSaveData {
    pub title: Option<String>,
    pub description: Option<String>,
    pub clean_text: Option<String>,
    pub url: String,
}

impl ParsedPageSaveData {
    pub fn convert_parsed_page(page: &ParsedPage) -> Self {
        Self {
            title: page.title.clone(),
            description: page.description.clone(),
            clean_text: page.clean_text.clone(),
            url: page.url.to_string(),
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct DomainDataSaveData {
    pub domain_string: String,
    pub robots: Option<String>,
    pub delay: f32,
}

impl DomainDataSaveData {
    pub fn from_domain_data(data: &DomainData) -> Self {
        Self {
            domain_string: data.domain_string.clone(),
            robots: data.robots.as_ref().map(|arc| arc.as_str().to_string()),
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
