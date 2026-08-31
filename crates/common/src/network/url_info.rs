use reqwest::header::{ACCEPT, ACCEPT_LANGUAGE, HeaderMap, HeaderValue, USER_AGENT};
use serde::{Deserialize, Serialize};
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use url::Url;

use crate::error::crawler_error::CrawlerError;

const ATTEMPTS: i32 = 3;
const DURATION: Duration = Duration::from_secs(1);

pub struct NotParsedPageData {
    pub url: Url,
    pub content: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DomainData {
    pub domain_string: String,
    pub robots: Option<Arc<String>>,
    pub delay: f32,
}
#[derive(Debug, PartialEq, Eq)]
pub enum UrlAccess {
    Allowed,
    Disallowed,
    UnknownDomain,
    URLWithoutHost,
}
