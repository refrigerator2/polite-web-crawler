#[derive(thiserror::Error, Debug)]
pub enum CrawlerError {
    #[error("Network error: {0}")]
    Network(#[from] reqwest::Error),

    #[error("URL parsing error: {0}")]
    InvalidUrl(#[from] url::ParseError),
}
