#[derive(thiserror::Error, Debug)]
pub enum CrawlerError {
    #[error("Network error: {0}")]
    Network(#[from] reqwest::Error),

    #[error("URL parsing error: {0}")]
    InvalidUrl(#[from] url::ParseError),

    #[error("Url is not allowed")]
    NotAllowed(),

    #[error("Database error: {0}")]
    DbError(#[from] sqlx::Error),

    #[error("Robots.txt parsing error: {0}")]
    RobotParseError(#[from] anyhow::Error),
}
