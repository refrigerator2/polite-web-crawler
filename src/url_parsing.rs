use std::time::Duration;

use crate::crawler_error::CrawlerError;
use reqwest::get;
use url::Url;

pub struct LinkFetcher {
    url: Url,
    keywords: Option<Vec<String>>,
    title: String,
    content: String,
    client: reqwest::Client,
}
impl LinkFetcher {
    pub fn new(u: Url, keys: Option<Vec<String>>) -> LinkFetcher {
        LinkFetcher {
            url: u,
            keywords: keys,
            title: String::new(),
            content: String::new(),
            client: reqwest::Client::new(),
        }
    }
    async fn check_url(self) -> Result<(), CrawlerError> {
        let res = self
            .client
            .head(self.url)
            .timeout(Duration::from_secs(2))
            .send()
            .await?;
        if !res.status().is_success() {
            let status_error = res.error_for_status().unwrap_err();
            return Err(CrawlerError::Network(status_error));
        }
        Ok(())
    }
    async fn get_robot_list(self) -> Result<String, CrawlerError> {
        let robot_url = Url::join(&self.url, "robots.txt")?;
        let response = self.client.get(robot_url).send().await?;
        if response.status().is_success() {
            let body = response.text().await?;
            Ok(body)
        } else if response.status() == reqwest::StatusCode::NOT_FOUND {
            Ok(String::new())
        } else {
            let status_err = response.error_for_status().unwrap_err();
            Err(CrawlerError::Network(status_err))
        }
    }
    async fn get_page(self) -> Result<String, CrawlerError> {
        let response = self.client.get(self.url).send().await?;
        if response.status().is_success() {
            let body = response.text().await?;
            Ok(body)
        } else if response.status() == reqwest::StatusCode::NOT_FOUND {
            Ok(String::new())
        } else {
            let status_err = response.error_for_status().unwrap_err();
            Err(CrawlerError::Network(status_err))
        }
    }
    pub async fn run(self) -> Result<(), CrawlerError> {
        self.check_url().await?;

        Ok(())
    }
}
