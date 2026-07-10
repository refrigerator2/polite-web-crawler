use std::time::Duration;

use crate::crawler_error::CrawlerError;
use url::Url;

pub struct NotParsedPageData {
    pub url: Url,
    pub robots: String,
    pub content: String,
}
pub struct LinkFetcher {
    url: Url,
    client: reqwest::Client,
}
impl LinkFetcher {
    pub fn new(u: Url) -> LinkFetcher {
        LinkFetcher {
            url: u,
            client: reqwest::Client::new(),
        }
    }
    async fn check_url(&self) -> Result<(), CrawlerError> {
        let res = self
            .client
            .head(self.url.to_string())
            .timeout(Duration::from_secs(2))
            .send()
            .await?;
        if !res.status().is_success() {
            let status_error = res.error_for_status().unwrap_err();
            return Err(CrawlerError::Network(status_error));
        }
        Ok(())
    }
    async fn get_robot_list(&self) -> Result<String, CrawlerError> {
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
    async fn get_page(&self) -> Result<String, CrawlerError> {
        let response = self.client.get(self.url.clone()).send().await?;
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
    pub async fn run(self) -> Result<NotParsedPageData, CrawlerError> {
        self.check_url().await?;
        let robs = self.get_robot_list().await?;
        let page = self.get_page().await?;

        let LinkFetcher { url, .. } = self;

        Ok(NotParsedPageData {
            url,
            robots: robs,
            content: page,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_link_fetcher_new() {
        let target_url = Url::parse("https://artixlinux.org").unwrap();
        let fetcher = LinkFetcher::new(target_url.clone());

        assert_eq!(fetcher.url, target_url);
    }

    #[tokio::test]
    async fn test_check_url_success() {
        let url = Url::parse("https://www.google.com").unwrap();
        let fetcher = LinkFetcher::new(url);

        let result = fetcher.check_url().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_check_url_invalid_domain() {
        let url = Url::parse("https://this-domain-definitely-does-not-exist-12345.com").unwrap();
        let fetcher = LinkFetcher::new(url);

        let result = fetcher.check_url().await;
        assert!(result.is_err());
    }
    #[tokio::test]
    async fn test_run_lifecycle() -> Result<(), CrawlerError> {
        let target_url = Url::parse("https://www.google.com").unwrap();
        let fetcher = LinkFetcher::new(target_url.clone());

        let data = fetcher.run().await?;

        assert_eq!(data.url, target_url);
        assert!(!data.content.is_empty());

        Ok(())
    }
}
