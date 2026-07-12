use std::time::Duration;

use crate::crawler_error::CrawlerError;
use texting_robots::Robot;
use url::Url;

pub struct NotParsedPageData {
    pub url: Url,
    pub content: String,
    pub delay: Option<f32>,
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
    async fn get_robot_list(&self) -> Result<Option<Robot>, CrawlerError> {
        let domain = Url::domain(&self.url).unwrap();
        let scheme = Url::scheme(&self.url);
        let main_link = Url::parse((scheme.to_string() + "://" + domain).as_str());
        if let Err(e) = main_link {
            return Err(CrawlerError::InvalidUrl(e));
        }
        let main_link = main_link.unwrap();
        let robot_url = Url::join(&main_link, "robots.txt")?;
        let response = self.client.get(robot_url).send().await?;
        if response.status().is_success() {
            let body = response.text().await?;
            let rclient = Robot::new("CrabCrawler", body.as_bytes()).unwrap();
            if !rclient.allowed(self.url.as_str()) {
                return Err(CrawlerError::NotAllowed());
            }
            Ok(Some(rclient))
        } else if response.status() == reqwest::StatusCode::NOT_FOUND {
            Ok(None)
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
        let mut delay = None;
        if let Some(r) = robs {
            delay = r.delay;
        }
        let page = self.get_page().await?;

        let LinkFetcher { url, .. } = self;
        Ok(NotParsedPageData {
            url,
            content: page,
            delay,
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
