use std::time::Duration;

use crate::crawler_error::CrawlerError;
use url::Url;

const ATTEMTS: usize = 3;

pub struct NotParsedPageData {
    pub url: Url,
    pub content: String,
}
#[derive(Debug)]
pub struct DomainData {
    pub domain_string: String,
    pub robots: Option<String>,
    pub delay: f32,
}
pub struct LinkFetcher {
    pub url: Url,
    pub delay: f32,
    client: reqwest::Client,
}
impl LinkFetcher {
    pub fn new(u: Url, delay: f32) -> LinkFetcher {
        LinkFetcher {
            url: u,
            delay,
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
    async fn get_robot_list(&self) -> Result<Option<String>, CrawlerError> {
        let domain = Url::domain(&self.url).unwrap();
        let scheme = Url::scheme(&self.url);
        let main_link = Url::parse((scheme.to_string() + "://" + domain).as_str());
        if let Err(e) = main_link {
            return Err(CrawlerError::InvalidUrl(e));
        }
        let main_link = main_link.unwrap();
        let robot_url = Url::join(&main_link, "robots.txt")?;
        let mut backoff = Duration::from_secs(1);
        for i in 0..ATTEMTS {
            let response = self.client.get(robot_url.clone()).send().await?;
            if response.status().is_success() {
                let body = response.text().await?;
                return Ok(Some(body));
            } else if response.status() == reqwest::StatusCode::NOT_FOUND {
                return Ok(None);
            } else if i == ATTEMTS - 1 {
                let status_err = response.error_for_status().unwrap_err();
                return Err(CrawlerError::Network(status_err));
            }
            tokio::time::sleep(backoff).await;
            backoff *= 2;
        }
        Ok(None)
    }
    async fn get_page(&self) -> Result<String, CrawlerError> {
        for i in 0..ATTEMTS {
            let response = self.client.get(self.url.clone()).send().await?;
            if response.status().is_success() {
                let body = response.text().await?;
                return Ok(body);
            } else if response.status() == reqwest::StatusCode::NOT_FOUND {
                return Ok(String::new());
            } else if i == ATTEMTS - 1 {
                let status_err = response.error_for_status().unwrap_err();
                return Err(CrawlerError::Network(status_err));
            }
            tokio::time::sleep(Duration::from_secs_f32(self.delay)).await;
        }
        Ok(String::new())
    }
    pub async fn get_domain_data(&mut self, user_agent: &str) -> Result<DomainData, CrawlerError> {
        let domain = self.url.clone();

        let domain = domain
            .domain()
            .ok_or(CrawlerError::UrlDoesntContainDomain())?;

        let scheme = self.url.scheme();
        let root_url = Url::parse(&format!("{}://{}/", scheme, domain))?;

        let temp = self.url.clone();
        self.url = root_url;
        self.check_url().await?;

        let robot_body = self.get_robot_list().await?;
        self.url = temp;
        let robots_str = robot_body.as_deref().unwrap_or("");
        let robot_matcher = texting_robots::Robot::new(user_agent, robots_str.as_bytes())?;

        let delay = robot_matcher.delay.unwrap_or(0.0);

        Ok(DomainData {
            domain_string: domain.to_string(),
            robots: robot_body,
            delay,
        })
    }
    pub async fn get_page_data(self) -> Result<NotParsedPageData, CrawlerError> {
        self.check_url().await?;

        let page = self.get_page().await?;

        let LinkFetcher { url, .. } = self;
        Ok(NotParsedPageData { url, content: page })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_link_fetcher_new() {
        let target_url = Url::parse("https://artixlinux.org").unwrap();
        let fetcher = LinkFetcher::new(target_url.clone(), 0.0);

        assert_eq!(fetcher.url, target_url);
    }

    #[tokio::test]
    async fn test_check_url_success() {
        let url = Url::parse("https://www.google.com").unwrap();
        let fetcher = LinkFetcher::new(url, 1.0);

        let result = fetcher.check_url().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_check_url_invalid_domain() {
        let url = Url::parse("https://this-domain-definitely-does-not-exist-12345.com").unwrap();
        let fetcher = LinkFetcher::new(url, 0.0);

        let result = fetcher.check_url().await;
        assert!(result.is_err());
    }
    #[tokio::test]
    async fn test_run_lifecycle() -> Result<(), CrawlerError> {
        let target_url = Url::parse("https://www.google.com").unwrap();
        let fetcher = LinkFetcher::new(target_url.clone(), 3.0);

        let data = fetcher.get_page_data().await?;

        assert_eq!(data.url, target_url);
        assert!(!data.content.is_empty());
        Ok(())
    }
    #[tokio::test]
    async fn test_get_domain() -> Result<(), CrawlerError> {
        let target_url = Url::parse("https://www.google.com").unwrap();
        let mut fetcher = LinkFetcher::new(target_url.clone(), 2.7);

        let data = fetcher.get_domain_data("CrawlerTest").await?;
        assert_eq!(data.domain_string, target_url.domain().unwrap());
        assert_eq!(data.delay, 0.0);
        Ok(())
    }
}
