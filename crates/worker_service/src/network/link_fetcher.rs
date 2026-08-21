use reqwest::header::{ACCEPT, ACCEPT_LANGUAGE, HeaderMap, HeaderValue, USER_AGENT};
use serde::{Deserialize, Serialize};
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use url::Url;

use common::error::crawler_error::CrawlerError;

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

pub struct LinkFetcher {
    pub url: Url,
    pub delay: f32,
    client: reqwest::Client,
}

impl LinkFetcher {
    pub fn new(url: Url, delay: f32) -> Self {
        let mut headers = HeaderMap::new();

        headers.insert(
            USER_AGENT,
            HeaderValue::from_static(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            ),
        );
        headers.insert(
            ACCEPT,
            HeaderValue::from_static(
                "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            ),
        );
        headers.insert(
            ACCEPT_LANGUAGE,
            HeaderValue::from_static("ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7"),
        );

        let client = reqwest::Client::builder()
            .default_headers(headers)
            .timeout(Duration::from_secs(8))
            .connect_timeout(Duration::from_secs(4))
            .redirect(reqwest::redirect::Policy::limited(5))
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());

        Self { url, delay, client }
    }

    fn is_blacklisted_extension(url: &Url) -> bool {
        let path = url.path().to_lowercase();
        let bad_extensions = [
            ".pdf", ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx", ".zip", ".rar", ".7z",
            ".tar", ".gz", ".png", ".jpg", ".jpeg", ".gif", ".svg", ".ico", ".webp", ".mp3",
            ".mp4", ".avi", ".mkv", ".css", ".js", ".json", ".xml",
        ];
        bad_extensions.iter().any(|ext| path.ends_with(ext))
    }

    async fn get_robot_list(&self) -> Result<Option<String>, CrawlerError> {
        let domain = self
            .url
            .domain()
            .ok_or_else(|| CrawlerError::UrlDoesntContainDomain())?;

        let scheme = self.url.scheme();
        let base_url_str = format!("{}://{}", scheme, domain);
        let main_link = Url::parse(&base_url_str).map_err(CrawlerError::InvalidUrl)?;

        let robot_url = main_link
            .join("robots.txt")
            .map_err(CrawlerError::InvalidUrl)?;

        let response = self.client.get(robot_url).send().await?;
        if response.status().is_success() {
            let body = response.text().await?;
            Ok(Some(body))
        } else if response.status() == reqwest::StatusCode::NOT_FOUND {
            Ok(None)
        } else {
            let status_err = response.error_for_status().unwrap_err();
            Err(CrawlerError::Network(status_err))
        }
    }

    async fn with_retry<F, Fut, T>(
        attempts: i32,
        delay: Duration,
        mut operation: F,
    ) -> Result<T, CrawlerError>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T, CrawlerError>>,
    {
        for i in 1..=attempts {
            match operation().await {
                Ok(res) => return Ok(res),
                Err(e) => {
                    if i == attempts {
                        return Err(e);
                    }
                    tokio::time::sleep(delay).await;
                }
            }
        }
        unreachable!("Unreachable retry block")
    }

    pub async fn get_page(&self) -> Result<NotParsedPageData, CrawlerError> {
        if Self::is_blacklisted_extension(&self.url) {
            return Ok(NotParsedPageData {
                url: self.url.clone(),
                content: String::new(),
            });
        }

        let response = Self::with_retry(ATTEMPTS, DURATION, || async {
            let resp = self
                .client
                .get(self.url.clone())
                .send()
                .await
                .map_err(CrawlerError::Network)?;
            Ok(resp)
        })
        .await?;

        if let Some(ct) = response.headers().get(reqwest::header::CONTENT_TYPE) {
            let ct_str = ct.to_str().unwrap_or("");
            if !ct_str.contains("text/html") && !ct_str.contains("application/xhtml+xml") {
                return Ok(NotParsedPageData {
                    url: response.url().clone(),
                    content: String::new(),
                });
            }
        }

        if response.status().is_success() {
            let final_url = response.url().clone();
            let content = response.text().await?;
            Ok(NotParsedPageData {
                url: final_url,
                content,
            })
        } else if response.status() == reqwest::StatusCode::NOT_FOUND {
            Ok(NotParsedPageData {
                url: self.url.clone(),
                content: String::new(),
            })
        } else {
            let status_err = response.error_for_status().unwrap_err();
            Err(CrawlerError::Network(status_err))
        }
    }

    pub async fn get_domain_data(&self, user_agent: &str) -> Result<DomainData, CrawlerError> {
        let domain_str = self
            .url
            .domain()
            .ok_or_else(|| CrawlerError::UrlDoesntContainDomain())?
            .to_string();

        let robot_body =
            Self::with_retry(ATTEMPTS, DURATION, || async { self.get_robot_list().await }).await?;

        if let Some(rb) = robot_body {
            let robot_matcher = texting_robots::Robot::new(user_agent, rb.as_bytes())?;
            let delay = robot_matcher.delay.unwrap_or(0.0);

            Ok(DomainData {
                domain_string: domain_str,
                robots: Some(Arc::new(rb)),
                delay,
            })
        } else {
            Ok(DomainData {
                domain_string: domain_str,
                robots: None,
                delay: 0.0,
            })
        }
    }

    pub async fn get_xml_content(&self) -> Result<String, CrawlerError> {
        let response = Self::with_retry(ATTEMPTS, DURATION, || async {
            let resp = self
                .client
                .get(self.url.clone())
                .send()
                .await
                .map_err(CrawlerError::Network)?;
            Ok(resp)
        })
        .await?;

        let ct_val = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok());

        if let Some(ct_str) = ct_val {
            let ct_lower = ct_str.to_lowercase();

            let is_xml_mime = ct_lower.contains("text/xml")
                || ct_lower.contains("application/xml")
                || ct_lower.contains("application/x-xml")
                || ct_lower.contains("+xml")
                || ct_lower.contains("gzip")
                || ct_lower.contains("octet-stream");

            if !is_xml_mime {
                return Err(CrawlerError::NoXMLContent());
            }
        }
        if response.status().is_success() {
            let content = response.text().await?;
            Ok(content)
        } else if response.status() == reqwest::StatusCode::NOT_FOUND {
            Ok(String::new())
        } else {
            let status_err = response.error_for_status().unwrap_err();
            Err(CrawlerError::Network(status_err))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[test]
    fn test_link_fetcher_new() {
        let target_url = Url::parse("https://artixlinux.org").unwrap();
        let fetcher = LinkFetcher::new(target_url.clone(), 0.0);

        assert_eq!(fetcher.url, target_url);
    }

    #[tokio::test]
    async fn test_get_page_success() -> Result<(), CrawlerError> {
        let target_url = Url::parse("https://www.google.com").unwrap();
        let fetcher = LinkFetcher::new(target_url.clone(), 0.0);

        let page_data = fetcher.get_page().await?;
        assert!(!page_data.content.is_empty());
        assert_eq!(page_data.url.host_str(), target_url.host_str());
        Ok(())
    }

    #[tokio::test]
    async fn test_get_page_blacklisted_extension() -> Result<(), CrawlerError> {
        let pdf_url = Url::parse("https://example.com/document.pdf").unwrap();
        let fetcher = LinkFetcher::new(pdf_url.clone(), 0.0);

        let page_data = fetcher.get_page().await?;
        assert!(page_data.content.is_empty());
        assert_eq!(page_data.url, pdf_url);
        Ok(())
    }

    #[tokio::test]
    async fn test_get_domain_data() -> Result<(), CrawlerError> {
        let target_url = Url::parse("https://www.google.com").unwrap();
        let fetcher = LinkFetcher::new(target_url.clone(), 2.7);

        let data = fetcher.get_domain_data("CrawlerTest").await?;
        assert_eq!(data.domain_string, target_url.domain().unwrap());
        Ok(())
    }

    fn create_test_fetcher(server_uri: &str, endpoint: &str) -> LinkFetcher {
        let full_url = format!("{}{}", server_uri, endpoint);
        let url = Url::parse(&full_url).unwrap();
        LinkFetcher::new(url, 0.0)
    }

    #[tokio::test]
    async fn test_get_xml_content_success_valid_content_type() {
        let mock_server = MockServer::start().await;
        let xml_body =
            r#"<?xml version="1.0"?><urlset><url><loc>https://example.com</loc></url></urlset>"#;

        Mock::given(method("GET"))
            .and(path("/sitemap.xml"))
            .respond_with(ResponseTemplate::new(200).set_body_raw(
                xml_body.as_bytes().to_vec(),
                "application/xml; charset=utf-8",
            ))
            .expect(1..)
            .mount(&mock_server)
            .await;

        let fetcher = create_test_fetcher(&mock_server.uri(), "/sitemap.xml");
        let result = fetcher.get_xml_content().await;

        assert_eq!(result.expect("Should be Ok"), xml_body);
    }

    #[tokio::test]
    async fn test_get_xml_content_success_gzip_content_type() {
        let mock_server = MockServer::start().await;
        let xml_body = r#"<?xml version="1.0"?><sitemapindex></sitemapindex>"#;

        Mock::given(method("GET"))
            .and(path("/sitemap.xml.gz"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_raw(xml_body.as_bytes().to_vec(), "application/gzip"),
            )
            .expect(1..)
            .mount(&mock_server)
            .await;

        let fetcher = create_test_fetcher(&mock_server.uri(), "/sitemap.xml.gz");
        let result = fetcher.get_xml_content().await;

        assert_eq!(result.expect("Should be Ok"), xml_body);
    }

    #[tokio::test]
    async fn test_get_xml_content_invalid_content_type() {
        let mock_server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/not-xml"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("Content-Type", "text/html; charset=utf-8")
                    .set_body_string("<html><body>Not XML</body></html>"),
            )
            .expect(1..)
            .mount(&mock_server)
            .await;

        let fetcher = create_test_fetcher(&mock_server.uri(), "/not-xml");
        let result = fetcher.get_xml_content().await;

        assert!(matches!(result, Err(CrawlerError::NoXMLContent())));
    }

    #[tokio::test]
    async fn test_get_xml_content_not_found_404() {
        let mock_server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/missing.xml"))
            .respond_with(
                ResponseTemplate::new(404).insert_header("Content-Type", "application/xml"),
            )
            .expect(1..)
            .mount(&mock_server)
            .await;

        let fetcher = create_test_fetcher(&mock_server.uri(), "/missing.xml");
        let result = fetcher.get_xml_content().await;

        assert_eq!(result.expect("404 should return empty Ok string"), "");
    }

    #[tokio::test]
    async fn test_get_xml_content_server_error_500() {
        let mock_server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/error.xml"))
            .respond_with(
                ResponseTemplate::new(500).insert_header("Content-Type", "application/xml"),
            )
            .expect(1..)
            .mount(&mock_server)
            .await;

        let fetcher = create_test_fetcher(&mock_server.uri(), "/error.xml");
        let result = fetcher.get_xml_content().await;

        assert!(matches!(result, Err(CrawlerError::Network(_))));
    }
}
