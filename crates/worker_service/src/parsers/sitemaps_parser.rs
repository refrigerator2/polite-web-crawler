use crate::network::link_fetcher::{self, LinkFetcher};
use quick_xml::Reader;
use quick_xml::events::Event;
use url::Url;

pub struct SitemapsParser {
    sitemaps: Vec<String>,
    delay: f32,
}

impl SitemapsParser {
    pub fn new(sitemaps: Vec<String>, delay: f32) -> Self {
        Self { sitemaps, delay }
    }

    pub async fn parse(&self) -> Vec<Url> {
        if self.sitemaps.is_empty() {
            return Vec::new();
        }

        let valid_sitemap_urls: Vec<Url> = self
            .sitemaps
            .iter()
            .filter_map(|s| Url::parse(s).ok())
            .collect();

        if valid_sitemap_urls.is_empty() {
            return Vec::new();
        }

        let mut urls = vec![];
        for sitemap in valid_sitemap_urls {
            let link_fetcher = LinkFetcher::new(sitemap, self.delay);
            let parsed_urls = link_fetcher.get_xml_content().await;
            if let Ok(xml) = parsed_urls {
                urls.append(&mut Self::parse_sitemap(&xml));
            }
        }
        urls
    }

    pub fn parse_sitemap(content: &str) -> Vec<Url> {
        let mut reader = Reader::from_str(content);
        reader.config_mut().trim_text(true);
        let mut buf = Vec::new();
        let mut urls = Vec::new();

        loop {
            match reader.read_event_into(&mut buf) {
                Ok(Event::Start(ref e)) if e.name() == quick_xml::name::QName(b"loc") => {
                    match reader.read_text(e.name()) {
                        Ok(bytes_text) => {
                            let raw_bytes = bytes_text.into_inner();

                            if let Ok(str_slice) = reader.decoder().decode(&raw_bytes) {
                                if let Ok(unescaped) = quick_xml::escape::unescape(&str_slice) {
                                    let url_str = unescaped.trim().to_string();
                                    if !url_str.is_empty() {
                                        if let Ok(url) = Url::parse(&url_str) {
                                            urls.push(url);
                                        }
                                    }
                                }
                            }
                        }
                        Err(err) => {
                            eprintln!("Failed to parse <loc>: {:?}", err);
                        }
                    }
                }
                Ok(Event::Eof) => break,
                Err(err) => {
                    eprintln!("Error parsing XML: {:?}", err);
                    break;
                }
                _ => (),
            }
            buf.clear();
        }

        urls
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_valid_sitemap_xml() {
        let xml_data = r#"<?xml version="1.0" encoding="UTF-8"?>
        <urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
            <url>
                <loc>https://example.com/page1</loc>
                <lastmod>2026-01-01</lastmod>
            </url>
            <url>
                <loc>https://example.com/page2</loc>
                <lastmod>2026-01-02</lastmod>
            </url>
        </urlset>"#;

        let urls = SitemapsParser::parse_sitemap(xml_data);

        assert_eq!(urls.len(), 2);
        assert_eq!(urls[0].as_str(), "https://example.com/page1");
        assert_eq!(urls[1].as_str(), "https://example.com/page2");
    }

    #[test]
    fn test_parse_sitemap_index_xml() {
        let xml_data = r#"<?xml version="1.0" encoding="UTF-8"?>
        <sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
            <sitemap>
                <loc>https://example.com/sitemap1.xml</loc>
            </sitemap>
            <sitemap>
                <loc>https://example.com/sitemap2.xml</loc>
            </sitemap>
        </sitemapindex>"#;

        let urls = SitemapsParser::parse_sitemap(xml_data);

        assert_eq!(urls.len(), 2);
        assert_eq!(urls[0].as_str(), "https://example.com/sitemap1.xml");
        assert_eq!(urls[1].as_str(), "https://example.com/sitemap2.xml");
    }

    #[test]
    fn test_parse_empty_or_broken_xml() {
        let invalid_xml = "NOT_A_VALID_XML_STRING <broken>>>";
        let urls = SitemapsParser::parse_sitemap(invalid_xml);
        assert!(urls.is_empty());

        let empty_xml = "";
        let urls_empty = SitemapsParser::parse_sitemap(empty_xml);
        assert!(urls_empty.is_empty());
    }

    #[tokio::test]
    async fn test_parse_with_invalid_urls_in_vec() {
        let bad_sitemaps = vec![
            "invalid_url_1".to_string(),
            "httpt:///broken".to_string(),
            "".to_string(),
        ];

        let parser = SitemapsParser::new(bad_sitemaps, 0.0);
        let urls = parser.parse().await;

        assert!(urls.is_empty());
    }

    #[tokio::test]
    async fn test_parse_empty_sitemaps_list() {
        let parser = SitemapsParser::new(vec![], 0.0);
        let urls = parser.parse().await;
        assert!(urls.is_empty());
    }
}
