use crate::network::link_fetcher::NotParsedPageData;
use scraper::{Html, Selector};
use std::sync::Arc;
use url::{ParseError, Url};

pub struct ParsedPage {
    pub title: Option<String>,
    pub description: Option<String>,
    pub clean_text: Option<String>,
    pub outbound_links: Vec<Url>,
    pub keywords: Arc<Option<Vec<String>>>,
    pub url: Url,
    pub keywords_in_it: bool,
}
impl ParsedPage {
    pub fn parse(data: NotParsedPageData, keywords: Arc<Option<Vec<String>>>) -> ParsedPage {
        let mut parsed_page = Self::init(&data, keywords);

        let doc = Html::parse_document(&data.content);

        parsed_page.set_clean_text(&doc);
        if !parsed_page.is_keywords_in_html() {
            parsed_page.set_outbound_urls(&doc);
            parsed_page.clean_text = None;
            return parsed_page;
        }
        parsed_page.set_outbound_urls(&doc);
        parsed_page.set_description(&doc);
        parsed_page.set_title(&doc);
        parsed_page.keywords_in_it = true;
        parsed_page
    }
    fn init(data: &NotParsedPageData, keywords: Arc<Option<Vec<String>>>) -> ParsedPage {
        ParsedPage {
            title: None,
            description: None,
            clean_text: None,
            outbound_links: vec![],
            keywords,
            url: data.url.clone(),
            keywords_in_it: false,
        }
    }
    fn set_outbound_urls(&mut self, doc: &Html) {
        let link_selector = Selector::parse("a").unwrap();

        let mut urls = vec![];
        for element in doc.select(&link_selector) {
            if let Some(href) = element.value().attr("href") {
                let parsed = Url::parse(href);
                match parsed {
                    Ok(mut u) => {
                        if u.scheme() == "http" || u.scheme() == "https" {
                            u.set_fragment(None);
                            if u != self.url {
                                urls.push(u);
                            }
                        };
                    }
                    Err(ParseError::RelativeUrlWithoutBase) => {
                        let temp = self.url.join(href);
                        if let Ok(mut u) = temp {
                            u.set_fragment(None);
                            if u != self.url {
                                urls.push(u);
                            }
                        }
                    }
                    Err(_) => continue,
                }
            }
        }
        self.outbound_links = urls;
    }
    fn set_title(&mut self, doc: &Html) {
        let title_selector = Selector::parse("title").unwrap();

        self.title = doc
            .select(&title_selector)
            .next()
            .map(|element| {
                let full_text = element
                    .children()
                    .filter_map(|child| child.value().as_text())
                    .map(|text| text.trim())
                    .filter(|text| !text.is_empty())
                    .collect::<Vec<_>>()
                    .join(" ");

                if let Some(index) = full_text.find('<') {
                    full_text[..index].trim().to_string()
                } else {
                    full_text
                }
            })
            .filter(|title| !title.is_empty());
    }
    fn is_keywords_in_html(&self) -> bool {
        let keywords = match self.keywords.as_deref() {
            Some(keys) => keys,
            None => return true,
        };

        let text = match self.clean_text.as_deref() {
            Some(t) => t,
            None => return false,
        };

        for key in keywords {
            if !text.contains(key) {
                return false;
            }
        }

        true
    }
    fn set_clean_text(&mut self, doc: &Html) {
        let mut clean_text = String::new();

        let clean_tags_selector =
            Selector::parse("*:not(script):not(style):not(head):not(noscript):not(meta)").unwrap();

        for element in doc.select(&clean_tags_selector) {
            for child in element.children() {
                if let Some(text_node) = child.value().as_text() {
                    let trimmed = text_node.trim();
                    if !trimmed.is_empty() {
                        let mut res = trimmed;
                        if trimmed.contains("<") || trimmed.contains(">") {
                            let less_sym = trimmed.find("<");
                            let g_sym = trimmed.find(">");
                            if let Some(ind) = less_sym {
                                res = &trimmed[..ind];
                            }
                            if let Some(ind) = g_sym
                                && (less_sym.is_none() || less_sym.unwrap() > ind)
                            {
                                res = &trimmed[..ind];
                            }
                        }
                        clean_text.push_str(res);
                        clean_text.push(' ');
                    }
                }
            }
        }

        self.clean_text = Some(clean_text.trim().to_string());
    }
    fn set_description(&mut self, doc: &Html) {
        let desc_selector = Selector::parse("meta[name=\"description\"]").unwrap();

        let desc_text = doc
            .select(&desc_selector)
            .next()
            .and_then(|element| element.value().attr("content"))
            .map(|text| text.to_string());

        self.description = desc_text;
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    fn create_test_data(html_content: &str) -> NotParsedPageData {
        NotParsedPageData {
            url: Url::parse("https://example.com/blog/page1.html").unwrap(),
            content: html_content.to_string(),
        }
    }
    #[test]
    fn test_successful_metadata_and_text_extraction() {
        let html = r#"
            <!DOCTYPE html>
            <html>
            <head>
                <title>Hello, World! — My Website</title>
                <meta name="description" content="This is a great page description for the crawler.">
                <style>body { color: red; }</style>
                <script>console.log("garbage text");</script>
            </head>
            <body>
                <h1>Article Header</h1>
                <p>Main paragraph text.</p>
                <div>Some more content.</div>
            </body>
            </html>
        "#;

        let npd = create_test_data(html);
        let parsed = ParsedPage::parse(npd, Arc::default());

        assert_eq!(parsed.title.as_deref(), Some("Hello, World! — My Website"));
        assert_eq!(
            parsed.description.as_deref(),
            Some("This is a great page description for the crawler.")
        );

        let clean = parsed.clean_text;
        assert!(clean.is_some());
        assert!(clean.as_deref().unwrap().contains("Article Header"));
        assert!(clean.as_deref().unwrap().contains("Main paragraph text."));
        assert!(clean.as_deref().unwrap().contains("Some more content."));
        assert!(!clean.as_deref().unwrap().contains("console.log"));
        assert!(!clean.as_deref().unwrap().contains("body {"));
    }

    #[test]
    fn test_missing_metadata_handles_gracefully() {
        let html = "<body><p>Just text</p></body>";

        let npd = create_test_data(html);
        let parsed = ParsedPage::parse(npd, Arc::default());

        assert_eq!(parsed.title, None);
        assert_eq!(parsed.description, None);
        assert_eq!(parsed.clean_text.unwrap(), "Just text");
    }

    #[test]
    fn test_outbound_urls_extraction_and_resolution() {
        let html = r##"
            <body>
                <a href="https://google.com/search?q=rust">Google</a>
                <a href="/about">About Us</a>
                <a href="next-post.html">Next Post</a>
                <a href="#contacts">Contacts Anchor</a>
                <a href="javascript:void(0)">JS Link</a>
                <a>Link Without Href</a>
            </body>
        "##;

        let npd = create_test_data(html);
        let parsed = ParsedPage::parse(npd, Arc::default());
        let urls = parsed.outbound_links;

        let expected_absolute = Url::parse("https://google.com/search?q=rust").unwrap();
        assert!(urls.contains(&expected_absolute));

        let expected_root_relative = Url::parse("https://example.com/about").unwrap();
        assert!(urls.contains(&expected_root_relative));

        let expected_path_relative = Url::parse("https://example.com/blog/next-post.html").unwrap();
        assert!(urls.contains(&expected_path_relative));
        assert_eq!(urls.len(), 3);
    }

    #[test]
    fn test_broken_html_handling() {
        let html = "<title>Broken HTML<div>Hello! <a href='https://broken.com'>Link</p>";

        let npd = create_test_data(html);
        let parsed = ParsedPage::parse(npd, Arc::default());

        assert_eq!(parsed.title.as_deref(), Some("Broken HTML"));
        assert_eq!(parsed.clean_text.as_deref(), Some("Broken HTML"));
        //let expected_url = Url::parse("https://broken.com").unwrap();
        //assert!(parsed.outbound_links.contains(&expected_url));
    }
}
