use crate::link_fetcher::NotParsedPageData;
use scraper::{Html, Selector};
use url::{ParseError, Url};

pub struct ParsedPage {
    pub title: Option<String>,
    pub description: Option<String>,
    pub clean_text: String,
    pub outbound_links: Vec<Url>,
    pub keywords: Option<Vec<String>>,
    pub delay: f32,
    pub url: Url,
}
impl ParsedPage {
    pub fn parse(&mut self, data: NotParsedPageData, keywords: Option<Vec<String>>) {
        self.init(&data, keywords);

        let doc = Html::parse_document(&data.content);

        self.set_clean_text(&doc);
        if !self.is_keywords_in_html() {
            return;
        }
        self.set_outbound_urls(&doc);
        self.set_description(&doc);
        self.set_title(&doc);
    }
    fn init(&mut self, data: &NotParsedPageData, keywords: Option<Vec<String>>) {
        self.title = None;
        self.description = None;
        self.clean_text = String::new();
        self.outbound_links = vec![];
        self.keywords = keywords;
        if let Some(del) = data.delay {
            self.delay = del;
        } else {
            self.delay = 0.0;
        }
        self.url = data.url.clone();
    }
    fn set_outbound_urls(&mut self, doc: &Html) {
        let link_selector = Selector::parse("a").unwrap();

        let mut urls = vec![];
        for element in doc.select(&link_selector) {
            if let Some(href) = element.value().attr("href") {
                let parsed = Url::parse(href);
                match parsed {
                    Ok(u) => urls.push(u),
                    Err(ParseError::RelativeUrlWithoutBase) => {
                        let temp = self.url.join(href);
                        if let Ok(u) = temp {
                            urls.push(u);
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

        let title_text = doc
            .select(&title_selector)
            .next()
            .map(|element| element.text().collect::<String>());

        self.title = title_text;
    }
    fn is_keywords_in_html(&self) -> bool {
        if self.keywords.is_none() {
            return true;
        } else if self.clean_text.is_empty() {
            return false;
        }
        for key in self.keywords.clone().unwrap() {
            if !self.clean_text.contains(&key) {
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
            for text_node in element.text() {
                let trimmed = text_node.trim();
                if !trimmed.is_empty() {
                    clean_text.push_str(trimmed);
                    clean_text.push(' ');
                }
            }
        }

        self.clean_text = clean_text.trim().to_string()
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
