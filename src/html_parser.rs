use crate::crawler_error::CrawlerError;
use crate::link_fetcher::NotParsedPageData;
use url::Url;

pub struct ParsedPage {
    pub title: String,
    pub description: Option<String>,
    pub clean_text: String,
    pub h1_headers: Vec<String>,
    pub outbound_links: Vec<Url>,
    pub keywords: Option<Vec<String>>,
}
impl ParsedPage {
    pub fn new() -> ParsedPage {
        ParsedPage {
            title: String::new(),
            description: None,
            clean_text: String::new(),
            h1_headers: vec![],
            outbound_links: vec![],
            keywords: None,
        }
    }
    pub fn parse(&mut self, data: NotParsedPageData) {
        if !self.is_keywords_in_html(&data.content) {
            return;
        }
    }
    fn get_robots(robots: &str) {}
    fn get_utls(cont: &str) {}
    fn get_title(cont: &str) {}
    fn is_keywords_in_html(&self, cont: &str) -> bool {
        if self.keywords.is_none() {
            return true;
        }
        true
    }
    fn get_clean_text(cont: &str) {}
    fn get_description(cont: &str) {}
}
