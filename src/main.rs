use clap::Parser;
use crawler::crawler_core::CrawlerCore;
use crawler::crawler_error::CrawlerError;
use crawler::link_fetcher::LinkFetcher;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;
use url::Url;

#[derive(Parser, Debug)]
pub struct Args {
    #[arg(short, long, value_parser = parse_url)]
    pub url: Url,
    #[arg(short, long, num_args = 1..)]
    pub keywords: Option<Vec<String>>,
}

fn parse_url(url: &str) -> Result<Url, String> {
    Url::parse(url).map_err(|e| format!("{e}"))
}

#[tokio::main]
async fn main() -> Result<(), CrawlerError> {
    let args = Args::parse();
    let url = args.url;
    let keywords = Arc::new(args.keywords);

    let core = CrawlerCore::new(keywords).await?;
    core.run(url).await?;
    Ok(())
}
