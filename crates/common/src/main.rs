use clap::Parser;
use common::core::crawler_core::CrawlerCore;
use common::error::crawler_error::CrawlerError;
use std::sync::Arc;
use url::Url;

const TOKIO_WORKERS: usize = 128;
const DEFAULT_DB_NAME: &str = "crawler";
const DEFAULT_AGENT_NAME: &str = "Aah";

#[derive(Parser, Debug)]
pub struct Args {
    #[arg(short, long, value_parser = parse_url)]
    pub url: Url,
    #[arg(short, long, num_args = 1..)]
    pub keywords: Option<Vec<String>>,
    #[arg(short, long)]
    pub db_name: Option<String>,
    #[arg(short, long)]
    pub tokio_workers: Option<usize>,
    #[arg(short, long)]
    pub agent_name: Option<String>,
    #[arg(short, long)]
    pub limit: Option<u64>,
}

fn parse_url(url: &str) -> Result<Url, String> {
    Url::parse(url).map_err(|e| format!("{e}"))
}

#[tokio::main]
async fn main() -> Result<(), CrawlerError> {
    let args = Args::parse();
    let url = args.url;
    let keywords = Arc::new(args.keywords);
    let db_name = args.db_name.unwrap_or(DEFAULT_DB_NAME.to_string());
    let db_name = format!("sqlite://{}.db/", db_name);
    let tw = args.tokio_workers.unwrap_or(TOKIO_WORKERS);
    let an = args.agent_name.unwrap_or(DEFAULT_AGENT_NAME.to_string());

    let core = CrawlerCore::new(keywords, db_name, tw, an, args.limit).await?;
    core.run(url).await?;
    Ok(())
}
