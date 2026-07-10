use clap::Parser;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;
use url::Url;

const TUSKS_BUF_SIZE: usize = 1000;
const TOKIO_WORKERS: u8 = 8;
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
async fn main() {
    let args = Args::parse();
    let url = args.url;
    let keywords = args.keywords;

    let (tx, rx) = mpsc::channel::<Url>(TUSKS_BUF_SIZE);

    let rx = Arc::new(tokio::sync::Mutex::new(rx));

    tx.send(url).await.unwrap();

    let mut workers = vec![];
    for _ in 0..TOKIO_WORKERS {
        let rx_clone = Arc::clone(&rx);
        let tx_clone = tx.clone();

        let handle = tokio::spawn(async move {
            loop {
                let mut rx_guard = rx_clone.lock().await;

                if let Some(u) = rx_guard.recv().await {
                    drop(rx_guard);
                    //parsing
                } else {
                    break;
                }
            }
        });
        workers.push(handle);
    }
    for worker in workers {
        let _ = worker.await;
    }
}
