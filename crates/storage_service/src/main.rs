pub mod storage;
use common::error::crawler_error::CrawlerError;
use redis::{self, aio::ConnectionManager};
use std::sync::Arc;
use storage::crawler_storage::CrawlerStorage;
#[tokio::main]
async fn main() -> Result<(), CrawlerError> {
    let storage = Arc::new(CrawlerStorage::new("crawler.db", "MyBot/1.0".to_string()).await?);
    let client = redis::Client::open("redis://127.0.0.1:6379/")?;
    let manager = ConnectionManager::new(client).await?;

    let mut stream_manager = manager.clone();
    let storage_writer = Arc::clone(&storage);
    let stream_consumer_task = tokio::spawn(async move {
        run_redis_stream_consumer(stream_manager, storage_writer).await;
    });
    let storage_reader = Arc::clone(&storage);
    let grpc_server_task = tokio::spawn(async move {
        run_grpc_server("[::1]:50051".parse().unwrap(), storage_reader).await;
    });
    let _ = tokio::try_join!(stream_consumer_task, grpc_server_task)?;

    Ok(())
}
async fn run_redis_stream_consumer(
    stream_manager: ConnectionManager,
    storage: Arc<CrawlerStorage>,
) {
}
async fn run_grpc_server(link: String, storage: Arc<CrawlerStorage>) {}
