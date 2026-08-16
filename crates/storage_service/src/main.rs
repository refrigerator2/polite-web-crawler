pub mod storage;
use common::core::crawler_core::CRAWLER_TASK_QUEUE_NAME;
use common::error::crawler_error::CrawlerError;
use common::parsers::parsed_data::ParsedData;
use common::task_queue;
use common::task_queue::task_queue::TaskQueue;
use redis::AsyncCommands;
use redis::streams::{StreamReadOptions, StreamReadReply};
use redis::{self, aio::ConnectionManager};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use storage::crawler_storage::CrawlerStorage;
use url::Url;

const STREAM_NAME: &str = "crawler_events";
const GROUP_NAME: &str = "storage_workers";
const CONSUMER_NAME: &str = "storage_consumer_1";

#[tokio::main]
async fn main() -> Result<(), CrawlerError> {
    let storage = Arc::new(CrawlerStorage::new("crawler.db", "MyBot/1.0".to_string()).await?);
    let client = redis::Client::open("redis://127.0.0.1:6379/")?;
    let manager = ConnectionManager::new(client).await?;
    let task_queue = TaskQueue::new("redis://127.0.0.1:6379", CRAWLER_TASK_QUEUE_NAME, 5.0).await?;
    let mut stream_manager = manager.clone();
    let storage_writer = Arc::clone(&storage);
    let stream_consumer_task = tokio::spawn(async move {
        run_redis_stream_consumer(stream_manager, storage_writer, task_queue).await;
    });
    let storage_reader = Arc::clone(&storage);
    let grpc_server_task = tokio::spawn(async move {
        run_grpc_server("[::1]:50051".parse().unwrap(), storage_reader).await;
    });
    let _ = tokio::try_join!(stream_consumer_task, grpc_server_task)?;

    Ok(())
}

async fn run_redis_stream_consumer(
    mut conn: ConnectionManager,
    storage: Arc<CrawlerStorage>,
    tq: TaskQueue,
) {
    let create_result: redis::RedisResult<()> = conn
        .xgroup_create_mkstream(STREAM_NAME, GROUP_NAME, "$")
        .await;

    if let Err(e) = create_result {
        if !e.to_string().contains("BUSYGROUP") {
            eprintln!("Failed to create consumer group: {}", e);
            return;
        }
    }

    let opts = StreamReadOptions::default()
        .group(GROUP_NAME, CONSUMER_NAME)
        .count(10)
        .block(5000);

    loop {
        let reply: redis::RedisResult<StreamReadReply> =
            conn.xread_options(&[STREAM_NAME], &[">"], &opts).await;

        match reply {
            Ok(reply) => {
                if reply.keys.is_empty() {
                    continue;
                }

                for stream_key in reply.keys {
                    for stream_id in stream_key.ids {
                        match process_stream_entry(&stream_id, &storage).await {
                            Ok(s) => {
                                for url in s {
                                    if let Ok(u) = Url::parse(&url)
                                        && !storage.insert_url_in_seen_urls(&u)
                                    {
                                        for i in 1..=3 {
                                            if let Err(e) = tq.push(u.as_str()).await {
                                                eprintln!(
                                                    "Error during pushing sitemap to task queue: {}",
                                                    e
                                                );
                                                if i == 3 {
                                                    println!(
                                                        "Couldnt push sitemap into task queue"
                                                    );
                                                    break;
                                                }
                                                tokio::time::sleep(Duration::from_millis(200))
                                                    .await;
                                            } else {
                                                break;
                                            }
                                        }
                                    }
                                }
                            }
                            Err(e) => {
                                eprintln!("Failed to process entry {}: {:?}", stream_id.id, e);
                            }
                        }
                        let _: redis::RedisResult<i32> =
                            conn.xack(STREAM_NAME, GROUP_NAME, &[&stream_id.id]).await;
                    }
                }
            }
            Err(e) => {
                eprintln!("Redis stream read error: {}", e);
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
}
async fn process_stream_entry(
    entry: &redis::streams::StreamId,
    storage: &Arc<CrawlerStorage>,
) -> Result<Vec<String>, CrawlerError> {
    let payload: String = match entry.map.get("payload") {
        Some(redis::Value::BulkString(bytes)) => String::from_utf8_lossy(bytes).to_string(),
        _ => {
            eprintln!("Entry {} missing 'payload' field", entry.id);
            return Ok(vec![]);
        }
    };
    let parsed_data: ParsedData = serde_json::from_str(&payload)?;
    let res = storage.save_parsed_data(parsed_data).await?;
    Ok(res)
}

async fn run_grpc_server(link: String, storage: Arc<CrawlerStorage>) {}
