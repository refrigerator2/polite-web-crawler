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
use storage::crawler_storage_grpc_service::StorageGrpcService;
use storage_proto::storage_service_server::StorageServiceServer;
use url::Url;

pub mod storage_proto {
    tonic::include_proto!("storage");
}
const STREAM_NAME: &str = "crawler_events";
const GROUP_NAME: &str = "storage_workers";
const CONSUMER_NAME: &str = "storage_consumer_1";

#[tokio::main]
async fn main() -> Result<(), CrawlerError> {
    let storage = CrawlerStorage::new("crawler.db", "MyBot/1.0".to_string()).await?;
    let client = redis::Client::open("redis://127.0.0.1:6379/")?;
    let manager = ConnectionManager::new(client).await?;
    let task_queue = TaskQueue::new("redis://127.0.0.1:6379", CRAWLER_TASK_QUEUE_NAME, 5.0).await?;
    let mut stream_manager = manager.clone();
    let storage_writer = storage.clone();
    let stream_consumer_task = tokio::spawn(async move {
        run_redis_stream_consumer(stream_manager, storage_writer, task_queue).await;
    });
    let storage_reader = storage.clone();
    let grpc_server_task = tokio::spawn(async move {
        run_grpc_server("[::1]:50051".parse().unwrap(), storage_reader).await;
    });
    let _ = tokio::try_join!(stream_consumer_task, grpc_server_task)?;

    Ok(())
}

async fn run_redis_stream_consumer(
    mut conn: ConnectionManager,
    storage: CrawlerStorage,
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
                                let _: redis::RedisResult<i32> =
                                    conn.xack(STREAM_NAME, GROUP_NAME, &[&stream_id.id]).await;
                            }
                            Err(e) => {
                                eprintln!("Failed to process entry {}: {:?}", stream_id.id, e);
                            }
                        }
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
    storage: &CrawlerStorage,
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

pub async fn run_grpc_server(addr: std::net::SocketAddr, storage: CrawlerStorage) {
    let service = StorageGrpcService::new(storage);

    if let Err(e) = tonic::transport::Server::builder()
        .add_service(StorageServiceServer::new(service))
        .serve(addr)
        .await
    {
        eprintln!("gRPC server error: {}", e);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use redis::streams::StreamId;
    use std::collections::HashMap;
    use std::sync::Arc;

    fn make_stream_id(id: &str, payload: &str) -> StreamId {
        let mut map = HashMap::new();
        map.insert(
            "payload".to_string(),
            redis::Value::BulkString(payload.as_bytes().to_vec()),
        );
        StreamId {
            id: id.to_string(),
            map,
            delivered_count: Some(1),
            milliseconds_elapsed_from_delivery: Some(0),
        }
    }

    async fn test_storage() -> CrawlerStorage {
        CrawlerStorage::new("sqlite::memory:", "TestBot/1.0".to_string())
            .await
            .expect("failed to create in-memory storage")
    }

    fn sample_page_event(url: &str) -> ParsedData {
        ParsedData::ParsedPage(common::parsers::parsed_data::ParsedPageSaveData {
            title: Some("Test Title".to_string()),
            description: Some("Test description".to_string()),
            clean_text: Some("Some unique clean text content for dedup".to_string()),
            url: url.to_string(),
        })
    }

    fn sample_domain_event(domain: &str) -> ParsedData {
        ParsedData::ParsedDomain(common::parsers::parsed_data::DomainDataSaveData {
            domain_string: domain.to_string(),
            robots: None,
            delay: 1.0,
        })
    }

    #[tokio::test]
    async fn test_process_stream_entry_missing_payload_returns_empty_ok() {
        let storage = test_storage().await;

        let entry = StreamId {
            id: "1-1".to_string(),
            map: HashMap::new(),
            delivered_count: Some(1),
            milliseconds_elapsed_from_delivery: Some(0),
        };

        let result = process_stream_entry(&entry, &storage).await;

        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_process_stream_entry_invalid_json_returns_err() {
        let storage = test_storage().await;

        let entry = make_stream_id("1-1", "this is not valid json");
        let result = process_stream_entry(&entry, &storage).await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_process_stream_entry_domain_event_saves_domain_and_returns_sitemaps() {
        let storage = test_storage().await;

        let event = sample_domain_event("example.com");
        let payload = serde_json::to_string(&event).expect("serialization failed");
        let entry = make_stream_id("1-1", &payload);

        let result = process_stream_entry(&entry, &storage).await;
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());

        let domain_id = storage.get_domain_id("example.com").await.unwrap();
        assert!(domain_id.is_some());
    }

    #[tokio::test]
    async fn test_process_stream_entry_page_event_requires_known_domain() {
        let storage = test_storage().await;

        let event = sample_page_event("https://unknown-domain.com/page");
        let payload = serde_json::to_string(&event).expect("serialization failed");
        let entry = make_stream_id("1-1", &payload);

        let result = process_stream_entry(&entry, &storage).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_process_stream_entry_page_event_saves_after_domain_known() {
        let storage = test_storage().await;

        let domain_event = sample_domain_event("example.com");
        let domain_payload = serde_json::to_string(&domain_event).unwrap();
        let domain_entry = make_stream_id("1-1", &domain_payload);
        process_stream_entry(&domain_entry, &storage)
            .await
            .expect("domain save should succeed");

        let page_event = sample_page_event("https://example.com/page");
        let page_payload = serde_json::to_string(&page_event).unwrap();
        let page_entry = make_stream_id("1-2", &page_payload);

        let result = process_stream_entry(&page_entry, &storage).await;
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty()); // ParsedPage всегда возвращает vec![]
    }

    #[tokio::test]
    #[ignore]
    async fn test_stream_consumer_end_to_end() {
        use testcontainers::runners::AsyncRunner;
        use testcontainers_modules::redis::Redis;

        let redis_container = Redis::default()
            .start()
            .await
            .expect("failed to start redis container");
        let port = redis_container
            .get_host_port_ipv4(6379)
            .await
            .expect("failed to get redis port");
        let redis_url = format!("redis://127.0.0.1:{}", port);

        let client = redis::Client::open(redis_url.as_str()).expect("bad redis url");
        let conn = ConnectionManager::new(client)
            .await
            .expect("failed to connect to redis");

        let storage = test_storage().await;
        let tq = TaskQueue::new(&redis_url, "test_tasks_queue", 1.0)
            .await
            .expect("failed to create task queue");

        let mut publish_conn = conn.clone();
        let domain_event = sample_domain_event("example.com");
        let domain_payload = serde_json::to_string(&domain_event).unwrap();
        let _: String = publish_conn
            .xadd(STREAM_NAME, "*", &[("payload", domain_payload.as_str())])
            .await
            .expect("failed to xadd domain event");

        let page_event = sample_page_event("https://example.com/page");
        let page_payload = serde_json::to_string(&page_event).unwrap();
        let _: String = publish_conn
            .xadd(STREAM_NAME, "*", &[("payload", page_payload.as_str())])
            .await
            .expect("failed to xadd page event");

        let consumer_conn = conn.clone();
        let storage_clone = storage.clone();
        let handle = tokio::spawn(async move {
            run_redis_stream_consumer(consumer_conn, storage_clone, tq).await;
        });

        tokio::time::sleep(Duration::from_secs(3)).await;
        handle.abort();

        let domain_id = storage.get_domain_id("example.com").await.unwrap();
        assert!(domain_id.is_some(), "domain should have been saved");

        let mut check_conn = conn.clone();
        let pending: redis::streams::StreamPendingReply = check_conn
            .xpending(STREAM_NAME, GROUP_NAME)
            .await
            .expect("xpending failed");

        match pending {
            redis::streams::StreamPendingReply::Empty => {}
            redis::streams::StreamPendingReply::Data(data) => {
                assert_eq!(data.count, 0, "messages should be acked, but still pending");
            }
            _ => unreachable!("unexpected StreamPendingReply variant"),
        }
    }

    async fn start_test_grpc_server(storage: CrawlerStorage) -> std::net::SocketAddr {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("failed to bind");
        let addr = listener.local_addr().expect("failed to get local addr");

        let service = StorageGrpcService::new(storage);
        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(StorageServiceServer::new(service))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await
                .expect("grpc server failed");
        });

        tokio::time::sleep(Duration::from_millis(200)).await;
        addr
    }

    #[tokio::test]
    async fn test_grpc_check_url_allowed_unknown_domain() {
        let storage = test_storage().await;
        let addr = start_test_grpc_server(storage).await;

        let mut client = storage_proto::storage_service_client::StorageServiceClient::connect(
            format!("http://{}", addr),
        )
        .await
        .expect("failed to connect to grpc server");

        let response = client
            .check_url_allowed(storage_proto::CheckUrlRequest {
                url: "https://never-seen-domain.com".to_string(),
            })
            .await
            .expect("grpc call failed");

        assert_eq!(response.into_inner().access, "unknown_domain");
    }

    #[tokio::test]
    async fn test_grpc_check_url_allowed_invalid_url() {
        let storage = test_storage().await;
        let addr = start_test_grpc_server(storage).await;

        let mut client = storage_proto::storage_service_client::StorageServiceClient::connect(
            format!("http://{}", addr),
        )
        .await
        .expect("failed to connect to grpc server");

        let response = client
            .check_url_allowed(storage_proto::CheckUrlRequest {
                url: "not a valid url".to_string(),
            })
            .await;

        assert!(response.is_err());
        assert_eq!(response.unwrap_err().code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn test_grpc_get_delay_unknown_domain_returns_default() {
        let storage = test_storage().await;
        let addr = start_test_grpc_server(storage).await;

        let mut client = storage_proto::storage_service_client::StorageServiceClient::connect(
            format!("http://{}", addr),
        )
        .await
        .expect("failed to connect to grpc server");

        let response = client
            .get_delay(storage_proto::GetDelayRequest {
                domain: "unknown-domain.com".to_string(),
            })
            .await
            .expect("grpc call failed");

        assert_eq!(response.into_inner().delay_secs, 1.0);
    }

    #[tokio::test]
    async fn test_grpc_insert_url_first_time_not_duplicate() {
        let storage = test_storage().await;
        let addr = start_test_grpc_server(storage).await;

        let mut client = storage_proto::storage_service_client::StorageServiceClient::connect(
            format!("http://{}", addr),
        )
        .await
        .expect("failed to connect to grpc server");

        let response = client
            .insert_url(storage_proto::InsertUrlRequest {
                url: "https://example.com/first-time".to_string(),
            })
            .await
            .expect("grpc call failed");

        let is_duplicate = response.into_inner().is_duplicate;
        assert!(!is_duplicate, "first insertion should not be a duplicate");
    }

    #[tokio::test]
    async fn test_grpc_insert_url_second_time_is_duplicate() {
        let storage = test_storage().await;
        let addr = start_test_grpc_server(storage).await;

        let mut client = storage_proto::storage_service_client::StorageServiceClient::connect(
            format!("http://{}", addr),
        )
        .await
        .expect("failed to connect to grpc server");

        let url = "https://example.com/repeated";

        let first = client
            .insert_url(storage_proto::InsertUrlRequest {
                url: url.to_string(),
            })
            .await
            .expect("grpc call failed")
            .into_inner();
        assert!(!first.is_duplicate);

        let second = client
            .insert_url(storage_proto::InsertUrlRequest {
                url: url.to_string(),
            })
            .await
            .expect("grpc call failed")
            .into_inner();

        assert!(
            second.is_duplicate,
            "second insertion should be a duplicate"
        );
    }

    #[tokio::test]
    async fn test_grpc_insert_url_invalid_url() {
        let storage = test_storage().await;
        let addr = start_test_grpc_server(storage).await;

        let mut client = storage_proto::storage_service_client::StorageServiceClient::connect(
            format!("http://{}", addr),
        )
        .await
        .expect("failed to connect to grpc server");

        let response = client
            .insert_url(storage_proto::InsertUrlRequest {
                url: "not a valid url".to_string(),
            })
            .await;

        assert!(response.is_err());
        assert_eq!(response.unwrap_err().code(), tonic::Code::InvalidArgument);
    }
}
