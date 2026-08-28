use crate::{
    parsers::html_parser::{ParsedPage, convert_parsed_page_to_save_data},
    storage_client::grpc_client::GrpcClient,
};
use common::{
    STREAM_NAME,
    error::crawler_error::CrawlerError,
    network::url_info::{DomainData, UrlAccess},
    parsers::parsed_data::{DomainDataSaveData, ParsedData},
};
use redis::{self, AsyncCommands, aio::ConnectionManager};

#[derive(Clone)]
pub struct StorageClient {
    grpc: GrpcClient,
    conn: ConnectionManager,
}

impl StorageClient {
    pub async fn new(grpc_addr: String, redis_addr: String) -> Result<Self, CrawlerError> {
        let client = redis::Client::open(redis_addr)?;
        Ok(Self {
            grpc: GrpcClient::connect(grpc_addr).await?,
            conn: ConnectionManager::new(client).await?,
        })
    }

    pub async fn save_parsed_page(&self, pp: &ParsedPage) -> Result<String, CrawlerError> {
        let data_from_pp = convert_parsed_page_to_save_data(pp);
        let event = ParsedData::ParsedPage(data_from_pp);
        self.publish_event(&event).await
    }

    pub async fn save_domain_data(&self, dd: &DomainData) -> Result<String, CrawlerError> {
        let data_from_dd = DomainDataSaveData::from_domain_data(dd);
        let event = ParsedData::ParsedDomain(data_from_dd);
        self.publish_event(&event).await
    }

    async fn publish_event(&self, event: &ParsedData) -> Result<String, CrawlerError> {
        let payload = serde_json::to_string(event)?;
        let mut conn_clone = self.conn.clone();
        let id: String = conn_clone
            .xadd(STREAM_NAME, "*", &[("payload", payload.as_str())])
            .await?;
        Ok(id)
    }

    pub async fn check_if_url_allowed(&self, url: &str) -> Result<UrlAccess, CrawlerError> {
        let mut grpc_clone = self.grpc.clone();
        Ok(grpc_clone.check_if_url_allowed(url).await?)
    }

    pub async fn get_delay(&self, domain: &str) -> Result<f32, CrawlerError> {
        let mut grpc_clone = self.grpc.clone();
        Ok(grpc_clone.get_delay(domain).await?)
    }

    pub async fn insert_url(&self, url: &str) -> Result<bool, CrawlerError> {
        let mut grpc_clone = self.grpc.clone();
        Ok(grpc_clone.insert_url(url).await?)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage_client::grpc_client::storage_proto::{
        self,
        storage_service_server::{StorageService, StorageServiceServer},
    };
    use common::network::url_info::NotParsedPageData;
    use std::collections::HashSet;
    use std::sync::Arc;
    use std::sync::Mutex;
    use testcontainers::ContainerAsync;
    use testcontainers::runners::AsyncRunner;
    use testcontainers_modules::redis::Redis;
    use tonic::{Request, Response, Status};
    use url::Url;

    struct FakeStorageService {
        seen: Mutex<HashSet<String>>,
    }

    impl FakeStorageService {
        fn new() -> Self {
            Self {
                seen: Mutex::new(HashSet::new()),
            }
        }
    }

    #[tonic::async_trait]
    impl StorageService for FakeStorageService {
        async fn check_url_allowed(
            &self,
            request: Request<storage_proto::CheckUrlRequest>,
        ) -> Result<Response<storage_proto::CheckUrlResponse>, Status> {
            let url = request.into_inner().url;

            let access = if url.contains("disallowed") {
                storage_proto::UrlAccess::Disallowed
            } else if url.contains("unknown") {
                storage_proto::UrlAccess::UnknownDomain
            } else if url.contains("no-host") {
                storage_proto::UrlAccess::UrlWithoutHost
            } else {
                storage_proto::UrlAccess::Allowed
            };

            Ok(Response::new(storage_proto::CheckUrlResponse {
                access: access as i32,
            }))
        }

        async fn get_delay(
            &self,
            _request: Request<storage_proto::GetDelayRequest>,
        ) -> Result<Response<storage_proto::GetDelayResponse>, Status> {
            Ok(Response::new(storage_proto::GetDelayResponse {
                delay_secs: 2.5,
            }))
        }

        async fn insert_url(
            &self,
            request: Request<storage_proto::InsertUrlRequest>,
        ) -> Result<Response<storage_proto::InsertUrlResponse>, Status> {
            let url = request.into_inner().url;
            let mut seen = self.seen.lock().unwrap();
            let is_duplicate = !seen.insert(url);
            Ok(Response::new(storage_proto::InsertUrlResponse {
                is_duplicate,
            }))
        }
    }

    async fn start_fake_grpc_server() -> std::net::SocketAddr {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("failed to bind");
        let addr = listener.local_addr().expect("failed to get local addr");

        let service = FakeStorageService::new();
        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(StorageServiceServer::new(service))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await
                .expect("fake grpc server failed");
        });

        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        addr
    }

    async fn start_test_environment() -> (ContainerAsync<Redis>, StorageClient) {
        let redis_container = Redis::default()
            .start()
            .await
            .expect("failed to start redis container");
        let port = redis_container
            .get_host_port_ipv4(6379)
            .await
            .expect("failed to get redis port");
        let redis_url = format!("redis://127.0.0.1:{}", port);

        let grpc_addr = start_fake_grpc_server().await;

        let client = StorageClient::new(format!("http://{}", grpc_addr), redis_url)
            .await
            .expect("failed to create storage client");

        (redis_container, client)
    }

    #[tokio::test]
    #[ignore]
    async fn test_check_if_url_allowed_variants() {
        let (_container, client) = start_test_environment().await;

        assert_eq!(
            client
                .check_if_url_allowed("https://allowed.com")
                .await
                .unwrap(),
            UrlAccess::Allowed
        );
        assert_eq!(
            client
                .check_if_url_allowed("https://disallowed.com")
                .await
                .unwrap(),
            UrlAccess::Disallowed
        );
        assert_eq!(
            client
                .check_if_url_allowed("https://unknown.com")
                .await
                .unwrap(),
            UrlAccess::UnknownDomain
        );
        assert_eq!(
            client
                .check_if_url_allowed("https://no-host.com")
                .await
                .unwrap(),
            UrlAccess::URLWithoutHost
        );
    }

    #[tokio::test]
    #[ignore]
    async fn test_get_delay_returns_value_from_server() {
        let (_container, client) = start_test_environment().await;

        let delay = client.get_delay("example.com").await.unwrap();
        assert_eq!(delay, 2.5);
    }

    #[tokio::test]
    #[ignore]
    async fn test_insert_url_first_time_not_duplicate_second_time_is() {
        let (_container, client) = start_test_environment().await;

        let first = client.insert_url("https://example.com/page").await.unwrap();
        assert!(!first, "first insertion should not be a duplicate");

        let second = client.insert_url("https://example.com/page").await.unwrap();
        assert!(second, "second insertion should be a duplicate");
    }

    #[tokio::test]
    #[ignore]
    async fn test_save_parsed_page_publishes_to_stream() {
        let (container, client) = start_test_environment().await;

        let (_, page) = ParsedPage::parse(
            NotParsedPageData {
                content: "<h1>Test</h1>".to_string(),
                url: Url::parse("https://example.com/page").unwrap(),
            },
            Arc::default(),
        );

        let msg_id = client.save_parsed_page(&page).await;
        assert!(msg_id.is_ok());

        let len = stream_len(&container).await;
        assert_eq!(len, 1);
    }

    #[tokio::test]
    #[ignore]
    async fn test_save_domain_data_publishes_to_stream() {
        let (container, client) = start_test_environment().await;

        let domain_data = DomainData {
            domain_string: "example.com".to_string(),
            robots: None,
            delay: 1.0,
        };

        let msg_id = client.save_domain_data(&domain_data).await;
        assert!(msg_id.is_ok());

        let len = stream_len(&container).await;
        assert_eq!(len, 1);
    }

    #[tokio::test]
    #[ignore]
    async fn test_save_multiple_events_accumulate_in_stream() {
        let (container, client) = start_test_environment().await;

        let domain_data = DomainData {
            domain_string: "example.com".to_string(),
            robots: None,
            delay: 1.0,
        };
        client.save_domain_data(&domain_data).await.unwrap();

        let (_, page) = ParsedPage::parse(
            NotParsedPageData {
                content: "<h1>Second</h1>".to_string(),
                url: Url::parse("https://example.com/second").unwrap(),
            },
            Arc::default(),
        );
        client.save_parsed_page(&page).await.unwrap();

        let len = stream_len(&container).await;
        assert_eq!(len, 2, "both events should be in the stream");
    }

    async fn stream_len(container: &ContainerAsync<Redis>) -> usize {
        let port = container
            .get_host_port_ipv4(6379)
            .await
            .expect("failed to get redis port");
        let redis_url = format!("redis://127.0.0.1:{}", port);

        let redis_client = redis::Client::open(redis_url.as_str()).unwrap();
        let mut conn = ConnectionManager::new(redis_client).await.unwrap();
        redis::AsyncCommands::xlen(&mut conn, STREAM_NAME)
            .await
            .unwrap()
    }
}
