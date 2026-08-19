use crate::storage::crawler_storage::CrawlerStorage;
use crate::storage_proto;
use common::storage::db::UrlAccess;
use std::sync::Arc;
use storage_proto::storage_service_server::{StorageService, StorageServiceServer};
use storage_proto::{
    CheckUrlRequest, CheckUrlResponse, GetDelayRequest, GetDelayResponse, InsertUrlRequest,
    InsertUrlResponse,
};
use tonic::{Request, Response, Status};
use url::Url;

pub struct StorageGrpcService {
    storage: CrawlerStorage,
}

impl StorageGrpcService {
    pub fn new(storage: CrawlerStorage) -> Self {
        Self { storage }
    }
}
#[tonic::async_trait]
impl StorageService for StorageGrpcService {
    async fn check_url_allowed(
        &self,
        request: Request<CheckUrlRequest>,
    ) -> Result<Response<CheckUrlResponse>, Status> {
        let req = request.into_inner();

        let url = Url::parse(&req.url).map_err(|_| Status::invalid_argument("invalid url"))?;

        let access = self
            .storage
            .check_if_url_allowed(&url)
            .await
            .map_err(|e| Status::internal(format!("{:?}", e)))?;

        let access_str = match access {
            UrlAccess::Allowed => "allowed",
            UrlAccess::Disallowed => "disallowed",
            UrlAccess::UnknownDomain => "unknown_domain",
            UrlAccess::URLWithoutHost => "no_host",
        };

        Ok(Response::new(CheckUrlResponse {
            access: access_str.to_string(),
        }))
    }
    async fn get_delay(
        &self,
        request: Request<GetDelayRequest>,
    ) -> Result<Response<GetDelayResponse>, Status> {
        let req = request.into_inner();
        let res = self
            .storage
            .get_delay(&req.domain)
            .await
            .map_err(|e| Status::internal(format!("{:?}", e)))?;
        Ok(Response::new(GetDelayResponse {
            delay_secs: res.as_secs_f32(),
        }))
    }
    async fn insert_url(
        &self,
        request: Request<InsertUrlRequest>,
    ) -> Result<Response<InsertUrlResponse>, Status> {
        let req = request.into_inner();

        let url = Url::parse(&req.url).map_err(|_| Status::invalid_argument("invalid url"))?;

        let is_duplicate = self.storage.insert_url_in_seen_urls(&url);
        Ok(Response::new(InsertUrlResponse { is_duplicate }))
    }
}
