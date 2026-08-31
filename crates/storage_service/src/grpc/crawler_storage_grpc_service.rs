use crate::storage::crawler_storage::CrawlerStorage;
use crate::storage_proto;
use common::error::crawler_error::CrawlerError;
use common::network::url_info::UrlAccess as DefaultUrlAccess;
use storage_proto::UrlAccess as ProtoUrlAccess;
use storage_proto::storage_service_server::{StorageService, StorageServiceServer};
use storage_proto::{
    CheckUrlRequest, CheckUrlResponse, GetDelayRequest, GetDelayResponse, InsertUrlRequest,
    InsertUrlResponse,
};
use tonic::{Request, Response, Status};
use url::Url;
impl From<ProtoUrlAccess> for DefaultUrlAccess {
    fn from(access: ProtoUrlAccess) -> Self {
        match access {
            ProtoUrlAccess::Allowed => DefaultUrlAccess::Allowed,
            ProtoUrlAccess::Disallowed => DefaultUrlAccess::Disallowed,
            ProtoUrlAccess::UnknownDomain => DefaultUrlAccess::UnknownDomain,
            ProtoUrlAccess::UrlWithoutHost => DefaultUrlAccess::URLWithoutHost,
        }
    }
}
impl From<DefaultUrlAccess> for ProtoUrlAccess {
    fn from(access: DefaultUrlAccess) -> Self {
        match access {
            DefaultUrlAccess::Allowed => ProtoUrlAccess::Allowed,
            DefaultUrlAccess::Disallowed => ProtoUrlAccess::Disallowed,
            DefaultUrlAccess::UnknownDomain => ProtoUrlAccess::UnknownDomain,
            DefaultUrlAccess::URLWithoutHost => ProtoUrlAccess::UrlWithoutHost,
        }
    }
}
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

        let proto_access: ProtoUrlAccess = access.into();

        Ok(Response::new(CheckUrlResponse {
            access: proto_access as i32,
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
