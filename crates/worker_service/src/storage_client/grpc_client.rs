pub mod storage_proto {
    tonic::include_proto!("storage");
}

use common::error::crawler_error::CrawlerError;
use common::network::url_info::UrlAccess;
use std::time::Duration;
use storage_proto::storage_service_client::StorageServiceClient;
use storage_proto::{
    CheckUrlRequest, GetDelayRequest, InsertUrlRequest, UrlAccess as ProtoUrlAccess,
};
use tonic::transport::{Channel, Endpoint};

impl From<ProtoUrlAccess> for UrlAccess {
    fn from(access: ProtoUrlAccess) -> Self {
        match access {
            ProtoUrlAccess::Allowed => UrlAccess::Allowed,
            ProtoUrlAccess::Disallowed => UrlAccess::Disallowed,
            ProtoUrlAccess::UnknownDomain => UrlAccess::UnknownDomain,
            ProtoUrlAccess::UrlWithoutHost => UrlAccess::URLWithoutHost,
        }
    }
}
#[derive(Clone)]
pub struct GrpcClient {
    client: StorageServiceClient<Channel>,
}

impl GrpcClient {
    pub async fn connect(addr: String) -> Result<Self, CrawlerError> {
        let endpoint = Endpoint::from_shared(addr)?
            .timeout(Duration::from_secs(5))
            .connect_timeout(Duration::from_secs(3));

        let client = StorageServiceClient::connect(endpoint).await?;
        Ok(Self { client })
    }
    pub async fn check_if_url_allowed(&mut self, url: &str) -> Result<UrlAccess, CrawlerError> {
        let req = tonic::Request::new(CheckUrlRequest {
            url: url.to_string(),
        });
        let res = self.client.check_url_allowed(req).await?;
        let access_i32 = res.into_inner().access;
        let access = ProtoUrlAccess::try_from(access_i32)
            .map_err(|_| tonic::Status::internal("unknown UrlAccess value from server"))?;

        Ok(access.into())
    }
    pub async fn get_delay(&mut self, domain: &str) -> Result<f32, CrawlerError> {
        let req = tonic::Request::new(GetDelayRequest {
            domain: domain.to_string(),
        });
        let res = self.client.get_delay(req).await?;
        Ok(res.into_inner().delay_secs)
    }
    pub async fn insert_url(&mut self, url: &str) -> Result<bool, CrawlerError> {
        let req = tonic::Request::new(InsertUrlRequest {
            url: url.to_string(),
        });
        let res = self.client.insert_url(req).await?;
        Ok(res.into_inner().is_duplicate)
    }
}
