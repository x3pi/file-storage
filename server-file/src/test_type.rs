use alloy::providers::RootProvider;
use alloy::transports::http::{Client, Http};
use anyhow::Result;
use url::Url;

pub struct App {
    pub http_client: reqwest::Client,
    pub rpc_url: Url,
}

impl App {
    pub fn contract(&self) -> Result<RootProvider<Http<Client>>> {
        let http_transport = Http::with_client(self.http_client.clone(), self.rpc_url.clone());
        let rpc_client = alloy::rpc::client::RpcClient::new(http_transport, true);
        let provider = RootProvider::new(rpc_client);
        Ok(provider)
    }
}
