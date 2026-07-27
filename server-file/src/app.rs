use crate::config::AppConfig;
use crate::models::{
    ConfirmationReceiver, ConfirmationSender, DownloadSessionCache, UploadFileCache,
    FileCache, ChunkTracker, UploadBatchSender, UploadBatchReceiver,
};
use anyhow::Result;
use dashmap::DashMap;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::Semaphore;
use tokio::sync::{mpsc, Mutex};

// Imports cho Alloy
use alloy::{providers::ProviderBuilder, signers::local::PrivateKeySigner};

// Import contract bindings từ file_contract.rs
use crate::file_contract::Files::FilesInstance;

/// App chứa tất cả các thành phần cốt lõi của ứng dụng.
pub struct App {
    pub config: AppConfig,
    pub download_cache: DownloadSessionCache,
    pub upload_file_cache: UploadFileCache,
    pub file_cache: FileCache,
    pub confirmation_sender: ConfirmationSender,
    pub confirmation_receiver: Arc<Mutex<ConfirmationReceiver>>,
    pub storage_root: PathBuf,
    pub log_dir: PathBuf,
    pub wallet: PrivateKeySigner,
    pub init_locks: Arc<DashMap<String, Arc<Mutex<()>>>>,
    pub task_semaphore: Arc<Semaphore>,
    pub chunk_tracker: ChunkTracker,
    pub upload_batch_sender: UploadBatchSender,
    pub upload_batch_receiver: Arc<Mutex<UploadBatchReceiver>>,
    pub http_client: alloy::transports::http::Client,
}

impl App {
    pub async fn setup(log_dir: PathBuf) -> Result<Self> {
        let config = AppConfig::from_env()?;
        let storage_root = PathBuf::from(&config.storage_root);
        let wallet = PrivateKeySigner::from_str(&config.private_key)?;
        let download_cache: DownloadSessionCache = Arc::new(DashMap::new());
        let upload_file_cache: UploadFileCache = Arc::new(DashMap::new());
        let file_cache: FileCache = Arc::new(DashMap::new());
        let chunk_tracker: ChunkTracker = Arc::new(DashMap::new());
        let (confirmation_sender, confirmation_receiver) = mpsc::channel(1_000);
        let (upload_batch_sender, upload_batch_receiver) = mpsc::channel(1_000);
        let init_locks = Arc::new(DashMap::new());
        // [LOAD TEST] Bỏ giới hạn luồng để kiểm thử tải tối đa.
        // Dùng Semaphore::MAX_PERMITS để không giới hạn số luồng đồng thời.
        // let semaphore_limit = tokio::sync::Semaphore::MAX_PERMITS;
        let semaphore_limit = 3000;
        let task_semaphore = Arc::new(Semaphore::new(semaphore_limit));
        
        // Khởi tạo HTTP Client 1 lần duy nhất để dùng chung Connection Pool (tránh lỗi TCP Handshake 700ms)
        let http_client = alloy::transports::http::Client::builder()
            .pool_idle_timeout(std::time::Duration::from_secs(60))
            .pool_max_idle_per_host(1000)
            .build()?;

        Ok(Self {
            config,
            download_cache,
            confirmation_sender,
            upload_file_cache,
            file_cache,
            confirmation_receiver: Arc::new(Mutex::new(confirmation_receiver)),
            storage_root,
            log_dir,
            wallet,
            init_locks,
            task_semaphore,
            chunk_tracker,
            upload_batch_sender,
            upload_batch_receiver: Arc::new(Mutex::new(upload_batch_receiver)),
            http_client,
        })
    }

    /// Tạo contract instance cho READ operations (view functions)
    pub async fn contract(&self) -> Result<FilesInstance<impl alloy::providers::Provider + Clone>> {
        use alloy::providers::RootProvider;
        use alloy::transports::http::Http;
        use alloy::rpc::client::RpcClient;
        use url::Url;

        let url = Url::parse(&self.config.rpc_url)?;
        let http_transport = Http::with_client(self.http_client.clone(), url);
        let rpc_client = RpcClient::new(http_transport, true);
        let provider = RootProvider::new(rpc_client);

        Ok(FilesInstance::new(self.config.contract_address, provider))
    }

    /// Tạo contract instance cho WRITE operations (transactions)
    /// Bao gồm signer để có thể gửi transactions
    pub async fn contract_with_signer(
        &self,
    ) -> Result<FilesInstance<impl alloy::providers::Provider + Clone>> {
        use alloy::network::EthereumWallet;

        let provider = ProviderBuilder::new()
            .wallet(EthereumWallet::from(self.wallet.clone()))
            .connect(&self.config.rpc_url)
            .await?;

        // Truy cập contract_address qua config
        Ok(FilesInstance::new(self.config.contract_address, provider))
    }
}
