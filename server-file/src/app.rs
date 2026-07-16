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
        // [FIX] Tối ưu giới hạn semaphore cho máy 6 Core, 16GB RAM.
        // Cấp 3000 luồng (tốn ~4-6GB RAM buffer), giữ lại ~10GB RAM cho hệ điều hành làm Page Cache đệm ổ cứng.
        let semaphore_limit = 3000;
        let task_semaphore = Arc::new(Semaphore::new(semaphore_limit));
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
        })
    }

    /// Tạo contract instance cho READ operations (view functions)
    pub async fn contract(&self) -> Result<FilesInstance<impl alloy::providers::Provider + Clone>> {
        let provider = ProviderBuilder::new().connect(&self.config.rpc_url).await?;
        // Truy cập contract_address qua config
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
