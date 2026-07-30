use crate::config::AppConfig;
use crate::models::{
    ConfirmationReceiver, ConfirmationSender, DownloadSessionCache, UploadFileCache,
    FileCache, ChunkTracker, UploadBatchSender, UploadBatchReceiver,
};
use alloy::primitives::Address;
use anyhow::Result;
use dashmap::DashMap;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::Semaphore;
use tokio::sync::{mpsc, Mutex};

// Imports cho Alloy
use alloy::network::EthereumWallet;
use alloy::providers::{ProviderBuilder, RootProvider};
use alloy::rpc::client::RpcClient;
use alloy::signers::local::PrivateKeySigner;
use alloy::transports::http::Http;
use url::Url;

// Import contract bindings từ file_contract.rs
use crate::contracts::file_contract::Files::FilesInstance;

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
    pub valid_contracts_cache: Arc<DashMap<Address, bool>>,
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
            valid_contracts_cache: Arc::new(DashMap::new()),
            http_client,
        })
    }

    /// Tạo contract instance cho READ operations (view functions)
    pub async fn contract(&self, contract_address: Address) -> Result<FilesInstance<impl alloy::providers::Provider + Clone>> {
        let url = Url::parse(&self.config.rpc_url)?;
        let http_transport = Http::with_client(self.http_client.clone(), url);
        let rpc_client = RpcClient::new(http_transport, true);
        let provider = RootProvider::new(rpc_client);

        Ok(FilesInstance::new(contract_address, provider))
    }

    /// Tạo contract instance cho WRITE operations (transactions)
    /// Bao gồm signer để có thể gửi transactions
    pub async fn contract_with_signer(
        &self,
        contract_address: Address,
    ) -> Result<FilesInstance<impl alloy::providers::Provider + Clone>> {
        let url = Url::parse(&self.config.rpc_url)?;
        let http_transport = Http::with_client(self.http_client.clone(), url);
        let rpc_client = RpcClient::new(http_transport, true);
        let root_provider = RootProvider::<alloy::network::Ethereum>::new(rpc_client);

        let provider = ProviderBuilder::new()
            .wallet(EthereumWallet::from(self.wallet.clone()))
            .connect_provider(root_provider);

        Ok(FilesInstance::new(contract_address, provider))
    }

    /// Check nếu một contract address là hợp lệ bằng cách gọi lên Registry
    pub async fn is_valid_contract(&self, contract_address: Address) -> bool {
        if let Some(is_valid) = self.valid_contracts_cache.get(&contract_address) {
            return *is_valid.value();
        }

        // Dùng interface từ file registry_contract.rs
        let url = match Url::parse(&self.config.rpc_url) {
            Ok(u) => u,
            Err(e) => {
                log::error!("❌ Invalid RPC URL: {}", e);
                return false;
            }
        };
        let http_transport = Http::with_client(self.http_client.clone(), url);
        let rpc_client = RpcClient::new(http_transport, true);
        let provider = RootProvider::<alloy::network::Ethereum>::new(rpc_client);

        let registry = crate::contracts::registry_contract::Registry::new(self.config.registry_address, provider);
        match registry.isContractValid(contract_address).call().await {
            Ok(result) => {
                let is_valid = result;
                if is_valid {
                    log::info!("insert contract: {}", contract_address);
                    self.valid_contracts_cache.insert(contract_address, true);
                }
                is_valid
            }
            Err(e) => {
                log::error!("❌ Error calling registry isContractValid: {}", e);
                false
            }
        }
    }

    pub async fn write_chunk(&self, file_key: &str, chunk_index: u64, chunk_data: &[u8]) -> Result<(), std::io::Error> {
        let file_key_owned = file_key.to_string();
        let chunk_data_owned = chunk_data.to_vec();
        let file_cache = self.file_cache.clone();
        let storage_root = self.storage_root.clone();

        tokio::task::spawn_blocking(move || {
            let level1 = &file_key_owned[0..2];
            let level2 = &file_key_owned[2..4];
            let file_dir: PathBuf = storage_root.join(level1).join(level2).join(&file_key_owned);
            std::fs::create_dir_all(&file_dir)?;

            let bin_path = file_dir.join(format!("{}.bin", file_key_owned));
            let meta_path = file_dir.join(format!("{}.meta", file_key_owned));

            use std::fs::OpenOptions;
            use std::io::Write;
            use std::os::unix::fs::FileExt;

            let open_files = if let Some(files) = file_cache.get(&file_key_owned) {
                files.value().clone()
            } else {
                file_cache.entry(file_key_owned.clone()).or_insert_with(|| {
                    let bin_file = OpenOptions::new()
                        .write(true)
                        .create(true)
                        .open(&bin_path).unwrap();
                    let meta_file = OpenOptions::new()
                        .append(true)
                        .create(true)
                        .open(&meta_path).unwrap();
                    crate::models::OpenFiles {
                        bin_file: std::sync::Arc::new(bin_file),
                        meta_file: std::sync::Arc::new(std::sync::Mutex::new(meta_file)),
                    }
                }).value().clone()
            };

            let chunk_size = 1024 * 1024; // 1MB
            let offset = chunk_index * chunk_size;
            open_files.bin_file.write_all_at(&chunk_data_owned, offset)?;

            let mut meta_file_guard = open_files.meta_file.lock().unwrap();
            meta_file_guard.write_all(format!("{}\n", chunk_index).as_bytes())?;

            Ok(())
        })
        .await
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, format!("Task join error: {}", e)))
        .and_then(|inner_result| inner_result)
    }
}
