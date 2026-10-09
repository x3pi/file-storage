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
    pub valid_contracts_cache: Arc<DashMap<Address, (bool, std::time::Instant)>>,
    pub invalid_download_keys: Arc<DashMap<String, std::time::Instant>>,
    pub http_client: alloy::transports::http::Client,
    pub admin_rate_limiter: Arc<DashMap<std::net::IpAddr, (u32, std::time::Instant)>>,
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
            invalid_download_keys: Arc::new(DashMap::new()),
            http_client,
            admin_rate_limiter: Arc::new(DashMap::new()),
        })
    }

    #[cfg(test)]
    pub fn new_test(storage_root: PathBuf, log_dir: PathBuf) -> Arc<Self> {
        use sha2::{Digest, Sha256};
        let (confirmation_sender, confirmation_receiver) = mpsc::channel(1_000);
        let (upload_batch_sender, upload_batch_receiver) = mpsc::channel(1_000);
        let dummy_key = "0x0000000000000000000000000000000000000000000000000000000000000001";
        let wallet = PrivateKeySigner::from_str(dummy_key).unwrap();
        let http_client = alloy::transports::http::Client::builder().build().unwrap();
        let config = crate::config::AppConfig {
            rpc_url: "http://127.0.0.1:8545".to_string(),
            rpc_url_ws: "ws://127.0.0.1:8546".to_string(),
            registry_address: Address::ZERO,
            private_key: dummy_key.to_string(),
            storage_root: storage_root.to_string_lossy().to_string(),
            session_timeout_seconds: 1800,
            wt_addr: "127.0.0.1:7081".to_string(),
            admin_log_password_hash: hex::encode(Sha256::digest(b"admin123")),
        };

        Arc::new(Self {
            config,
            download_cache: Arc::new(DashMap::new()),
            confirmation_sender,
            upload_file_cache: Arc::new(DashMap::new()),
            file_cache: Arc::new(DashMap::new()),
            confirmation_receiver: Arc::new(Mutex::new(confirmation_receiver)),
            storage_root,
            log_dir,
            wallet,
            init_locks: Arc::new(DashMap::new()),
            task_semaphore: Arc::new(Semaphore::new(3000)),
            chunk_tracker: Arc::new(DashMap::new()),
            upload_batch_sender,
            upload_batch_receiver: Arc::new(Mutex::new(upload_batch_receiver)),
            valid_contracts_cache: Arc::new(DashMap::new()),
            invalid_download_keys: Arc::new(DashMap::new()),
            http_client,
            admin_rate_limiter: Arc::new(DashMap::new()),
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

    /// Check nếu một contract address là hợp lệ bằng cách gọi lên Registry với TTL cache
    /// Trả về Result<bool, String> để phân biệt rõ ràng:
    /// - Ok(true): contract hợp lệ
    /// - Ok(false): contract không hợp lệ (contract rác thật)
    /// - Err(e): lỗi RPC/mạng (không kết luận được)
    pub async fn check_contract_validity(&self, contract_address: Address) -> Result<bool, String> {
        if let Some(entry) = self.valid_contracts_cache.get(&contract_address) {
            let (is_valid, timestamp) = *entry.value();
            // Contract hợp lệ cache lâu (4 giờ) để tránh gọi RPC lặp lại
            // Địa chỉ rác cache ngắn (60 giây) để chặn spam DoS
            let ttl = if is_valid {
                std::time::Duration::from_secs(4 * 3600)
            } else {
                std::time::Duration::from_secs(60)
            };
            if timestamp.elapsed() < ttl {
                return Ok(is_valid);
            }
        }

        // Dùng interface từ file registry_contract.rs
        let url = Url::parse(&self.config.rpc_url)
            .map_err(|e| format!("Invalid RPC URL: {}", e))?;
        let http_transport = Http::with_client(self.http_client.clone(), url);
        let rpc_client = RpcClient::new(http_transport, true);
        let provider = RootProvider::<alloy::network::Ethereum>::new(rpc_client);

        let registry = crate::contracts::registry_contract::Registry::new(self.config.registry_address, provider);
        match registry.isContractValid(contract_address).call().await {
            Ok(result) => {
                let is_valid = result;
                log::info!("Contract {} is_valid: {}", contract_address, is_valid);
                self.valid_contracts_cache.insert(contract_address, (is_valid, std::time::Instant::now()));
                Ok(is_valid)
            }
            Err(e) => {
                log::error!("❌ Error calling registry isContractValid: {}", e);
                Err(e.to_string())
            }
        }
    }

    /// Check nếu một contract address là hợp lệ (helper tương thích ngược)
    pub async fn is_valid_contract(&self, contract_address: Address) -> bool {
        self.check_contract_validity(contract_address).await.unwrap_or(false)
    }

    /// Đồng bộ danh sách contract hợp lệ từ Registry định kỳ
    pub async fn sync_registry_contracts(&self) {
        let url = match Url::parse(&self.config.rpc_url) {
            Ok(u) => u,
            Err(e) => {
                log::error!("❌ [Registry Sync] Invalid RPC URL: {}", e);
                return;
            }
        };
        let http_transport = Http::with_client(self.http_client.clone(), url);
        let rpc_client = RpcClient::new(http_transport, true);
        let provider = RootProvider::<alloy::network::Ethereum>::new(rpc_client);

        let registry = crate::contracts::registry_contract::Registry::new(self.config.registry_address, provider);
        match registry.getRegisteredContracts(alloy::primitives::U256::ZERO, alloy::primitives::U256::from(500)).call().await {
            Ok(contracts) => {
                let now = std::time::Instant::now();
                use std::collections::HashSet;
                let active_set: HashSet<Address> = contracts.iter().copied().collect();

                // 1. Cập nhật các contract active vào cache với TTL mới
                for addr in &contracts {
                    self.valid_contracts_cache.insert(*addr, (true, now));
                }

                // 2. Dọn sạch các contract đã bị deregister khỏi Registry
                self.valid_contracts_cache.retain(|addr, (is_valid, _)| {
                    if *is_valid && !active_set.contains(addr) {
                        log::warn!("🚫 Contract {} was deregistered on Registry. Removing from cache.", addr);
                        false
                    } else {
                        true
                    }
                });

                log::info!("🔄 [Registry Sync] Synced {} active contracts from registry successfully", contracts.len());
            }
            Err(e) => {
                log::warn!("⚠️ [Registry Sync] Failed to fetch registered contracts: {}", e);
            }
        }
    }

    pub async fn write_chunk(&self, file_key: &str, chunk_index: u64, chunk_data: &[u8]) -> Result<(), std::io::Error> {
        let file_key_owned = file_key.to_string();
        let chunk_data_owned = chunk_data.to_vec();
        let file_cache = self.file_cache.clone();
        let storage_root = self.storage_root.clone();

        tokio::task::spawn_blocking(move || {
            if file_key_owned.len() != 64 || !file_key_owned.chars().all(|c| c.is_ascii_hexdigit()) {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "Invalid file key: must be 64 hex characters",
                ));
            }
            let chunk_size = crate::models::CHUNK_SIZE; // 1MB
            if chunk_data_owned.is_empty() || chunk_data_owned.len() > chunk_size as usize {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "Chunk data cannot be empty or exceed 1MB",
                ));
            }

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
                let bin_file = OpenOptions::new()
                    .write(true)
                    .create(true)
                    .open(&bin_path)?;
                let meta_file = OpenOptions::new()
                    .append(true)
                    .create(true)
                    .open(&meta_path)?;
                let new_open_files = crate::models::OpenFiles {
                    bin_file: std::sync::Arc::new(bin_file),
                    meta_file: std::sync::Arc::new(std::sync::Mutex::new(meta_file)),
                };
                file_cache.insert(file_key_owned.clone(), new_open_files.clone());
                new_open_files
            };

            let offset = chunk_index
                .checked_mul(chunk_size)
                .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidInput, "Offset calculation overflowed"))?;
            open_files.bin_file.write_all_at(&chunk_data_owned, offset)?;

            let mut meta_file_guard = open_files.meta_file.lock()
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, format!("Mutex poison error: {}", e)))?;
            meta_file_guard.write_all(format!("{}\n", chunk_index).as_bytes())?;

            Ok(())
        })
        .await
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, format!("Task join error: {}", e)))
        .and_then(|inner_result| inner_result)
    }

    /// Khởi tạo hoặc lấy chunk tracker cho file_key.
    /// Đọc .meta file từ disk trong spawn_blocking để không block tokio runtime.
    pub async fn get_or_init_chunk_tracker(&self, file_key: &str) -> dashmap::mapref::one::RefMut<'_, String, std::collections::HashSet<u64>> {
        if let Some(entry) = self.chunk_tracker.get_mut(file_key) {
            return entry;
        }

        // Đọc .meta file từ disk trong spawn_blocking để không block tokio runtime
        let file_key_owned = file_key.to_string();
        let storage_root = self.storage_root.clone();
        let existing = tokio::task::spawn_blocking(move || {
            let mut set = std::collections::HashSet::new();
            if file_key_owned.len() == 64 && file_key_owned.chars().all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase()) {
                let level1 = &file_key_owned[0..2];
                let level2 = &file_key_owned[2..4];
                let file_dir: PathBuf = storage_root.join(level1).join(level2).join(&file_key_owned);
                let meta_path = file_dir.join(format!("{}.meta", file_key_owned));
                if meta_path.is_file() {
                    if let Ok(content) = std::fs::read_to_string(&meta_path) {
                        for line in content.lines() {
                            if let Ok(idx) = line.trim().parse::<u64>() {
                                set.insert(idx);
                            }
                        }
                    }
                }
            }
            set
        }).await.unwrap_or_default();

        let mut entry = self.chunk_tracker.entry(file_key.to_string()).or_default();
        if !existing.is_empty() {
            entry.extend(existing);
        }
        entry
    }
}
