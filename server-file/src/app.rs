use anyhow::Result;
use dashmap::DashMap;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};

use crate::config::AppConfig;
use crate::models::{ConfirmationReceiver, ConfirmationSender, DownloadSessionCache, VerifiedSignatureCache, VerifiedUploadSignatureCache};
// 🔥 THÊM IMPORTS CHO CHANNEL VÀ MUTEX

// Imports cho Alloy
use alloy::{
    providers::ProviderBuilder,
    signers::local::PrivateKeySigner,
};

// Import contract bindings từ file_contract.rs
use crate::file_contract::Files::FilesInstance;

/// App chứa tất cả các thành phần cốt lõi của ứng dụng.
pub struct App {
    pub config: AppConfig,
    // Note: Không còn trường `contract_address` trực tiếp
    pub download_cache: DownloadSessionCache,
    pub verified_signature_cache: VerifiedSignatureCache,
    pub verified_upload_cache:VerifiedUploadSignatureCache,
    pub confirmation_sender: ConfirmationSender,
    pub confirmation_receiver: Arc<Mutex<ConfirmationReceiver>>,
    pub storage_root: PathBuf,
    pub wallet: PrivateKeySigner,
    pub init_locks: Arc<DashMap<String, Arc<Mutex<()>>>>,
}

impl App {

    pub async fn setup() -> Result<Self> {
        let config = AppConfig::from_env()?;
        let storage_root = PathBuf::from(&config.storage_root);
        println!("📁 Storage root: {}", storage_root.display());
        println!("📡 RPC URL: {}", config.rpc_url);

        // Khởi tạo wallet từ private key
        let wallet = PrivateKeySigner::from_str(&config.private_key)?;
        println!("👤 Wallet address: {:?}", wallet.address());

        let contract_address = config.contract_address; // Vẫn cần đọc từ config
        let download_cache: DownloadSessionCache = Arc::new(DashMap::new());
        let verified_signature_cache: VerifiedSignatureCache = Arc::new(DashMap::new());
        let verified_upload_cache: VerifiedUploadSignatureCache = Arc::new(DashMap::new());
        // 🔥 FIX: Sử dụng unbounded_channel để match với models.rs
        let (confirmation_sender, confirmation_receiver) = mpsc::unbounded_channel(); 
        let init_locks = Arc::new(DashMap::new());

        println!("📝 File Contract address: {:?}", contract_address);
        println!("🎉 Application setup completed!\n");

        Ok(Self {
            config,
            download_cache,
            verified_signature_cache,
            confirmation_sender,
            verified_upload_cache,
            // Đảm bảo kiểu dữ liệu khớp
            confirmation_receiver: Arc::new(Mutex::new(confirmation_receiver)),
            storage_root,
            wallet,
            init_locks,
        })
    }
   
    /// Tạo contract instance cho READ operations (view functions)
    pub async fn contract(&self) -> Result<FilesInstance<impl alloy::providers::Provider + Clone>> {
        let provider = ProviderBuilder::new()
            .connect(&self.config.rpc_url)
            .await?;
        // Truy cập contract_address qua config
        Ok(FilesInstance::new(self.config.contract_address, provider))
    }

    /// Tạo contract instance cho WRITE operations (transactions)
    /// Bao gồm signer để có thể gửi transactions
    pub async fn contract_with_signer(&self) -> Result<FilesInstance<impl alloy::providers::Provider + Clone>> {
        use alloy::network::EthereumWallet;
        
        let provider = ProviderBuilder::new()
            .wallet(EthereumWallet::from(self.wallet.clone()))
            .connect(&self.config.rpc_url)
            .await?;
        
        // Truy cập contract_address qua config
        Ok(FilesInstance::new(self.config.contract_address, provider))
    }
}