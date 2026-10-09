use anyhow::{Context, Result};
use alloy::primitives::Address;
use sha2::{Digest, Sha256};

pub struct AppConfig {
    pub rpc_url: String, // Dùng cho HTTP (eth_call)
    #[allow(dead_code)]
    pub rpc_url_ws: String, // Dùng cho WebSockets (subscribe trong tương lai)
    pub registry_address: Address,
    pub private_key: String,
    pub storage_root: String,
    pub session_timeout_seconds: u64,
    pub wt_addr: String, // WebTransport server address (replaces http_addr)
    pub admin_log_password_hash: String,
}

impl AppConfig {
    pub fn from_env() -> Result<Self> {
        // ✅ Load from custom .env file if ENV_FILE is set
        if let Ok(env_file) = std::env::var("ENV_FILE") {
            println!("📄 Loading config from: {}", env_file);
            let _ = dotenv::from_filename(&env_file);
        } else {
            dotenv::dotenv().ok();
        }

        let raw_pass = std::env::var("ADMIN_LOG_PASSWORD")
            .context("Missing required environment variable: ADMIN_LOG_PASSWORD")?;
        let admin_log_password_hash = hex::encode(Sha256::digest(raw_pass.as_bytes()));

        let rpc_url = std::env::var("RPC_URL")
            .context("Missing required environment variable: RPC_URL")?;
        let rpc_url_ws = std::env::var("RPC_URL_WS")
            .context("Missing required environment variable: RPC_URL_WS")?;
        let contract_address_str = std::env::var("CONTRACT_ADDRESS")
            .context("Missing required environment variable: CONTRACT_ADDRESS")?;
        let registry_address: Address = contract_address_str
            .parse()
            .with_context(|| format!("Invalid CONTRACT_ADDRESS '{}'", contract_address_str))?;
        let private_key = std::env::var("PRIVATE_KEY")
            .context("Missing required environment variable: PRIVATE_KEY")?;
        let storage_root = std::env::var("STORAGE_ROOT")
            .context("Missing required environment variable: STORAGE_ROOT")?;
        let session_timeout_str = std::env::var("SESSION_TIMEOUT_SECONDS")
            .context("Missing required environment variable: SESSION_TIMEOUT_SECONDS")?;
        let session_timeout_seconds: u64 = session_timeout_str
            .parse()
            .with_context(|| format!("Invalid SESSION_TIMEOUT_SECONDS '{}'", session_timeout_str))?;
        let wt_addr = std::env::var("HTTP_ADDR")
            .context("Missing required environment variable: HTTP_ADDR")?;

        Ok(Self {
            rpc_url,
            rpc_url_ws,
            registry_address,
            private_key,
            storage_root,
            session_timeout_seconds,
            wt_addr,
            admin_log_password_hash,
        })
    }
}