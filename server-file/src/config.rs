use anyhow::Result;
use alloy::{
    primitives::Address,
};
pub struct AppConfig {
    pub rpc_url: String, // Dùng cho HTTP (eth_call)
    #[allow(dead_code)]
    pub rpc_url_ws: String, // Dùng cho WebSockets (subscribe trong tương lai)
    pub registry_address: Address,
    pub private_key: String,
    pub storage_root: String,
    pub session_timeout_seconds: u64,
    pub wt_addr: String, // WebTransport server address (replaces http_addr)
}
impl AppConfig {
    pub fn from_env() -> Result<Self> {
        // ✅ Load from custom .env file if ENV_FILE is set
        if let Ok(env_file) = std::env::var("ENV_FILE") {
            println!("📄 Loading config from: {}", env_file);
            dotenv::from_filename(&env_file).ok();
        } else {
            dotenv::dotenv().ok();
        }
        Ok(Self {
            rpc_url: std::env::var("RPC_URL")?,
            rpc_url_ws: std::env::var("RPC_URL_WS").unwrap_or_else(|_| std::env::var("RPC_URL").unwrap()),
            registry_address: std::env::var("CONTRACT_ADDRESS")?.parse()?,
            private_key: std::env::var("PRIVATE_KEY")?,
            storage_root: std::env::var("STORAGE_ROOT").unwrap_or_else(|_| "./storage".to_string()),
            session_timeout_seconds: std::env::var("SESSION_TIMEOUT_SECONDS").unwrap_or_else(|_| "900".to_string()).parse()?,
            // Đọc từ HTTP_ADDR để giữ tương thích ngược với .env hiện tại
            wt_addr: std::env::var("HTTP_ADDR").unwrap_or_else(|_| "0.0.0.0:8081".to_string()),
        })
    }
}