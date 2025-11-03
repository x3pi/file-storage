use anyhow::Result;
use alloy::{
    primitives::Address,
};
pub struct AppConfig {
    pub rpc_url: String,
    pub contract_address: Address,
    pub private_key: String,
    pub chain_id: u64,
    pub storage_root: String,
    pub enable_event_listener: bool,
    pub address_sign_admin: String
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
            contract_address: std::env::var("CONTRACT_ADDRESS")?.parse()?,
            private_key: std::env::var("PRIVATE_KEY")?,
            chain_id: std::env::var("CHAIN_ID")?.parse()?,
            storage_root: std::env::var("STORAGE_ROOT").unwrap_or_else(|_| "./storage".to_string()),
            enable_event_listener: std::env::var("ENABLE_EVENT_LISTENER")
                .unwrap_or_else(|_| "false".to_string())
                .parse()
                .unwrap_or(false),
            address_sign_admin: std::env::var("ADDRESS_SIGN_ADMIN")?,
        })
    }
}