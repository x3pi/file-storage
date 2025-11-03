use alloy::primitives::Address;
use serde::{Deserialize, Serialize};
use dashmap::DashMap;
use tokio::sync::mpsc;
use std::sync::Arc;

// --- Structs cho giao tiếp client-server ---
#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "command")]
#[serde(rename_all = "PascalCase")]
pub enum Command {
    UploadChunk { payload: UploadChunkPayload },
    DownloadChunkRequest { payload: DownloadChunkPayload },
}

#[derive(Serialize, Deserialize, Debug)]
pub struct UploadChunkPayload {
    pub file_key: String,
    pub chunk_index: u64,
    pub chunk_data_base64: String,
    pub signature: String,
}
#[derive(Serialize, Deserialize, Debug)]
pub struct DownloadChunkPayload {
    pub file_key: String,
    pub download_key: String,
    pub chunk_index: u64,
    pub signature: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct GenericResponse {
    pub status: String,
    pub message: String,
}
#[derive(Serialize, Deserialize, Debug)]
pub struct DownloadResponse {
    pub status: String,
    pub message: String,
    pub chunk_data_base64: Option<String>,
}


#[derive(Debug, Clone)]
pub struct DownloadSession {
    pub download_key: String,
    pub file_key: String,
    pub file_owner: Address,
    pub remaining_chunks: u32,
    pub total_chunks: u32,
}
// DashMap: downloadKey -> DownloadSession
pub type DownloadSessionCache = Arc<DashMap<String, DownloadSession>>;

// Cache verified signatures: (download_key + signature) -> verified owner
pub type VerifiedSignatureCache = Arc<DashMap<String, Address>>;
pub type VerifiedUploadSignatureCache = Arc<DashMap<String, Address>>;

pub type ConfirmationSender = mpsc::UnboundedSender<String>;
pub type ConfirmationReceiver = mpsc::UnboundedReceiver<String>;