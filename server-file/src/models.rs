use alloy::primitives::Address;
use tokio::sync::Mutex;
use serde::{Deserialize, Serialize};
use dashmap::DashMap;
use tokio::sync::mpsc;
use std::{collections::HashSet, net::IpAddr, sync::Arc, time::Instant};

// --- Structs cho giao tiếp client-server ---
pub const CHUNK_SIZE: u64 = 1_048_576;

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "command")]
#[serde(rename_all = "PascalCase")]
pub enum Command {
    UploadChunk { payload: UploadChunkPayload },
    DownloadChunkRequest { payload: DownloadChunkPayload },
    ListChunksRequest { payload: ListChunksPayload },
    GetLogList { payload: Option<()> }, // Lấy danh sách file, không cần payload
    GetLogContent { payload: GetLogContentPayload }, // Lấy nội dung
}

#[derive(Serialize, Deserialize, Debug)]
pub struct UploadChunkPayload {
    pub file_key: String,
    pub chunk_index: u64,
    pub signature: String,
    pub merkle_proof_hashes: Vec<String>, // Array of hex strings (32 bytes each)
    pub merkle_root: String, // Hex string of merkle root (32 bytes)
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
    #[serde(skip_serializing_if = "String::is_empty")]
    pub message: String,
}


#[derive(Debug, Clone)]
pub struct DownloadSession {
    #[allow(dead_code)]
    pub download_key: String,
    #[allow(dead_code)]
    pub file_key: String,
    pub file_owner: Address,
    pub remaining_chunks: u64,
    #[allow(dead_code)]
    pub total_chunks: u64,
    pub first_ip: IpAddr,
    pub retry_remaining:u64,
    pub confirmed_at: Option<Instant>,
    pub verified_signature: Arc<Mutex<Option<String>>>,
    pub is_public: bool,
    pub whitelist: HashSet<Address>,
    pub created_at: Instant, // Lưu thời gian tạo session để Sweeper quét
    pub file_handle: Arc<std::fs::File>,
}
// DashMap: downloadKey -> DownloadSession
pub type DownloadSessionCache = Arc<DashMap<String, DownloadSession>>;

// Cache verified signatures: (download_key + signature) -> verified owner
#[allow(dead_code)]
pub type VerifiedSignatureCache = Arc<DashMap<String, Address>>;

// Upload verification cache: stores both verified address and merkle root
#[derive(Debug, Clone)]
pub struct UploadFileInfo {
    pub verified_address: Address,
    pub merkle_root: String,
    pub total_chunks: u64,
}

pub type UploadFileCache = Arc<DashMap<String, UploadFileInfo>>;
pub type ChunkTracker = Arc<DashMap<String, HashSet<u64>>>;

pub type ConfirmationSender = mpsc::Sender<String>;
pub type ConfirmationReceiver = mpsc::Receiver<String>;

pub type UploadBatchSender = mpsc::Sender<String>;
pub type UploadBatchReceiver = mpsc::Receiver<String>;

#[derive(Clone)]
pub struct OpenFiles {
    pub bin_file: Arc<std::fs::File>,
    pub meta_file: Arc<std::sync::Mutex<std::fs::File>>,
}
pub type FileCache = Arc<DashMap<String, OpenFiles>>;



// --- API  ---
// chunk response
#[derive(Serialize, Deserialize, Debug)]
pub struct ListChunksPayload {
    pub file_key: String,
}
#[derive(Serialize, Deserialize, Debug)]
pub struct ListChunksResponse {
    pub status: String,
    pub message: String,
    pub chunk_indices: Vec<u64>,
}

// logs
// (Thêm struct GetLogsPayload)
#[derive(Serialize, Deserialize, Debug)]
pub struct LogsListResponse {
    pub status: String,
    pub message: String,
    pub available_files: Vec<String>,
}

// --- API 2: Lấy nội dung file ---
#[derive(Serialize, Deserialize, Debug)]
pub struct GetLogContentPayload {
    pub file_name: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct LogFileContent {
    pub file_name: String,
    pub content: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct LogsContentResponse {
    pub status: String,
    pub message: String,
    pub log_content: Option<LogFileContent>,
}