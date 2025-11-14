use crate::app::App;
use crate::file_contract::Files::FileStatus;
use crate::models::DownloadSession; // 🔥 FIX: Loại bỏ DownloadSessionCache
use alloy::primitives::B256;
use dashmap::mapref::one::Ref;
use futures_util::lock::Mutex;
use std::net::IpAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::fs;

pub async fn initialize_download_session<'a>(
    download_key: &str,
    app: &'a Arc<App>,
    request_ip: IpAddr,
) -> Result<Ref<'a, String, DownloadSession>, String> {
   let timeout_duration = Duration::from_secs(app.config.session_timeout_seconds);

    // --- Logic xử lý Timeout (On-Access Expiration) ---
    if let Some(session_ref) = app.download_cache.get(download_key) {
        if let Some(confirmed_at) = session_ref.confirmed_at {
            if confirmed_at.elapsed() > timeout_duration {
                log::info!("Removing expired download key ({}s timeout): {}", app.config.session_timeout_seconds, download_key);
                app.download_cache.remove(download_key);
            }
        }
    }
    if let Some(session_ref) = app.download_cache.get(download_key) {
        return Ok(session_ref);
    }
    // Khóa Mutex (luồng khác sẽ đợi ở đây)
    let lock_arc = {
        let entry = app
            .init_locks
            .entry(download_key.to_string())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())));
        Arc::clone(entry.value())
    };
    // Khóa Mutex (luồng khác sẽ đợi ở đây)
    let _lock = lock_arc.lock().await;
    // DOUBLE CHECK: Sau khi có lock, kiểm tra lại cache lần nữa
    if let Some(session_ref) = app.download_cache.get(download_key) {
        return Ok(session_ref);
    }
    
    // ----------- SLOW PATH (CHỈ MỘT LUỒNG CHẠY Ở ĐÂY) -----------
    let download_key_clean = download_key.trim_start_matches("0x");
    let download_key_bytes = hex::decode(download_key_clean)
        .map_err(|e| format!("Invalid download key hex: {}", e))?;

    if download_key_bytes.len() != 32 {
        return Err(format!(
            "Invalid download key length: expected 32 bytes, got {} bytes",
            download_key_bytes.len()
        ));
    }

    let download_key_b256 = B256::from_slice(&download_key_bytes);
    // Gọi contract để lấy thông tin (RPC CALL - LÀM CHẬM)
    let contract = app
        .contract()
        .await
        .map_err(|e| format!("Failed to create contract instance: {}", e))?;
        
    let session_info = contract
        .getDownloadSessionInfo(download_key_b256)
        .call()
        .await
        .map_err(|e| format!("Failed to get download session info: {}", e))?;
    if session_info.fileKey == B256::ZERO {
        return Err(format!("Download key '{}' not found on-chain", download_key));
    } else if session_info.isConfirmed == true {
        return Err(format!("Download key '{}' has expired", download_key));
    } 

    let file_info_onchain = contract
        .getFileInfo(session_info.fileKey)
        .call()
        .await
        .map_err(|e| format!("Failed to fetch file info: {}", e))?;
    let current_time_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| format!("System time error: {}", e))?
        .as_secs();
   if file_info_onchain.expireTime <= current_time_secs {
        return Err(format!("Download key has expired"));
    } else if file_info_onchain.status == FileStatus::Deleted {
        return Err(format!("File  has been deleted"));
    }
    // Đường dẫn file
    let file_key = hex::encode(session_info.fileKey);
    let level1 = &file_key[0..2];
    let level2 = &file_key[2..4];
    let file_path = Path::new(&app.config.storage_root)
        .join(level1)
        .join(level2)
    .join(&file_key);

    // Đếm số chunks
    let chunk_count = count_chunks(&file_path)
        .await
        .map_err(|e| format!("Failed to count chunks: {}", e))?;
    let session = DownloadSession {
        download_key: download_key.to_string(),
        file_key: file_key.clone(),
        remaining_chunks: chunk_count ,
        file_owner: file_info_onchain.owner,
        total_chunks: chunk_count,
        first_ip: request_ip,
        confirmed_at: None,
        retry_remaining: chunk_count *3,
        verified_signature: Arc::new(Mutex::new(None)),
    };
    // ✅ Insert vào cache
    app.download_cache.insert(download_key.to_string(), session);
    
    // Trả về session từ cache
    app.download_cache
        .get(download_key)
        .ok_or_else(|| "Failed to retrieve inserted session".to_string())

    // Lock sẽ tự động được giải phóng (_lock bị drop) khi hàm kết thúc
}

pub async fn count_chunks(file_path: &Path) -> Result<u64, String> {
    if !file_path.exists() {
        return Err("File path does not exist".to_string());
    }

    let mut count = 0u64;
    let mut entries = fs::read_dir(file_path)
        .await
        .map_err(|e| format!("Failed to read directory: {}", e))?;

    while let Some(entry) = entries
        .next_entry()
        .await
        .map_err(|e| format!("Failed to read entry: {}", e))?
    {
        if entry
            .file_type()
            .await
            .map_err(|e| format!("Failed to get file type: {}", e))?
            .is_file()
        {
            count += 1;
        }
    }
    Ok(count)
}
pub async fn list_chunks(file_path: &Path) -> Result<Vec<u64>, String> {
    if !file_path.exists() {
        return Err("File path does not exist".to_string());
    }
    let mut chunks = Vec::new();
    let mut entries = fs::read_dir(file_path)
        .await
        .map_err(|e| format!("Failed to read directory: {}", e))?;
    while let Some(entry) = entries.next_entry().await.map_err(|e| format!("Failed to read entry: {}", e))? {
        if entry.file_type().await.map_err(|e| format!("Failed to get file type: {}", e))?.is_file() {
            // Lấy tên file
            if let Some(file_name_str) = entry.file_name().to_str() {
                // Parse tên file (là chunk index) sang u64
                match file_name_str.parse::<u64>() {
                    Ok(index) => chunks.push(index),
                    Err(_) => {
                        // Bỏ qua các file không phải là số
                        log::warn!("Found non-numeric file in chunk directory: {}", file_name_str);
                    }
                }
            }
        }
    }
    // Sắp xếp lại cho dễ nhìn
    chunks.sort();
    Ok(chunks)
}
pub fn descrease_chunk_count(download_key: &str, app: &Arc<App>) -> Result<u64, String> {
    // if let Some(mut session) = app.download_cache.get_mut(download_key) {
    let mut entry = match app.download_cache.entry(download_key.to_string()) {
        dashmap::mapref::entry::Entry::Occupied(o) => o,
        dashmap::mapref::entry::Entry::Vacant(_) => {
            return Err("Download session not found".to_string())
        }
    };
    let session = entry.get_mut();
    if session.remaining_chunks > 0 {
        session.remaining_chunks -= 1;
        let remaining = session.remaining_chunks;
        if remaining == 0 {
            // ✅ FIX: `send()` trên UnboundedSender trả về Result (đã fix trong app.rs)
            if let Err(e) = app.confirmation_sender.send(download_key.to_string()) {
                println!("❌ Failed to send to confirmation queue: {:?}", e);
            }
        }
        return Ok(remaining);
    } else if session.retry_remaining > 0 {
        session.retry_remaining -= 1;
        let remaining = session.remaining_chunks;
        return Ok(remaining);
    }  else {
        return Err("No remaining chunks".to_string());
    }
}