use crate::app::App;
use crate::file_contract::Files::FileStatus;
use crate::models::DownloadSession; // 🔥 FIX: Loại bỏ DownloadSessionCache
use alloy::primitives::B256;
use dashmap::mapref::one::Ref;
use tokio::sync::Mutex;
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
    let mut remove_key = false;
    if let Some(session_ref) = app.download_cache.get(download_key) {
        if let Some(confirmed_at) = session_ref.confirmed_at {
            if confirmed_at.elapsed() > timeout_duration {
                remove_key = true;
            }
        }
    }
    
    if remove_key {
        log::info!(
            "Removing expired download key ({}s timeout): {}",
            app.config.session_timeout_seconds,
            download_key
        );
        app.download_cache.remove(download_key);
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
    let download_key_bytes =
        hex::decode(download_key_clean).map_err(|e| format!("Invalid download key hex: {}", e))?;

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
        return Err(format!(
            "Download key '{}' not found on-chain",
            download_key
        ));
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

    // Gọi isPublicFile để kiểm tra xem file có public không
    let is_public = contract
        .isPublicFile(session_info.fileKey)
        .call()
        .await
        .map_err(|e| format!("Failed to check if file is public: {}", e))?;

    // Gọi getWhitelist để lấy danh sách ví được phép tải
    let whitelist_addresses = contract
        .getWhitelist(session_info.fileKey)
        .call()
        .await
        .map_err(|e| format!("Failed to get whitelist: {}", e))?;

    // Chuyển đổi Vec<Address> thành HashSet<Address> để tra cứu nhanh
    let whitelist: std::collections::HashSet<_> = whitelist_addresses.into_iter().collect();

    let session = DownloadSession {
        download_key: download_key.to_string(),
        file_key: file_key.clone(),
        remaining_chunks: chunk_count,
        file_owner: file_info_onchain.owner,
        total_chunks: chunk_count,
        first_ip: request_ip,
        confirmed_at: None,
        retry_remaining: chunk_count * 3,
        verified_signature: Arc::new(Mutex::new(None)),
        is_public,
        whitelist,
        created_at: std::time::Instant::now(), // Ghi nhận thời điểm bắt đầu tải
    };
    // ✅ Insert vào cache
    app.download_cache.insert(download_key.to_string(), session);
    
    // 🧹 DỌN DẸP: Xóa lock khỏi bộ nhớ sau khi khởi tạo xong để tránh rò rỉ RAM (Memory Leak)
    app.init_locks.remove(download_key);

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
            // Lấy tên file
            if let Some(file_name_str) = entry.file_name().to_str() {
                // Parse tên file (là chunk index) sang u64
                match file_name_str.parse::<u64>() {
                    Ok(index) => chunks.push(index),
                    Err(_) => {
                        // Bỏ qua các file không phải là số
                        log::warn!(
                            "Found non-numeric file in chunk directory: {}",
                            file_name_str
                        );
                    }
                }
            }
        }
    }
    // Sắp xếp lại cho dễ nhìn
    chunks.sort();
    Ok(chunks)
}
pub async fn descrease_chunk_count(download_key: &str, app: &Arc<App>) -> Result<u64, String> {
    let (remaining, should_confirm) = {
        let mut entry = match app.download_cache.entry(download_key.to_string()) {
            dashmap::mapref::entry::Entry::Occupied(o) => o,
            dashmap::mapref::entry::Entry::Vacant(_) => {
                return Err("Download session not found".to_string())
            }
        };
        let session = entry.get_mut();
        if session.remaining_chunks > 0 {
            session.remaining_chunks -= 1;
            let rem = session.remaining_chunks;
            (rem, rem == 0)
        } else if session.retry_remaining > 0 {
            session.retry_remaining -= 1;
            (session.remaining_chunks, false)
        } else {
            return Err("No remaining chunks".to_string());
        }
    }; // Lock trên DashMap entry tự động giải phóng ở đây

    if should_confirm {
        // Dùng try_send() cho bounded channel
        if let Err(e) = app.confirmation_sender.try_send(download_key.to_string()) {
            match e {
                tokio::sync::mpsc::error::TrySendError::Full(_) => {
                    log::warn!("⚠️ Confirmation queue full! Falling back to disk for {}", download_key);
                    // Ghi ra file pending trên đĩa cứng
                    let pending_file_path = app.storage_root.join("pending_confirmations.txt");
                    // Dùng tokio::task::spawn để không block luồng hiện tại
                    let key_to_write = download_key.to_string();
                    tokio::spawn(async move {
                        let mut file = match tokio::fs::OpenOptions::new()
                            .create(true)
                            .append(true)
                            .open(&pending_file_path)
                            .await
                        {
                            Ok(f) => f,
                            Err(e) => {
                                log::error!("❌ CRITICAL: Failed to open pending_confirmations.txt: {}", e);
                                return;
                            }
                        };
                        use tokio::io::AsyncWriteExt;
                        let line = format!("{}\n", key_to_write);
                        if let Err(err) = file.write_all(line.as_bytes()).await {
                            log::error!("❌ CRITICAL: Failed to write to pending_confirmations.txt: {}", err);
                        }
                    });
                }
                tokio::sync::mpsc::error::TrySendError::Closed(_) => {
                    log::error!("❌ Failed to send to confirmation queue: Channel closed");
                }
            }
        }
    }
    Ok(remaining)
}
