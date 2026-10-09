use crate::app::App;
use crate::contracts::file_contract::Files::FileStatus;
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
    contract_address: alloy::primitives::Address,
    app: &'a Arc<App>,
    _request_ip: IpAddr,
) -> Result<Ref<'a, String, DownloadSession>, String> {
    let download_key_clean = download_key.trim_start_matches("0x").to_string();
    let timeout_duration = Duration::from_secs(app.config.session_timeout_seconds);

    // 0. Kiểm tra contract hợp lệ
    if !app.is_valid_contract(contract_address).await {
        return Err(format!("Contract {} is not registered or invalid", contract_address));
    }

    // 1. Kiểm tra Negative Cache (chặn spam RPC bằng download key rác)
    if let Some(entry) = app.invalid_download_keys.get(&download_key_clean) {
        if entry.value().elapsed() < Duration::from_secs(30) {
            return Err(format!("Download key '{}' is invalid (cached)", download_key_clean));
        }
    }

    // --- Logic xử lý Timeout (On-Access Expiration) ---
    let mut remove_key = false;
    if let Some(session_ref) = app.download_cache.get(&download_key_clean) {
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
            download_key_clean
        );
        app.download_cache.remove(&download_key_clean);
    }
    if let Some(session_ref) = app.download_cache.get(&download_key_clean) {
        if session_ref.contract_address != contract_address {
            return Err(format!(
                "Download key '{}' belongs to contract {}, but request specified {}",
                download_key_clean, session_ref.contract_address, contract_address
            ));
        }
        return Ok(session_ref);
    }

    // Khóa Mutex (luồng khác sẽ đợi ở đây)
    let lock_arc = {
        let entry = app
            .init_locks
            .entry(download_key_clean.clone())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())));
        Arc::clone(entry.value())
    };
    let _lock = lock_arc.lock().await;

    // RAII guard đảm bảo xoá entry trong init_locks khi thoát khỏi hàm (cả thành công lẫn lỗi)
    struct LockGuard<'b> {
        locks: &'b dashmap::DashMap<String, Arc<tokio::sync::Mutex<()>>>,
        key: &'b str,
    }
    impl<'b> Drop for LockGuard<'b> {
        fn drop(&mut self) {
            self.locks.remove(self.key);
        }
    }
    let _cleanup_guard = LockGuard {
        locks: &app.init_locks,
        key: &download_key_clean,
    };

    // DOUBLE CHECK: Sau khi có lock, kiểm tra lại cache lần nữa
    if let Some(session_ref) = app.download_cache.get(&download_key_clean) {
        if session_ref.contract_address != contract_address {
            return Err(format!(
                "Download key '{}' belongs to contract {}, but request specified {}",
                download_key_clean, session_ref.contract_address, contract_address
            ));
        }
        return Ok(session_ref);
    }

    // ----------- SLOW PATH (CHỈ MỘT LUỒNG CHẠY Ở ĐÂY) -----------
    let download_key_bytes = match hex::decode(&download_key_clean) {
        Ok(b) if b.len() == 32 => b,
        Ok(b) => {
            app.invalid_download_keys.insert(download_key_clean.clone(), std::time::Instant::now());
            return Err(format!(
                "Invalid download key length: expected 32 bytes (64 hex characters), got {} bytes",
                b.len()
            ));
        }
        Err(e) => {
            app.invalid_download_keys.insert(download_key_clean.clone(), std::time::Instant::now());
            return Err(format!("Invalid download key hex: {}", e));
        }
    };

    let canonical_key = hex::encode(&download_key_bytes);
    if download_key_clean != canonical_key {
        return Err(format!(
            "Download key format mismatch: '{}' must match on-chain canonical hex (lowercase 64 chars)",
            download_key_clean
        ));
    }

    let download_key_b256 = B256::from_slice(&download_key_bytes);
    
    // 🔍 DEBUG RPC TIMING
    let rpc_start = std::time::Instant::now();
    
    // Gọi contract để lấy thông tin (RPC CALL - LÀM CHẬM)
    let contract = app
        .contract(contract_address)
        .await
        .map_err(|e| format!("Failed to create contract instance: {}", e))?;

    let session_info = contract
        .getDownloadSessionInfo(download_key_b256)
        .call()
        .await
        .map_err(|e| format!("Failed to get download session info: {}", e))?;
        
    let rpc_mid = rpc_start.elapsed().as_millis();

    if session_info.fileKey == B256::ZERO {
        app.invalid_download_keys.insert(download_key_clean.clone(), std::time::Instant::now());
        return Err(format!(
            "Download key '{}' not found on-chain",
            download_key_clean
        ));
    } else if session_info.isConfirmed == true {
        return Err(format!("Download key '{}' has expired", download_key_clean));
    }

    // 4. Gọi các hàm liên quan đến file concurrently (tiết kiệm thời gian RTT)
    let contract = app.contract(contract_address).await.map_err(|e| e.to_string())?;
    
    let file_info_call = contract.getFileInfo(session_info.fileKey);
    let is_public_call = contract.isPublicFile(session_info.fileKey);
    let whitelist_call = contract.getWhitelist(session_info.fileKey);

    let (file_info_res, is_public_res, whitelist_res) = tokio::join!(
        file_info_call.call(),
        is_public_call.call(),
        whitelist_call.call()
    );

    let file_info_onchain = file_info_res.map_err(|e| format!("Failed to fetch file info: {}", e))?;
    let is_public = is_public_res.map_err(|e| format!("Failed to check if file is public: {}", e))?;
    let whitelist_addresses = whitelist_res.map_err(|e| format!("Failed to get whitelist: {}", e))?;
        
    let rpc_total = rpc_start.elapsed().as_millis();
    if rpc_total > 500 {
        log::warn!("⚠️ [RPC_SLOW] getDownloadSessionInfo mất {}ms, concurrently fetch info mất thêm {}ms. (Total: {}ms) - download_key: {}", 
            rpc_mid, rpc_total - rpc_mid, rpc_total, download_key_clean);
    }

    let current_time_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| format!("System time error: {}", e))?
        .as_secs();
    if file_info_onchain.expireTime <= current_time_secs {
        return Err(format!("Download key has expired"));
    } else if file_info_onchain.status == FileStatus::Deleted {
        return Err(format!("File has been deleted"));
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

    // Chuyển đổi Vec<Address> thành HashSet<Address> để tra cứu nhanh
    let whitelist: std::collections::HashSet<_> = whitelist_addresses.into_iter().collect();

    // BẢO MẬT: Đối với file private, session_user phải là owner hoặc nằm trong whitelist
    if !is_public && session_info.user != file_info_onchain.owner && !whitelist.contains(&session_info.user) {
        log::warn!(
            "❌ Download session unauthorized: user {:?} is neither owner ({:?}) nor in whitelist for private file {}",
            session_info.user,
            file_info_onchain.owner,
            file_key
        );
        return Err(format!(
            "Unauthorized download session: user {:?} is not authorized for private file {}",
            session_info.user,
            file_key
        ));
    }

    // Mở file .bin MỘT LẦN DUY NHẤT cho toàn bộ Session
    let bin_path = file_path.join(format!("{}.bin", file_key));
    let bin_file = std::fs::File::open(&bin_path)
        .map_err(|e| format!("Failed to open .bin file at {:?}: {}", bin_path, e))?;

    let session = DownloadSession {
        file_key: file_key.clone(),
        contract_address,
        remaining_chunks: chunk_count,
        total_chunks: file_info_onchain.totalChunks,
        file_owner: file_info_onchain.owner,
        session_user: session_info.user,
        first_ip: Arc::new(std::sync::OnceLock::new()),
        confirmed_at: None,
        retry_remaining: chunk_count * 3,
        verified_signature: Arc::new(Mutex::new(None)),
        is_public,
        whitelist,
        created_at: std::time::Instant::now(), // Ghi nhận thời điểm bắt đầu tải
        file_handle: Arc::new(bin_file),
    };
    // ✅ Insert vào cache
    app.download_cache.insert(download_key_clean.clone(), session);

    // Trả về session từ cache
    app.download_cache
        .get(&download_key_clean)
        .ok_or_else(|| "Failed to retrieve inserted session".to_string())

    // Lock sẽ tự động được giải phóng (_lock bị drop) khi hàm kết thúc
}

pub async fn count_chunks(file_path: &Path) -> Result<u64, String> {
    list_chunks(file_path).await.map(|v| v.len() as u64)
}

pub async fn list_chunks(file_path: &Path) -> Result<Vec<u64>, String> {
    if !file_path.exists() {
        return Err("File path does not exist".to_string());
    }
    let mut chunks = Vec::new();

    // 1. CÁCH MỚI: Đọc từ file .meta (nếu có)
    if let Some(file_name) = file_path.file_name() {
        let file_key = file_name.to_string_lossy().to_string();
        let meta_path = file_path.join(format!("{}.meta", file_key));
        if meta_path.exists() {
            if let Ok(content) = fs::read_to_string(&meta_path).await {
                for line in content.lines() {
                    if let Ok(index) = line.trim().parse::<u64>() {
                        chunks.push(index);
                    }
                }
            }
            chunks.sort();
            chunks.dedup(); // Loại bỏ trùng lặp nếu có retry
            return Ok(chunks);
        }
    }

    // 2. CÁCH CŨ: Duyệt thư mục tìm các file chunk lẻ
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
            if let Some(file_name_str) = entry.file_name().to_str() {
                match file_name_str.parse::<u64>() {
                    Ok(index) => chunks.push(index),
                    Err(_) => {
                        log::warn!(
                            "Found non-numeric file in chunk directory: {}",
                            file_name_str
                        );
                    }
                }
            }
        }
    }
    chunks.sort();
    Ok(chunks)
}
pub async fn descrease_chunk_count(download_key: &str, app: &Arc<App>) -> Result<u64, String> {
    let download_key_clean = download_key.trim_start_matches("0x").to_string();
    let (remaining, should_confirm, contract_address) = {
        let mut entry = match app.download_cache.entry(download_key_clean.clone()) {
            dashmap::mapref::entry::Entry::Occupied(o) => o,
            dashmap::mapref::entry::Entry::Vacant(_) => {
                return Err("Download session not found".to_string())
            }
        };
        let session = entry.get_mut();
        if session.remaining_chunks > 0 {
            session.remaining_chunks -= 1;
            let rem = session.remaining_chunks;
            let c_addr = session.contract_address;
            (rem, rem == 0, c_addr)
        } else if session.retry_remaining > 0 {
            session.retry_remaining -= 1;
            (session.remaining_chunks, false, session.contract_address)
        } else {
            return Err("No remaining chunks".to_string());
        }
    }; // Lock trên DashMap entry tự động giải phóng ở đây

    if should_confirm {
        let contract_addr_str = contract_address.to_string();
        let sender = app.confirmation_sender.clone();
        let key = download_key_clean.clone();
        tokio::spawn(async move {
            if let Err(e) = sender.send((key, contract_addr_str)).await {
                log::error!("❌ Failed to send to confirmation queue: Channel closed: {}", e);
            }
        });
    }
    Ok(remaining)
}
