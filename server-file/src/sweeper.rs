use crate::app::App;
use std::sync::Arc;

/// Khởi chạy Bác Quét Rác chạy ngầm
/// Nhiệm vụ: Xóa các DownloadSession bị bỏ hoang khỏi RAM
pub fn spawn_background_sweeper(app: Arc<App>) {
    tokio::spawn(async move {
        let mut last_session_sweep = std::time::Instant::now();
        loop {
            // Ngủ 60 giây giữa các lần quét
            tokio::time::sleep(tokio::time::Duration::from_secs(60)).await;
            
            let now = std::time::Instant::now();

            // 1. Dọn dẹp Invalid Download Keys (hết hạn sau 60 giây)
            let mut expired_invalid_keys = Vec::new();
            for entry in app.invalid_download_keys.iter() {
                if now.duration_since(*entry.value()).as_secs() > 60 {
                    expired_invalid_keys.push(entry.key().clone());
                }
            }
            for key in &expired_invalid_keys {
                app.invalid_download_keys.remove(key);
            }

            // 2. Dọn dẹp Download Session đã hoàn tất (sau 60 giây ân hạn để đóng file handle và giải phóng RAM)
            let mut confirmed_download_keys = Vec::new();
            for entry in app.download_cache.iter() {
                if let Some(confirmed_at) = entry.value().confirmed_at {
                    if now.duration_since(confirmed_at).as_secs() > 60 {
                        confirmed_download_keys.push(entry.key().clone());
                    }
                }
            }
            if !confirmed_download_keys.is_empty() {
                for key in &confirmed_download_keys {
                    app.download_cache.remove(key); // Xóa session -> tự động drop và đóng file_handle .bin
                }
                log::info!("🧹 Đã dọn dẹp {} download session hoàn tất (đã đóng file handle).", confirmed_download_keys.len());
            }

            // 3. Dọn dẹp Download/Upload Session bỏ hoang (mỗi 1 giờ quét 1 lần)
            if now.duration_since(last_session_sweep).as_secs() < 3600 {
                continue;
            }
            last_session_sweep = now;
            
            // Dọn dẹp Download Session
            let mut expired_download_keys = Vec::new();
            // Quét các session đã nằm trong RAM quá 4h (14400 giây)
            for entry in app.download_cache.iter() {
                if now.duration_since(entry.created_at).as_secs() > 14400 {
                    expired_download_keys.push(entry.key().clone());
                }
            }
            if !expired_download_keys.is_empty() {
                for key in &expired_download_keys {
                    app.download_cache.remove(key);
                }
                log::info!("🧹 Đã dọn dẹp xong {} download session bỏ hoang quá 4 giờ khỏi RAM.", expired_download_keys.len());
            }

            // 2. Dọn dẹp Upload Session
            let mut expired_upload_entries = Vec::new();
            for entry in app.upload_file_cache.iter() {
                if now.duration_since(entry.created_at).as_secs() > 14400 {
                    expired_upload_entries.push((entry.key().clone(), entry.contract_address));
                }
            }
            if !expired_upload_entries.is_empty() {
                for (file_key, contract_addr) in &expired_upload_entries {
                    let mut should_delete_disk = false;
                    let decoded = hex::decode(file_key.trim_start_matches("0x")).unwrap_or_default();
                    if decoded.len() == 32 {
                        let mut key_bytes = [0u8; 32];
                        key_bytes.copy_from_slice(&decoded);
                        if let Ok(contract) = app.contract(*contract_addr).await {
                            if let Ok(result) = contract.getFileInfo(alloy::primitives::B256::from(key_bytes)).call().await {
                                use crate::contracts::file_contract::Files::FileStatus;
                                match result.status {
                                    FileStatus::Active => {
                                        log::info!("🧹 File {} is Active on-chain. Cleaning RAM cache only, preserving disk data.", file_key);
                                        should_delete_disk = false;
                                    }
                                    FileStatus::Deleted => {
                                        log::info!("🧹 File {} is Deleted on-chain. Deleting disk data.", file_key);
                                        should_delete_disk = true;
                                    }
                                    FileStatus::Processing => {
                                        let now_secs = std::time::SystemTime::now()
                                            .duration_since(std::time::UNIX_EPOCH)
                                            .map(|d| d.as_secs())
                                            .unwrap_or(0);
                                        if result.expireTime <= now_secs {
                                            log::info!("🧹 File {} expired while Processing. Deleting disk data.", file_key);
                                            should_delete_disk = true;
                                        } else {
                                            log::info!("⏳ File {} is still Processing and not expired. Keeping disk data.", file_key);
                                            should_delete_disk = false;
                                        }
                                    }
                                    _ => {}
                                }
                            }
                        }
                    }

                    app.upload_file_cache.remove(file_key);
                    app.chunk_tracker.remove(file_key);
                    app.file_cache.remove(file_key);
                    
                    if should_delete_disk && file_key.len() >= 4 {
                        let level1 = &file_key[0..2];
                        let level2 = &file_key[2..4];
                        let file_dir = app.storage_root.join(level1).join(level2).join(file_key);
                        if file_dir.exists() {
                            if let Err(e) = std::fs::remove_dir_all(&file_dir) {
                                log::warn!("⚠️ Lỗi khi xóa thư mục rác của upload session {}: {}", file_key, e);
                            }
                        }
                    }
                }
                log::info!("🧹 Đã dọn dẹp xong {} upload session bỏ hoang quá 4 giờ khỏi RAM.", expired_upload_entries.len());
            }
        }
    });
}

pub fn spawn_confirmation_retry_worker(app: Arc<App>) {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(tokio::time::Duration::from_secs(120)).await; // Chạy mỗi 2 phút

            let pending_file = app.storage_root.join("pending_confirmations.txt");
            let processing_file = app.storage_root.join("processing_confirmations.txt");

            if !pending_file.exists() {
                continue;
            }

            // Đổi tên file để tránh xung đột với các task đang ghi
            if let Err(e) = tokio::fs::rename(&pending_file, &processing_file).await {
                log::error!("❌ Failed to rename pending file: {}", e);
                continue;
            }

            // Đọc file
            match tokio::fs::read_to_string(&processing_file).await {
                Ok(content) => {
                    let mut count = 0;
                    for line in content.lines() {
                        let parts: Vec<&str> = line.trim().split(',').collect();
                        if parts.len() == 2 {
                            let key = parts[0].trim().to_string();
                            let addr = parts[1].trim().to_string();
                            if !key.is_empty() && !addr.is_empty() {
                                // Dùng send().await để hút từ từ vào queue, nếu queue đang đầy thì worker sẽ chờ ở đây, điều tiết lưu lượng
                                if let Err(e) = app.confirmation_sender.send((key, addr)).await {
                                    log::error!("❌ Worker failed to send to confirmation queue: {:?}", e);
                                } else {
                                    count += 1;
                                }
                            }
                        }
                    }
                    if count > 0 {
                        log::info!("♻️ Đã khôi phục thành công {} confirmations từ ổ đĩa vào hàng đợi.", count);
                    }
                }
                Err(e) => {
                    log::error!("❌ Failed to read processing file: {}", e);
                }
            }

            // Xóa file sau khi xử lý xong (dù thành công hay thất bại đọc nội dung)
            if let Err(e) = tokio::fs::remove_file(&processing_file).await {
                log::error!("❌ Failed to remove processing file: {}", e);
            }
        }
    });
}

pub fn spawn_upload_retry_worker(app: Arc<App>) {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(tokio::time::Duration::from_secs(120)).await; // Chạy mỗi 2 phút

            let pending_file = app.storage_root.join("pending_uploads.txt");
            let processing_file = app.storage_root.join("processing_uploads.txt");

            if !pending_file.exists() {
                continue;
            }

            if let Err(e) = tokio::fs::rename(&pending_file, &processing_file).await {
                log::error!("❌ Failed to rename pending uploads file: {}", e);
                continue;
            }

            match tokio::fs::read_to_string(&processing_file).await {
                Ok(content) => {
                    let mut count = 0;
                    for line in content.lines() {
                        let parts: Vec<&str> = line.trim().split(',').collect();
                        if parts.len() == 2 {
                            let key = parts[0].trim().to_string();
                            if let Ok(addr) = parts[1].trim().parse::<alloy::primitives::Address>() {
                                if !key.is_empty() {
                                    if let Err(e) = app.upload_batch_sender.send((key, addr)).await {
                                        log::error!("❌ Worker failed to send to upload batch queue: {:?}", e);
                                    } else {
                                        count += 1;
                                    }
                                }
                            }
                        }
                    }
                    if count > 0 {
                        log::info!("♻️ Đã khôi phục thành công {} uploads từ ổ đĩa vào hàng đợi.", count);
                    }
                }
                Err(e) => {
                    log::error!("❌ Failed to read processing uploads file: {}", e);
                }
            }

            if let Err(e) = tokio::fs::remove_file(&processing_file).await {
                log::error!("❌ Failed to remove processing uploads file: {}", e);
            }
        }
    });
}
