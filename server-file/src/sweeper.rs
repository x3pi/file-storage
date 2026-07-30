use crate::app::App;
use std::sync::Arc;

/// Khởi chạy Bác Quét Rác chạy ngầm
/// Nhiệm vụ: Xóa các DownloadSession bị bỏ hoang khỏi RAM
pub fn spawn_background_sweeper(app: Arc<App>) {
    tokio::spawn(async move {
        loop {
            // Ngủ 10 giờ (36000 giây)
            tokio::time::sleep(tokio::time::Duration::from_secs(36000)).await;
            
            let now = std::time::Instant::now();
            let mut expired_keys = Vec::new();

            // Quét các session đã nằm trong RAM quá 24h (86400 giây)
            for entry in app.download_cache.iter() {
                if now.duration_since(entry.created_at).as_secs() > 86400 {
                    expired_keys.push(entry.key().clone());
                }
            }

            // Tiến hành dọn dẹp
            if !expired_keys.is_empty() {
                for key in &expired_keys {
                    app.download_cache.remove(key);
                }
                log::info!("🧹 Đã dọn dẹp xong {} session bỏ hoang quá 1 ngày khỏi RAM.", expired_keys.len());
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
