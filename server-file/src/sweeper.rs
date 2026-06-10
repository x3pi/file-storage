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
