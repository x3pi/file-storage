use crate::app::App;
use alloy::primitives::B256;
use alloy::providers::{Provider, ProviderBuilder};
use alloy::rpc::types::eth::Filter;
use alloy::transports::ws::WsConnect;
use futures_util::StreamExt;

use std::sync::Arc;
use std::time::Instant;
use tokio::time::{sleep, Duration};

// Import event từ file_contract
use crate::file_contract::Files::{DownloadKeyConfirmed, FileActivated, FileDeleted};
use alloy::sol_types::SolEvent;

pub async fn listen_download_confirmed_events(app: Arc<App>) {
    loop {
        match listen_download_confirmed_internal(app.clone()).await {
            Ok(_) => {}
            Err(_) => {}
        }
        sleep(Duration::from_millis(100)).await;
    }
}

async fn listen_download_confirmed_internal(app: Arc<App>) -> Result<(), String> {
    // Kết nối WebSocket
    let ws = WsConnect::new(&app.config.rpc_url_ws);
    let provider = ProviderBuilder::new()
        .connect_ws(ws)
        .await
        .map_err(|e| format!("Failed to connect WebSocket: {}", e))?;

    let filter = Filter::new().address(app.config.contract_address);
    let sub = provider
        .subscribe_logs(&filter)
        .await
        .map_err(|e| format!("Failed to subscribe to logs: {}", e))?;

    let mut stream = sub.into_stream();

    // Lắng nghe events
    while let Some(log) = stream.next().await {
        // Decode event DownloadKeyConfirmed
        if let Ok(event) = DownloadKeyConfirmed::decode_log(&log.inner.clone().into()) {
            // Xử lý event
            process_download_confirmed_event(event.downloadKey, &app).await;
        } else if let Ok(event) = FileActivated::decode_log(&log.inner.clone().into()) {
            // Xử lý event
            process_file_activated_event(event.fileKey, &app).await;
        } else if let Ok(event) = FileDeleted::decode_log(&log.inner.clone().into()) {
            // Xử lý xoá file vật lý
            process_file_deleted_event(event.fileKey, &app).await;
        }
    }

    Ok(())
}

async fn process_download_confirmed_event(download_key: B256, app: &Arc<App>) {
    let download_key_hex = hex::encode(download_key);
    // Xóa download key khỏi cache
    if let Some(mut session) = app.download_cache.get_mut(&download_key_hex) {
        session.confirmed_at = Some(Instant::now());
        let app_clone = app.clone();
        let key_to_delete = download_key_hex.clone();
        let timeout_seconds = app_clone.config.session_timeout_seconds;
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(timeout_seconds)).await;
            if let Some((_key, _session)) = app_clone.download_cache.remove(&key_to_delete) {
                log::info!(
                    "✅ Removed expired downloadKey from cache: {}",
                    key_to_delete
                );
            }
        });
    }
}
async fn process_file_activated_event(file_key: B256, app: &Arc<App>) {
    let file_key_hex = hex::encode(file_key);
    if let Some(_removed) = app.upload_file_cache.remove(&file_key_hex) {
        log::info!(
            "✅ Removed fileKey from upload cache (address + merkle_root): {}",
            file_key_hex
        );
    }
    if let Some((_, _)) = app.file_cache.remove(&file_key_hex) {
        log::info!("✅ Closed and removed file handle from file_cache: {}", file_key_hex);
    }
}

async fn process_file_deleted_event(file_key: B256, app: &Arc<App>) {
    let file_key_hex = hex::encode(file_key);
    log::info!(
        "🗑️ Received FileDeleted event for fileKey: {}",
        file_key_hex
    );

    // Tính toán đường dẫn thư mục giống như lúc lưu
    let level1 = &file_key_hex[0..2];
    let level2 = &file_key_hex[2..4];
    let file_dir = app
        .storage_root
        .join(level1)
        .join(level2)
        .join(&file_key_hex);

    // Xóa thư mục chứa file
    match tokio::fs::remove_dir_all(&file_dir).await {
        Ok(_) => {
            log::info!(
                "✅ Successfully deleted file chunks from disk: {:?}",
                file_dir
            );

            // Xóa tiếp các thư mục cha (level2 và level1) nếu chúng rỗng
            if let Some(level2_dir) = file_dir.parent() {
                let _ = tokio::fs::remove_dir(level2_dir).await; // Xóa nếu rỗng, bỏ qua lỗi nếu còn file khác
                if let Some(level1_dir) = level2_dir.parent() {
                    let _ = tokio::fs::remove_dir(level1_dir).await;
                }
            }
        }
        Err(e) => {
            if e.kind() == std::io::ErrorKind::NotFound {
                log::warn!(
                    "⚠️ File directory not found (already deleted or never existed): {:?}",
                    file_dir
                );
            } else {
                log::error!("❌ Failed to delete file directory {:?}: {}", file_dir, e);
            }
        }
    }

    // Xóa khỏi cache nếu đang có
    if let Some(_removed) = app.upload_file_cache.remove(&file_key_hex) {
        log::info!("✅ Removed fileKey from upload cache: {}", file_key_hex);
    }
    if let Some((_, _)) = app.file_cache.remove(&file_key_hex) {
        log::info!("✅ Closed and removed file handle from file_cache: {}", file_key_hex);
    }
}

pub async fn start_chain_id_monitor(app: Arc<App>) {
    log::info!("📡 Starting Chain ID monitor (using WebSocket)...");
    // Lấy URL từ config
    let rpc_url = app.config.rpc_url_ws.clone();
    // để tái sử dụng kết nối WebSocket
    let mut provider_option = None;
    loop {
        // Nếu chúng ta chưa có provider (lần đầu, hoặc sau lỗi kết nối)
        if provider_option.is_none() {
            let ws = WsConnect::new(&rpc_url);
            match ProviderBuilder::new().connect_ws(ws).await {
                Ok(p) => {
                    provider_option = Some(p); // Lưu lại provider
                }
                Err(_e) => {
                    // log::warn!(
                    //     "Failed to connect WebSocket for chain ID monitor (will retry in 20s): {}",
                    //     e
                    // );
                    // Ngủ 20 giây trước khi thử kết nối lại
                    sleep(Duration::from_secs(20)).await;
                    continue; // Bỏ qua phần còn lại của vòng lặp, thử kết nối lại
                }
            }
        }

        // Nếu chúng ta CÓ provider, hãy sử dụng nó
        if let Some(provider) = &provider_option {
            log::debug!("Polling for chain ID over WebSocket...");
            match provider.get_chain_id().await {
                Ok(_) => {
                    // log::info!("✅ Chain ID check OK (over Ws): {}", chain_id);
                }
                Err(e) => {
                    log::warn!("Failed to get chain ID over WebSocket: {}", e);
                    provider_option = None;
                }
            }
        }

        // Chờ 20 giây trước khi poll lần tiếp theo
        sleep(Duration::from_secs(20)).await;
    }
}
