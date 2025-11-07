use crate::app::App;
use alloy::primitives::B256;
use alloy::providers::{Provider, ProviderBuilder};
use alloy::rpc::types::eth::Filter;
use alloy::transports::ws::WsConnect;
use futures_util::StreamExt;

use std::sync::Arc;
use tokio::time::{sleep, Duration};

// Import event từ file_contract
use crate::file_contract::Files::{DownloadKeyConfirmed, FileActivated};
use alloy::sol_types::SolEvent;

pub async fn listen_download_confirmed_events(app: Arc<App>) -> Result<(), String> {
    loop {
        match listen_download_confirmed_internal(app.clone()).await {
            Ok(_) => {
                return Ok(());
            }
            Err(e) => {
                sleep(Duration::from_secs(1)).await;
            }
        }
    }
}

async fn listen_download_confirmed_internal(app: Arc<App>) -> Result<(), String> {
    // Kết nối WebSocket
    let ws = WsConnect::new(&app.config.rpc_url);
    let provider = ProviderBuilder::new()
        .connect_ws(ws)
        .await
        .map_err(|e| format!("Failed to connect WebSocket: {}", e))?;

    // println!(
    //     "👂 Listening for DownloadKeyConfirmed events at {:?}",
    //     app.config.contract_address
    // );
    // Tạo filter để lắng nghe events từ contract
    // 🔥 FIX E0609: Truy cập contract_address qua config
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
        }
    }

    Ok(())
}

async fn process_download_confirmed_event(download_key: B256, app: &Arc<App>) {
    let download_key_hex = hex::encode(download_key);
    // Xóa download key khỏi cache
    if let Some(_removed) = app.download_cache.remove(&download_key_hex) {
        println!("✅ Removed downloadKey from cache: {}", download_key_hex);
    }
}
async fn process_file_activated_event(file_key: B256, app: &Arc<App>) {
    let file_key_hex = hex::encode(file_key);
    if let Some(_removed) = app.verified_upload_cache.remove(&file_key_hex) {
        println!(
            "✅Removed fileKey from upload signature cache: {}",
            file_key_hex
        );
    }
}
