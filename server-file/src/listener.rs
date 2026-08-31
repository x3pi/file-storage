use crate::app::App;
use alloy::primitives::B256;
use alloy::rpc::types::eth::Filter;
use std::sync::Arc;
use tokio::time::{sleep, Duration};

// Import event từ file_contract
use crate::contracts::file_contract::Files::{DownloadKeyConfirmed, FileActivated, FileDeleted};
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
    // Lấy block hiện tại làm mốc
    let mut last_block = match get_block_number(&app).await {
        Ok(b) => b,
        Err(e) => return Err(format!("Failed to get initial block number: {}", e)),
    };

    log::info!("🔄 [POLLING] Started polling for events from block {}", last_block);

    loop {
        let current_block = get_block_number(&app).await.unwrap_or(last_block);
        if current_block > last_block {
            // ⚠️ FIX: Đợi 2500ms để custom chain kịp ghi block hash và index log vào DB.
            // Tránh lỗi race condition: getBlockNumber trả về block mới nhưng getLogs lại trả về null.
            tokio::time::sleep(std::time::Duration::from_millis(2500)).await;

            let filter = Filter::new()
                .from_block(last_block + 1)
                .to_block(current_block)
                .event_signature(vec![
                    DownloadKeyConfirmed::SIGNATURE_HASH,
                    FileActivated::SIGNATURE_HASH,
                    FileDeleted::SIGNATURE_HASH,
                ]);

            // Tự gửi HTTP request để bypass lỗi `null` trả về từ custom chain
            #[derive(serde::Serialize)]
            struct RpcRequest<'a> {
                jsonrpc: &'static str,
                id: u64,
                method: &'static str,
                params: Vec<&'a Filter>,
            }

            let req = RpcRequest {
                jsonrpc: "2.0",
                id: 1,
                method: "eth_getLogs",
                params: vec![&filter],
            };
            
            if let Ok(response) = app.http_client.post(&app.config.rpc_url).json(&req).send().await {
                if let Ok(json) = response.json::<serde_json::Value>().await {
                    if let Some(result) = json.get("result") {
                        if result.is_null() {
                            // Custom chain trả về null => Không có log nào
                        } else {
                            match serde_json::from_value::<Vec<alloy::rpc::types::eth::Log>>(result.clone()) {
                                Ok(logs) => {
                                    for log in logs {
                                        log::info!("🔍 [EVENT DEBUG] Received a log from address: {}", log.address());
                                        let is_valid = app.is_valid_contract(log.address()).await;
                                        if !is_valid {
                                            continue; // Bỏ qua event từ contract rác/fake
                                        }

                                        // Decode event
                                        if let Ok(event) = DownloadKeyConfirmed::decode_log(&log.inner.clone().into()) {
                                            process_download_confirmed_event(event.downloadKey, &app).await;
                                        } else if let Ok(event) = FileActivated::decode_log(&log.inner.clone().into()) {
                                            process_file_activated_event(event.fileKey, &app).await;
                                        } else if let Ok(event) = FileDeleted::decode_log(&log.inner.clone().into()) {
                                            process_file_deleted_event(event.fileKey, &app).await;
                                        } else {
                                            log::warn!("⚠️ [EVENT DEBUG] Failed to decode log! Topics: {:?}", log.inner.topics());
                                        }
                                    }
                                }
                                Err(e) => {
                                    log::warn!("⚠️ [EVENT DEBUG] Failed to parse get_logs result: {:?}, raw JSON: {}", e, result);
                                }
                            }
                        }
                    } else if let Some(err) = json.get("error") {
                        log::warn!("⚠️ [EVENT DEBUG] RPC Error from get_logs: {}", err);
                    }
                }
            } else {
                log::warn!("⚠️ [EVENT DEBUG] Failed to send get_logs HTTP request for blocks {} to {}", last_block + 1, current_block);
            }
            
            // Cập nhật last_block
            last_block = current_block;
        }

        // Poll mỗi 2 giây
        sleep(Duration::from_millis(2000)).await;
    }
}

async fn get_block_number(app: &Arc<App>) -> Result<u64, String> {
    #[derive(serde::Serialize)]
    struct RpcRequest {
        jsonrpc: &'static str,
        id: u64,
        method: &'static str,
        params: Vec<()>,
    }

    let req = RpcRequest {
        jsonrpc: "2.0",
        id: 1,
        method: "eth_blockNumber",
        params: vec![],
    };

    let response = app
        .http_client
        .post(&app.config.rpc_url)
        .json(&req)
        .send()
        .await
        .map_err(|e| format!("Request error: {}", e))?;

    let json: serde_json::Value = response
        .json()
        .await
        .map_err(|e| format!("JSON error: {}", e))?;

    let result = json
        .get("result")
        .and_then(|v| v.as_str())
        .ok_or_else(|| "Invalid response".to_string())?;

    let hex_str = result.trim_start_matches("0x");
    u64::from_str_radix(hex_str, 16).map_err(|e| format!("Parse error: {}", e))
}

async fn process_download_confirmed_event(download_key: B256, app: &Arc<App>) {
    let download_key_hex = hex::encode(download_key);
    // Xóa download key khỏi cache
    if let Some(mut session) = app.download_cache.get_mut(&download_key_hex) {
        session.confirmed_at = Some(std::time::Instant::now());
        let app_clone = app.clone();
        let key_to_delete = download_key_hex.clone();
        tokio::spawn(async move {
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
