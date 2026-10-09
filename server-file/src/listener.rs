use crate::app::App;
use alloy::primitives::{Address, B256};
use alloy::rpc::types::eth::Filter;
use std::sync::Arc;
use tokio::time::{sleep, Duration};

// Import event từ file_contract
use crate::contracts::file_contract::Files::{DownloadKeyConfirmed, FileActivated, FileDeleted};
use alloy::sol_types::SolEvent;

pub const LAST_SCANNED_BLOCK_FILE: &str = "last_scanned_block.txt";

#[derive(Debug, PartialEq, Eq)]
pub enum CatchupPlan {
    None { start_block: u64 },
    CatchupRequired {
        realtime_start: u64,
        catchup_from: u64,
        catchup_to: u64,
    },
}

/// Xác định kế hoạch quét block:
/// - Nếu block trong file > block hiện tại (chain reset / testnet mới): ưu tiên lấy block hiện tại, KHÔNG quét lùi.
/// - Nếu block trong file < block hiện tại: Luồng 1 chạy từ block hiện tại, Luồng 2 quét bù FileDeleted từ saved + 1 đến current.
/// - Các trường hợp khác: bắt đầu từ block hiện tại.
pub fn determine_catchup_plan(saved_block: Option<u64>, current_block: u64) -> CatchupPlan {
    match saved_block {
        Some(saved) if saved > current_block => CatchupPlan::None {
            start_block: current_block,
        },
        Some(saved) if saved < current_block => CatchupPlan::CatchupRequired {
            realtime_start: current_block,
            catchup_from: saved + 1,
            catchup_to: current_block,
        },
        _ => CatchupPlan::None {
            start_block: current_block,
        },
    }
}

pub async fn read_saved_last_block(storage_root: &std::path::Path) -> Option<u64> {
    let path = storage_root.join(LAST_SCANNED_BLOCK_FILE);
    match tokio::fs::read_to_string(&path).await {
        Ok(content) => content.trim().parse::<u64>().ok(),
        Err(_) => None,
    }
}

pub async fn write_saved_last_block(storage_root: &std::path::Path, block: u64) {
    let path = storage_root.join(LAST_SCANNED_BLOCK_FILE);
    let _ = tokio::fs::write(&path, block.to_string()).await;
}

/// Luồng 2 (Worker ngầm): Quét bù các sự kiện FileDeleted trong quá khứ khi server tắt
async fn catch_up_historical_deleted_events(app: Arc<App>, from_block: u64, to_target_block: u64) {
    log::info!(
        "🚀 [CATCH-UP WORKER] Bắt đầu quét bù FileDeleted từ block {} đến {}...",
        from_block,
        to_target_block
    );

    let mut cursor = from_block;
    let mut deleted_count = 0;

    while cursor <= to_target_block {
        let chunk_to = (cursor + 499).min(to_target_block);
        let filter = Filter::new()
            .from_block(cursor)
            .to_block(chunk_to)
            .event_signature(vec![FileDeleted::SIGNATURE_HASH]);

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

        match app.http_client.post(&app.config.rpc_url).json(&req).send().await {
            Ok(resp) => {
                if let Ok(json) = resp.json::<serde_json::Value>().await {
                    if let Some(result) = json.get("result") {
                        if !result.is_null() {
                            if let Ok(logs) = serde_json::from_value::<Vec<alloy::rpc::types::eth::Log>>(result.clone()) {
                                for log in logs {
                                    if let Ok(true) = app.check_contract_validity(log.address()).await {
                                        if let Ok(event) = FileDeleted::decode_log(&log.inner.clone().into()) {
                                            deleted_count += 1;
                                            process_file_deleted_event(event.fileKey, log.address(), &app).await;
                                        }
                                    }
                                }
                            }
                        }
                        cursor = chunk_to + 1;
                    } else if let Some(err) = json.get("error") {
                        let err_str = err.to_string();
                        log::warn!(
                            "⚠️ [CATCH-UP WORKER] RPC error cho block {}..={}: {}.",
                            cursor,
                            chunk_to,
                            err_str
                        );
                        if err_str.contains("prune") || err_str.contains("range") {
                            log::error!("🛑 [CATCH-UP WORKER] Block cũ bị RPC prune. Dừng quét bù.");
                            break;
                        }
                        cursor = chunk_to + 1;
                    } else {
                        cursor = chunk_to + 1;
                    }
                } else {
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
            }
            Err(e) => {
                log::warn!("⚠️ [CATCH-UP WORKER] Lỗi kết nối RPC: {}. Thử lại sau 1s...", e);
                tokio::time::sleep(Duration::from_millis(1000)).await;
            }
        }

        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    log::info!(
        "✅ [CATCH-UP WORKER] Hoàn tất quét bù! Đã xử lý {} sự kiện FileDeleted trong phạm vi block {}..={}",
        deleted_count,
        from_block,
        to_target_block
    );
}

pub async fn listen_download_confirmed_events(app: Arc<App>) {
    let mut is_initial_start = true;
    loop {
        match listen_download_confirmed_internal(app.clone(), is_initial_start).await {
            Ok(_) => {}
            Err(e) => {
                log::warn!("⚠️ [LISTENER] Polling error: {}. Reconnecting in 1s...", e);
            }
        }
        is_initial_start = false;
        sleep(Duration::from_millis(1000)).await;
    }
}

async fn listen_download_confirmed_internal(
    app: Arc<App>,
    is_initial_start: bool,
) -> Result<(), String> {
    let current_block = match get_block_number(&app).await {
        Ok(b) => b,
        Err(e) => return Err(format!("Failed to get initial block number: {}", e)),
    };

    let saved_block = read_saved_last_block(&app.storage_root).await;
    let mut last_block = current_block;

    if is_initial_start {
        let plan = determine_catchup_plan(saved_block, current_block);
        match plan {
            CatchupPlan::None { start_block } => {
                if let Some(saved) = saved_block {
                    if saved > current_block {
                        log::warn!(
                            "⚠️ [LISTENER] Block trong file ({}) > block hiện tại ({}) (có thể do reset mạng blockchain). Ưu tiên lấy block hiện tại {} và KHÔNG quét lùi.",
                            saved, current_block, current_block
                        );
                        write_saved_last_block(&app.storage_root, current_block).await;
                    }
                } else {
                    write_saved_last_block(&app.storage_root, current_block).await;
                }
                last_block = start_block;
            }
            CatchupPlan::CatchupRequired {
                realtime_start,
                catchup_from,
                catchup_to,
            } => {
                log::info!(
                    "🔄 [LISTENER] Phát hiện saved_block={} < current_block={}. Khởi chạy luồng 2 quét bù FileDeleted từ block {} đến {}.",
                    catchup_from - 1, current_block, catchup_from, catchup_to
                );
                let app_catchup = app.clone();
                tokio::spawn(async move {
                    catch_up_historical_deleted_events(app_catchup, catchup_from, catchup_to).await;
                });
                last_block = realtime_start;
            }
        }
    } else if let Some(saved) = saved_block {
        if saved <= current_block {
            last_block = saved;
        }
    }

    log::info!("🔄 [POLLING] Started realtime polling for events from block {}", last_block);

    let mut consecutive_parse_errors = 0;

    loop {
        let current_block = get_block_number(&app).await.unwrap_or(last_block);
        if current_block > last_block {
            // Giới hạn tối đa 500 block mỗi lần quét để tránh vượt quá limit của RPC khi vừa khởi động lại
            let to_block = current_block.min(last_block + 500);

            // ⚠️ FIX: Đợi 2500ms để custom chain kịp ghi block hash và index log vào DB.
            // Tránh lỗi race condition: getBlockNumber trả về block mới nhưng getLogs lại trả về null.
            tokio::time::sleep(std::time::Duration::from_millis(2500)).await;

            let filter = Filter::new()
                .from_block(last_block + 1)
                .to_block(to_block)
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
            
            let mut poll_succeeded = false;
            if let Ok(response) = app.http_client.post(&app.config.rpc_url).json(&req).send().await {
                if let Ok(json) = response.json::<serde_json::Value>().await {
                    if let Some(result) = json.get("result") {
                        if result.is_null() {
                            // Custom chain trả về null => Không có log nào
                            poll_succeeded = true;
                            consecutive_parse_errors = 0;
                        } else {
                            match serde_json::from_value::<Vec<alloy::rpc::types::eth::Log>>(result.clone()) {
                                Ok(logs) => {
                                    consecutive_parse_errors = 0;
                                    poll_succeeded = true;
                                    for log in logs {
                                        log::info!("🔍 [EVENT DEBUG] Received a log from address: {}", log.address());
                                        match app.check_contract_validity(log.address()).await {
                                            Ok(true) => {
                                                // Contract hợp lệ -> Xử lý tiếp
                                            }
                                            Ok(false) => {
                                                continue; // Bỏ qua event từ contract rác/fake
                                            }
                                            Err(e) => {
                                                // RPC lỗi kết nối -> KHÔNG coi là poll thành công, dừng để thử lại block này!
                                                log::warn!("⚠️ [EVENT RETRY] RPC error checking contract validity for {}: {}. Will retry block range.", log.address(), e);
                                                poll_succeeded = false;
                                                break;
                                            }
                                        }

                                        let contract_addr = log.address();

                                        // Decode event
                                        if let Ok(event) = DownloadKeyConfirmed::decode_log(&log.inner.clone().into()) {
                                            process_download_confirmed_event(event.downloadKey, contract_addr, &app).await;
                                        } else if let Ok(event) = FileActivated::decode_log(&log.inner.clone().into()) {
                                            process_file_activated_event(event.fileKey, contract_addr, &app).await;
                                        } else if let Ok(event) = FileDeleted::decode_log(&log.inner.clone().into()) {
                                            process_file_deleted_event(event.fileKey, contract_addr, &app).await;
                                        } else {
                                            log::warn!("⚠️ [EVENT DEBUG] Failed to decode log! Topics: {:?}", log.inner.topics());
                                        }
                                    }
                                }
                                Err(e) => {
                                    consecutive_parse_errors += 1;
                                    log::warn!("⚠️ [EVENT DEBUG] Failed to parse get_logs result (attempt {}): {:?}, raw JSON: {}", consecutive_parse_errors, e, result);
                                    if consecutive_parse_errors >= 5 {
                                        log::error!("💀💀💀 CRITICAL: Consecutive parse errors ({}) for blocks {} to {}. Skipping block range to prevent listener lockup.", consecutive_parse_errors, last_block + 1, to_block);
                                        poll_succeeded = true;
                                        consecutive_parse_errors = 0;
                                    }
                                }
                            }
                        }
                    } else if let Some(err) = json.get("error") {
                        log::warn!("⚠️ [EVENT DEBUG] RPC Error from get_logs: {}", err);
                    }
                }
            } else {
                log::warn!("⚠️ [EVENT DEBUG] Failed to send get_logs HTTP request for blocks {} to {}", last_block + 1, to_block);
            }
            
            // Cập nhật last_block CHỈ KHI lấy log thành công
            if poll_succeeded {
                last_block = to_block;
                write_saved_last_block(&app.storage_root, to_block).await;
            } else {
                log::warn!("⚠️ [EVENT RETRY] get_logs failed for blocks {} to {}. Will retry next poll.", last_block + 1, to_block);
            }
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

async fn process_download_confirmed_event(download_key: B256, contract_addr: Address, app: &Arc<App>) {
    let download_key_hex = hex::encode(download_key);
    // Đánh dấu thời điểm xác nhận thành công theo đúng contract_address
    if let Some(mut session) = app.download_cache.get_mut(&download_key_hex) {
        if session.contract_address != contract_addr {
            log::warn!(
                "⚠️ [Cross-Contract Protection] DownloadKey {} belongs to contract {}, ignoring event from {}",
                download_key_hex, session.contract_address, contract_addr
            );
            return;
        }
        session.confirmed_at = Some(std::time::Instant::now());
        log::info!(
            "✅ Download key {} confirmed on contract {}. Marked confirmed_at.",
            download_key_hex, contract_addr
        );
    }
}

async fn process_file_activated_event(file_key: B256, contract_addr: Address, app: &Arc<App>) {
    let file_key_hex = hex::encode(file_key);
    let mut is_matched = false;
    if let Some(entry) = app.upload_file_cache.get(&file_key_hex) {
        if entry.contract_address == contract_addr {
            is_matched = true;
        } else {
            log::warn!(
                "⚠️ [Cross-Contract Protection] FileKey {} belongs to contract {}, ignoring FileActivated from {}",
                file_key_hex, entry.contract_address, contract_addr
            );
        }
    } else {
        is_matched = true;
    }

    if is_matched {
        if let Some(_removed) = app.upload_file_cache.remove(&file_key_hex) {
            log::info!(
                "✅ Removed fileKey {} from upload cache for contract: {}",
                file_key_hex, contract_addr
            );
        }
        if let Some((_, _)) = app.file_cache.remove(&file_key_hex) {
            log::info!("✅ Closed and removed file handle from file_cache: {}", file_key_hex);
        }
    }
}

async fn process_file_deleted_event(file_key: B256, contract_addr: Address, app: &Arc<App>) {
    let file_key_hex = hex::encode(file_key);
    log::info!(
        "🗑️ Received FileDeleted event for fileKey: {} from contract: {}",
        file_key_hex, contract_addr
    );

    // Cross-Contract Protection: kiểm tra xem fileKey có thuộc về contract này hay không
    if let Some(entry) = app.upload_file_cache.get(&file_key_hex) {
        if entry.contract_address != contract_addr {
            log::warn!(
                "⚠️ [Cross-Contract Protection] FileKey {} belongs to contract {}, ignoring FileDeleted from {}",
                file_key_hex, entry.contract_address, contract_addr
            );
            return;
        }
    } else {
        // Nếu không có trong upload cache, xác thực trực tiếp trên contract emit event
        if let Ok(contract) = app.contract(contract_addr).await {
            if let Ok(info) = contract.getFileInfo(file_key).call().await {
                use crate::contracts::file_contract::Files::FileStatus;
                if info.status != FileStatus::Deleted {
                    log::warn!(
                        "⚠️ [Cross-Contract Protection] FileKey {} on contract {} is not Deleted (status: {:?}), ignoring FileDeleted event",
                        file_key_hex, contract_addr, info.status
                    );
                    return;
                }
            }
        }
    }

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
