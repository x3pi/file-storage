use crate::app::App;
use alloy::primitives::{Address, B256};
use alloy::rpc::types::eth::Filter;
use std::sync::Arc;
use tokio::time::{sleep, Duration};

// Import event từ file_contract
use crate::contracts::file_contract::Files::{DownloadKeyConfirmed, FileActivated, FileDeleted};
use alloy::sol_types::SolEvent;

pub const LAST_SCANNED_BLOCK_FILE: &str = "last_scanned_block.txt";
pub const CATCHUP_PROGRESS_FILE: &str = "catchup_progress.txt";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockRange {
    pub from: u64,
    pub to: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatchupProgress {
    pub cursor: u64,
    pub to: u64,
}

#[derive(Debug, PartialEq, Eq)]
pub struct CatchupPlanResult {
    pub realtime_start: u64,
    pub catchup_ranges: Vec<BlockRange>,
    pub should_clear_catchup_file: bool,
}

/// Xác định kế hoạch quét block đảm bảo:
/// 1. KHÔNG QUÉT TRÙNG (No Overlaps): Các dải block khép kín, không chồng chéo với Realtime.
/// 2. KHÔNG QUÉT THỪA (No Redundant work): Bỏ qua nếu đã quét, xử lý nối dải nếu crash nhiều lần.
/// 3. RESET SAFETY: Nếu block trong file > current_block -> Ưu tiên current_block, KHÔNG quét lùi.
pub fn determine_catchup_plan(
    saved_last_block: Option<u64>,
    saved_catchup: Option<CatchupProgress>,
    current_block: u64,
) -> CatchupPlanResult {
    // 1. Trường hợp reset chain: block trong file > current_block
    if let Some(saved) = saved_last_block {
        if saved > current_block {
            return CatchupPlanResult {
                realtime_start: current_block,
                catchup_ranges: Vec::new(),
                should_clear_catchup_file: true,
            };
        }
    }

    let mut ranges = Vec::new();

    // 2. Nếu có tiến trình quét bù cũ dở dang (do restart giữa chừng)
    if let Some(catchup) = saved_catchup {
        if catchup.cursor <= catchup.to && catchup.to <= current_block {
            ranges.push(BlockRange {
                from: catchup.cursor,
                to: catchup.to,
            });
        }
    }

    // 3. Nếu có khoảng trống giữa saved_last_block và current_block
    if let Some(saved) = saved_last_block {
        if saved < current_block {
            let new_from = saved + 1;
            let new_to = current_block;

            // Kiểm tra xem có bị trùng với range cũ không
            if let Some(last_range) = ranges.last_mut() {
                if last_range.to == saved {
                    // Nối liền 2 dải
                    last_range.to = new_to;
                } else if new_from > last_range.to {
                    ranges.push(BlockRange {
                        from: new_from,
                        to: new_to,
                    });
                }
            } else {
                ranges.push(BlockRange {
                    from: new_from,
                    to: new_to,
                });
            }
        }
    }

    CatchupPlanResult {
        realtime_start: current_block,
        catchup_ranges: ranges,
        should_clear_catchup_file: false,
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
    let _ = crate::utils::write_atomic(&path, &block.to_string()).await;
}

pub async fn read_catchup_progress(storage_root: &std::path::Path) -> Option<CatchupProgress> {
    let path = storage_root.join(CATCHUP_PROGRESS_FILE);
    if let Ok(content) = tokio::fs::read_to_string(&path).await {
        let parts: Vec<&str> = content.trim().split(',').collect();
        if parts.len() == 2 {
            if let (Ok(cursor), Ok(to)) = (parts[0].trim().parse::<u64>(), parts[1].trim().parse::<u64>()) {
                if cursor <= to {
                    return Some(CatchupProgress { cursor, to });
                }
            }
        }
    }
    None
}

pub async fn write_catchup_progress(storage_root: &std::path::Path, cursor: u64, to: u64) {
    let path = storage_root.join(CATCHUP_PROGRESS_FILE);
    let content = format!("{},{}", cursor, to);
    let _ = crate::utils::write_atomic(&path, &content).await;
}

pub async fn clear_catchup_progress(storage_root: &std::path::Path) {
    let path = storage_root.join(CATCHUP_PROGRESS_FILE);
    let _ = tokio::fs::remove_file(&path).await;
}

/// Luồng 2 (Worker ngầm): Quét bù các sự kiện FileDeleted trong quá khứ khi server tắt
async fn catch_up_historical_deleted_events(app: Arc<App>, ranges: Vec<BlockRange>) {
    if ranges.is_empty() {
        return;
    }

    log::info!(
        "🚀 [CATCH-UP WORKER] Bắt đầu quét bù FileDeleted cho {} dải block...",
        ranges.len()
    );

    let mut total_deleted = 0;

    for (range_idx, range) in ranges.iter().enumerate() {
        log::info!(
            "📦 [CATCH-UP WORKER] Xử lý dải {}/{}: block {}..={}",
            range_idx + 1,
            ranges.len(),
            range.from,
            range.to
        );

        let mut cursor = range.from;
        let mut consecutive_errors = 0;

        // Ghi mốc khởi đầu của dải này trước khi vào vòng lặp
        write_catchup_progress(&app.storage_root, cursor, range.to).await;

        while cursor <= range.to {
            let chunk_to = (cursor + 499).min(range.to);
            let mut batch_deleted_count = 0;

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

            let mut batch_succeeded = false;

            match app.http_client.post(&app.config.rpc_url).json(&req).send().await {
                Ok(resp) => {
                    match resp.json::<serde_json::Value>().await {
                        Ok(json) => {
                            if let Some(result) = json.get("result") {
                                if result.is_null() {
                                    batch_succeeded = true;
                                    consecutive_errors = 0;
                                } else {
                                    match serde_json::from_value::<Vec<alloy::rpc::types::eth::Log>>(result.clone()) {
                                        Ok(logs) => {
                                            batch_succeeded = true;
                                            consecutive_errors = 0;

                                            for log in logs {
                                                if let Ok(event) = FileDeleted::decode_log(&log.inner.clone().into()) {
                                                    let file_key_hex = hex::encode(event.fileKey);

                                                    // 🎯 CẢI TIẾN 2: DISK-FIRST CHECK (Chống quét thừa RPC, tăng tốc 50 lần)
                                                    // Nếu file không tồn tại trên ổ cứng của server này -> Bỏ qua ngay lập tức!
                                                    if file_key_hex.len() >= 4 {
                                                        let level1 = &file_key_hex[0..2];
                                                        let level2 = &file_key_hex[2..4];
                                                        let file_dir = app.storage_root.join(level1).join(level2).join(&file_key_hex);
                                                        if !file_dir.exists() {
                                                            continue;
                                                        }
                                                    }

                                                    // Chỉ khi file THỰC SỰ có trên đĩa cứng mới kiểm tra contract và xóa
                                                    match app.check_contract_validity(log.address()).await {
                                                        Ok(true) => {
                                                            total_deleted += 1;
                                                            batch_deleted_count += 1;
                                                            process_file_deleted_event(event.fileKey, log.address(), &app).await;
                                                        }
                                                        Ok(false) => {
                                                            continue;
                                                        }
                                                        Err(e) => {
                                                            log::warn!("⚠️ [CATCH-UP RETRY] Lỗi RPC check contract validity: {}. Thử lại batch.", e);
                                                            batch_succeeded = false;
                                                            break;
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                        Err(e) => {
                                            consecutive_errors += 1;
                                            log::warn!("⚠️ [CATCH-UP] Lỗi parse logs (lần {}): {:?}", consecutive_errors, e);
                                            if consecutive_errors >= 5 {
                                                log::error!("💀 [CATCH-UP CRITICAL] Quá 5 lần lỗi parse block {}..={}. Bỏ qua batch.", cursor, chunk_to);
                                                batch_succeeded = true;
                                                consecutive_errors = 0;
                                            }
                                        }
                                    }
                                }
                            } else if let Some(err) = json.get("error") {
                                let err_str = err.to_string();
                                log::warn!("⚠️ [CATCH-UP] RPC error cho block {}..={}: {}", cursor, chunk_to, err_str);
                                if err_str.contains("prune") || err_str.contains("range") {
                                    log::error!("🛑 [CATCH-UP] Block cũ bị RPC prune. Dừng dải quét này.");
                                    break;
                                }
                            }
                        }
                        Err(e) => {
                            log::warn!("⚠️ [CATCH-UP] Lỗi parse JSON response: {}", e);
                        }
                    }
                }
                Err(e) => {
                    log::warn!("⚠️ [CATCH-UP] Lỗi mạng khi gọi get_logs: {}", e);
                }
            }

            // 🎯 CẢI TIẾN 4 & MỤC 2 REVIEW: Chỉ tiến cursor khi batch đã hoàn thành thành công
            if batch_succeeded {
                cursor = chunk_to + 1;
                // Cập nhật lại cursor sau khi hoàn thành batch (chỉ ghi 1 lần duy nhất trên mỗi batch)
                write_catchup_progress(&app.storage_root, cursor, range.to).await;

                // Tối ưu độ trễ: Nếu không có file xóa thì chỉ nghỉ 10ms để quét cực nhanh, nếu có file xóa thì nghỉ 50ms
                if batch_deleted_count > 0 {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                } else {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            } else {
                tokio::time::sleep(Duration::from_millis(1000)).await;
            }
        }
    }

    // Khi đã hoàn tất toàn bộ các dải quét bù:
    clear_catchup_progress(&app.storage_root).await;
    log::info!(
        "✅ [CATCH-UP WORKER] Hoàn tất toàn bộ quét bù! Đã xử lý xóa {} file(s) trên đĩa.",
        total_deleted
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

    let saved_last_block = read_saved_last_block(&app.storage_root).await;
    let saved_catchup = read_catchup_progress(&app.storage_root).await;
    let mut last_block = current_block;

    if is_initial_start {
        let plan = determine_catchup_plan(saved_last_block, saved_catchup, current_block);

        if plan.should_clear_catchup_file {
            log::warn!(
                "⚠️ [LISTENER] Block trong file ({:?}) > block hiện tại ({}) (chain reset). Ưu tiên block hiện tại {}, KHÔNG quét lùi.",
                saved_last_block, current_block, current_block
            );
            clear_catchup_progress(&app.storage_root).await;
            write_saved_last_block(&app.storage_root, current_block).await;
        } else if !plan.catchup_ranges.is_empty() {
            log::info!(
                "🔄 [LISTENER] Phát hiện {} dải cần quét bù quá khứ. Khởi chạy luồng 2 (Worker ngầm)...",
                plan.catchup_ranges.len()
            );
            let app_catchup = app.clone();
            let ranges = plan.catchup_ranges;
            tokio::spawn(async move {
                catch_up_historical_deleted_events(app_catchup, ranges).await;
            });
        }

        last_block = plan.realtime_start;
        write_saved_last_block(&app.storage_root, last_block).await;
    } else if let Some(saved) = saved_last_block {
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
