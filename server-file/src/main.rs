mod app;
mod config;
mod download_manager;
mod ethereum;
mod contracts;
mod wt_server;
mod listener;
mod models;

mod server;
mod sweeper;
pub mod utils;

#[cfg(test)]
mod tests;

/// Số lần retry tối đa ở luồng chính (Fast Failover).
/// Chỉ retry tối đa 3 lần với delay ngắn (2s, 4s, 8s) để không làm nghẽn hàng đợi upload của người dùng khác.
pub(crate) const MAIN_TX_RETRIES: u32 = 3;

/// Số lần retry tối đa ở luồng nền (Background Retry Worker).
/// Định kỳ chạy khi server rảnh, retry tối đa 10 lần trước khi dừng và báo admin xem xét.
pub(crate) const MAX_BACKGROUND_RETRIES: u32 = 10;

#[allow(dead_code)]
pub(crate) const MAX_TX_RETRIES: u32 = 30;
use crate::app::App;
use flexi_logger::{detailed_format, Cleanup, Criterion, FileSpec, Logger, Naming};
use network::transport::Transport;
use rlimit::{getrlimit, Resource};
use std::env;
use std::fs;
use std::sync::Arc;
use sysinfo::System;

#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[tokio::main]
async fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() != 2 {
        eprintln!("Usage: {} <host:port>", args[0]);
        std::process::exit(1);
    }
    let server_addr = &args[1];

    // Lấy port để tạo tên thư mục log riêng biệt (vd: log_7081)
    let port = server_addr.split(':').last().unwrap_or("unknown");
    let log_dir_path = format!("log_{}", port); // Định nghĩa đường dẫn thư mục log

    // Đảm bảo thư mục log tồn tại mà không xoá lịch sử log cũ
    if let Err(e) = fs::create_dir_all(&log_dir_path) {
        eprintln!("⚠️ Warning: Could not create log directory '{}': {}", log_dir_path, e);
    }

    let _logger = Logger::try_with_env_or_str("info, alloy_transport_ws=off, alloy_rpc_client=off") // Ẩn log của alloy để tránh spam khi mất kết nối
        .unwrap()
        .log_to_file(FileSpec::default().directory(&log_dir_path).basename("app"))
        .append() // <--- THÊM DÒNG NÀY
        .format_for_files(detailed_format) // Format chi tiết cho file
        .format_for_stdout(detailed_format) // Format chi tiết cho console
        .rotate(
            Criterion::Size(4_000_000),
            Naming::Numbers,           // Đặt tên file xoay vòng là .1, .2
            Cleanup::KeepLogFiles(40), // Chỉ giữ 2 file log
        )
        .duplicate_to_stdout(flexi_logger::Duplicate::All) // Hiển thị log ra cả console
        .start()
        .expect("Could not start logger");
    // BẮT BUỘC: Cài đặt Panic Hook để ghi log lỗi Crash Server vào file log_7081
    std::panic::set_hook(Box::new(|panic_info| {
        let msg = match panic_info.payload().downcast_ref::<&'static str>() {
            Some(s) => *s,
            None => match panic_info.payload().downcast_ref::<String>() {
                Some(s) => &s[..],
                None => "Box<dyn Any>",
            },
        };
        let location = panic_info
            .location()
            .map_or("unknown location".to_string(), |l| {
                format!("{}:{}", l.file(), l.line())
            });
        log::error!("🔥 CRITICAL PANIC CRASH 🔥 at {}: {}", location, msg);
    }));

    let (soft, hard) = getrlimit(Resource::NOFILE).unwrap();
    log::info!("Max open files (soft): {}", soft);
    log::info!("Max open files (hard): {}", hard);

    // Validate file descriptor limit
    const MIN_REQUIRED_FILES: u64 = 500000;
    if soft < MIN_REQUIRED_FILES {
        eprintln!("\n❌ ERROR: File descriptor limit too low!");
        eprintln!("   Current soft limit: {}", soft);
        eprintln!("   Required minimum: {}", MIN_REQUIRED_FILES);
        eprintln!(" run sudo ./setup_ulimit.sh to increase the limit. Remember to reboot after running ./setup_ulimit.sh\n");
        std::process::exit(1);
    }

    let listen_addr = server_addr;
    let log_dir = std::path::PathBuf::from(&log_dir_path);
    let app = Arc::new(App::setup(log_dir).await.expect("Failed to initialize app"));
    // Tạo storage directory từ config
    if let Err(e) = fs::create_dir_all(&app.storage_root) {
        panic!(
            "Could not create storage directory '{}': {}",
            app.storage_root.display(),
            e
        );
    }
    let stale_tmp = utils::cleanup_stale_tmp_files(&app.storage_root).await;
    if stale_tmp > 0 {
        log::info!("🧹 Đã dọn {} file tạm *.tmp.* còn sót lại trong {:?}", stale_tmp, app.storage_root);
    }
    tokio::spawn(async move {
        let mut sys = System::new();
        let pid = sysinfo::get_current_pid().expect("Failed to get PID");

        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(15));
        loop {
            interval.tick().await;
            sys.refresh_processes_specifics(
                sysinfo::ProcessesToUpdate::Some(&[pid]),
                sysinfo::ProcessRefreshKind::everything(),
            );
            // Lấy thông tin process hiện tại
            let process = sys.process(pid);

            if let Some(proc) = process {
                let memory_mb = proc.memory() / 1024 / 1024; // Convert to MB
                let cpu_usage = proc.cpu_usage();
                log::info!(
                    "📊 [SYSTEM MONITOR]  | RAM: {} MB | CPU: {:.2}%",
                    memory_mb,
                    cpu_usage
                );
            }
        }
    });

    // Spawn event listener (WebSocket)
    let app_clone = app.clone();
    tokio::spawn(async move {
        listener::listen_download_confirmed_events(app_clone).await;
        log::error!("💀💀💀 CRITICAL: Event listener died unexpectedly!");
    });

    // Spawn registry sync worker (mỗi 10 phút)
    let app_clone = app.clone();
    tokio::spawn(async move {
        app_clone.sync_registry_contracts().await;
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(600));
        loop {
            interval.tick().await;
            app_clone.sync_registry_contracts().await;
        }
    });

    // ==========================================
    // TRANSACTION MANAGER (SINGLE WORKER)
    // ==========================================
    // ==========================================
    // TRANSACTION MANAGER (SINGLE WORKER - SINGLE WRITER)
    // ==========================================
    // Giải quyết triệt để lỗi Nonce Conflict do dùng chung 1 ví
    // và đảm bảo tính bền vững (persistence) mà không gây race condition
    let app_clone = app.clone();
    tokio::spawn(async move {
        let mut download_receiver = app_clone.confirmation_receiver.lock().await;
        let mut upload_receiver = app_clone.upload_batch_receiver.lock().await;
        let pending_upload_path = app_clone.storage_root.join("pending_uploads.txt");
        let pending_dl_path = app_clone.storage_root.join("pending_confirmations.txt");
        let failed_upload_json = app_clone.storage_root.join("failed_uploads.json");
        let failed_dl_json = app_clone.storage_root.join("failed_confirmations.json");

        // --- 1. STARTUP RECOVERY (Khôi phục các upload/download chưa hoàn thành từ đĩa) ---
        // 1.1 Khôi phục các bản ghi NEEDS_ADMIN_REVIEW thành PENDING_BACKGROUND_RETRY tuần tự trước khi vào loop, tránh race condition
        let mut boot_upload_records = load_failed_records(&failed_upload_json).await;
        let mut reset_uploads = 0;
        for r in &mut boot_upload_records {
            if r.status == "NEEDS_ADMIN_REVIEW" {
                r.status = "PENDING_BACKGROUND_RETRY".to_string();
                r.attempts = 0;
                r.next_retry_at = None;
                reset_uploads += 1;
            }
        }
        if reset_uploads > 0 {
            let _ = save_failed_records(&failed_upload_json, &boot_upload_records).await;
            log::info!("♻️ [TX Manager Recovery] Khôi phục {} upload records NEEDS_ADMIN_REVIEW sang PENDING_BACKGROUND_RETRY.", reset_uploads);
        }

        let mut boot_dl_records = load_failed_records(&failed_dl_json).await;
        let mut reset_dls = 0;
        for r in &mut boot_dl_records {
            if r.status == "NEEDS_ADMIN_REVIEW" {
                r.status = "PENDING_BACKGROUND_RETRY".to_string();
                r.attempts = 0;
                r.next_retry_at = None;
                reset_dls += 1;
            }
        }
        if reset_dls > 0 {
            let _ = save_failed_records(&failed_dl_json, &boot_dl_records).await;
            log::info!("♻️ [TX Manager Recovery] Khôi phục {} download records NEEDS_ADMIN_REVIEW sang PENDING_BACKGROUND_RETRY.", reset_dls);
        }

        // 1.2 Nạp các pending tx từ đĩa vào kênh channel (đọc xong nhả Lock ngay rồi mới send, loại bỏ 100% deadlock khi pending > 1000)
        let app_recovery = app_clone.clone();
        let pending_upload_recovery = pending_upload_path.clone();
        let pending_dl_recovery = pending_dl_path.clone();
        tokio::spawn(async move {
            let pending_uploads: Vec<(String, alloy::primitives::Address)> = if tokio::fs::try_exists(&pending_upload_recovery).await.unwrap_or(false) {
                let _guard = app_recovery.pending_uploads_lock.lock().await;
                let mut items = Vec::new();
                if let Ok(content) = tokio::fs::read_to_string(&pending_upload_recovery).await {
                    for line in content.lines() {
                        let parts: Vec<&str> = line.trim().split(',').collect();
                        if parts.len() == 2 {
                            let key = parts[0].trim().to_string();
                            if let Ok(addr) = parts[1].trim().parse::<alloy::primitives::Address>() {
                                if !key.is_empty() {
                                    items.push((key, addr));
                                }
                            }
                        }
                    }
                }
                items
            } else {
                Vec::new()
            }; // _guard nhả ngay lập tức tại đây, không giữ lock khi send!

            let mut count = 0;
            for (key, addr) in pending_uploads {
                let _ = app_recovery.upload_batch_sender.send((key, addr)).await;
                count += 1;
            }
            if count > 0 {
                log::info!("♻️ [TX Manager Recovery] Khôi phục {} uploads chưa confirm từ đĩa vào hàng đợi.", count);
            }

            let pending_dls: Vec<(String, String)> = if tokio::fs::try_exists(&pending_dl_recovery).await.unwrap_or(false) {
                let _guard = app_recovery.pending_confirmations_lock.lock().await;
                let mut items = Vec::new();
                if let Ok(content) = tokio::fs::read_to_string(&pending_dl_recovery).await {
                    for line in content.lines() {
                        let parts: Vec<&str> = line.trim().split(',').collect();
                        if parts.len() == 2 {
                            let key = parts[0].trim().to_string();
                            let addr = parts[1].trim().to_string();
                            if !key.is_empty() && !addr.is_empty() {
                                items.push((key, addr));
                            }
                        }
                    }
                }
                items
            } else {
                Vec::new()
            }; // _guard nhả ngay lập tức tại đây!

            let mut dl_count = 0;
            for (key, addr) in pending_dls {
                let _ = app_recovery.confirmation_sender.send((key, addr)).await;
                dl_count += 1;
            }
            if dl_count > 0 {
                log::info!("♻️ [TX Manager Recovery] Khôi phục {} download confirms chưa hoàn thành từ đĩa.", dl_count);
            }
        });

        let mut background_timer = tokio::time::interval(tokio::time::Duration::from_secs(30));
        background_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                // Tuyệt đối không dùng biased; để Tokio luân phiên công bằng giữa upload và download, chống starvation
                Some((file_key, contract_addr)) = upload_receiver.recv() => {
                    use std::collections::HashMap;
                    let mut batches: HashMap<alloy::primitives::Address, Vec<String>> = HashMap::new();
                    batches.entry(contract_addr).or_default().push(file_key);
                    
                    // Vét sạch (drain) tất cả các file upload khác đang nằm trong ống chờ
                    while let Ok((other_key, other_addr)) = upload_receiver.try_recv() {
                        let batch = batches.entry(other_addr).or_default();
                        if !batch.contains(&other_key) {
                            batch.push(other_key);
                        }
                        if batch.len() >= 50 { break; } // Giới hạn mảng tối đa 50 mỗi contract
                    }

                    // File đã được ghi vào pending_uploads.txt từ finalize_upload_file trước khi vào channel,
                    // do đó không cần acquire lock đọc lại đĩa ở đây, loại bỏ 100% lock contention trên hot path!
                    for (addr, current_batch) in batches {
                        log::info!("🚀 [TX Manager] Priority Upload Confirm ({} files) for contract {}", current_batch.len(), addr);
                        match crate::ethereum::confirm_upload_batch(app_clone.clone(), addr, current_batch.clone()).await {
                            Ok(()) => {
                                // CHỈ XOÁ KHỎI ĐĨA KHI TX THÀNH CÔNG TRÊN CHAIN
                                remove_pending_uploads(&pending_upload_path, &current_batch, &addr.to_string(), &app_clone.pending_uploads_lock).await;
                            }
                            Err(e) => {
                                let err_str = e.to_string();
                                if is_already_confirmed(&err_str) {
                                    log::info!("✅ [TX Manager] Batch {} file(s) đã được confirm on-chain trước đó. Xóa khỏi pending.", current_batch.len());
                                    remove_pending_uploads(&pending_upload_path, &current_batch, &addr.to_string(), &app_clone.pending_uploads_lock).await;
                                } else if is_terminal_contract_revert(&err_str) {
                                    log::error!("❌ [TX Manager] confirm_upload_batch gặp lỗi contract không thể phục hồi: {}. Ghi vào failed_uploads.json và xóa khỏi pending.", err_str);
                                    record_failed_upload_batch(
                                        &app_clone.storage_root,
                                        &current_batch,
                                        &addr.to_string(),
                                        &err_str,
                                        0,
                                        "TERMINAL_ERROR",
                                    )
                                    .await;
                                    remove_pending_uploads(&pending_upload_path, &current_batch, &addr.to_string(), &app_clone.pending_uploads_lock).await;
                                } else {
                                    // Mọi lỗi mạng / RPC tạm thời (timeout, connection reset, nonce, gas price,...):
                                    // Gửi 1 lần không được thì ĐẨY NGAY vào failed_uploads.json (attempts = 1) để luồng chính tiếp tục
                                    // xử lý các file khác của user mà không bị delay/nghẽn queue!
                                    log::warn!(
                                        "⚠️ [TX Manager] confirm_upload_batch cho {} file(s) gặp lỗi: {}. Đẩy ngay vào hàng đợi nền (failed_uploads.json) để luồng chính không bị nghẽn.",
                                        current_batch.len(),
                                        err_str
                                    );
                                    record_failed_upload_batch(
                                        &app_clone.storage_root,
                                        &current_batch,
                                        &addr.to_string(),
                                        &err_str,
                                        1,
                                        "PENDING_BACKGROUND_RETRY",
                                    )
                                    .await;
                                    remove_pending_uploads(&pending_upload_path, &current_batch, &addr.to_string(), &app_clone.pending_uploads_lock).await;
                                }
                            }
                        }
                    }
                }

                // 2. Download confirmation từ user
                Some((download_key, contract_addr)) = download_receiver.recv() => {
                    log::info!("⏳ [TX Manager] Processing Download Confirm: {}", download_key);
                    
                    // Ghi nhận vào pending_confirmations.txt trước khi gửi tx nếu chưa có (có Lock bảo vệ)
                    {
                        let _guard = app_clone.pending_confirmations_lock.lock().await;
                        use tokio::io::AsyncWriteExt;
                        let clean_dl_key = download_key.trim_start_matches("0x");
                        let mut exists = false;
                        if let Ok(content) = tokio::fs::read_to_string(&pending_dl_path).await {
                            exists = content.lines().any(|l| {
                                let key_part = l.trim().split(',').next().unwrap_or("").trim_start_matches("0x");
                                key_part.eq_ignore_ascii_case(clean_dl_key)
                            });
                        }
                        if !exists {
                            if let Ok(mut file) = tokio::fs::OpenOptions::new().create(true).append(true).open(&pending_dl_path).await {
                                let line = format!("{},{}\n", download_key, contract_addr);
                                let _ = file.write_all(line.as_bytes()).await;
                                let _ = file.flush().await;
                                let _ = file.sync_data().await;
                            }
                        }
                    }

                    match crate::ethereum::handle_confirm_download(download_key.clone(), contract_addr.clone(), app_clone.clone()).await {
                        Ok(()) => {
                            // Thành công: Xoá khỏi pending_confirmations.txt
                            remove_pending_download(&pending_dl_path, &download_key, &app_clone.pending_confirmations_lock).await;
                        }
                        Err(e) => {
                            let err_str = e.to_string();
                            if is_already_confirmed(&err_str) {
                                log::info!("✅ [TX Manager] Download key {} đã được confirm on-chain trước đó. Xóa khỏi pending.", download_key);
                                remove_pending_download(&pending_dl_path, &download_key, &app_clone.pending_confirmations_lock).await;
                            } else if is_terminal_contract_revert(&err_str) {
                                log::error!("❌ [TX Manager] Download confirmation gặp lỗi contract không thể phục hồi: {}. Ghi vào failed_confirmations.json và xóa khỏi pending.", err_str);
                                record_failed_download_detailed(&app_clone.storage_root, &download_key, &contract_addr, &err_str, 0, "TERMINAL_ERROR").await;
                                remove_pending_download(&pending_dl_path, &download_key, &app_clone.pending_confirmations_lock).await;
                            } else {
                                // Lỗi tạm thời: Đẩy ngay sang failed_confirmations.json (attempts = 1)
                                log::warn!(
                                    "⚠️ [TX Manager] Download confirmation cho key {} gặp lỗi ở luồng chính: {}. Đẩy ngay vào hàng đợi nền (failed_confirmations.json).",
                                    download_key,
                                    err_str
                                );
                                record_failed_download_detailed(
                                    &app_clone.storage_root,
                                    &download_key,
                                    &contract_addr,
                                    &err_str,
                                    1,
                                    "PENDING_BACKGROUND_RETRY",
                                )
                                .await;
                                remove_pending_download(&pending_dl_path, &download_key, &app_clone.pending_confirmations_lock).await;
                            }
                        }
                    }
                }

                // 3. KHI SERVER RẢNH VÀ ĐẾN HẸN (Background Retry Worker):
                _ = background_timer.tick() => {
                    // Ưu tiên tuyệt đối luồng chính: nếu có request mới đang xếp hàng thì nhường ngay
                    if app_clone.upload_batch_sender.capacity() < crate::app::TX_CHANNEL_CAPACITY || app_clone.confirmation_sender.capacity() < crate::app::TX_CHANNEL_CAPACITY {
                        continue;
                    }

                    // 3.1. Thử lại tối đa 3 batch upload từ failed_uploads.json (nếu còn rảnh)
                    let mut upload_records = load_failed_records(&failed_upload_json).await;
                    let mut upload_modified = false;
                    let mut batches_tried = 0;

                    loop {
                        if batches_tried >= 3 || app_clone.upload_batch_sender.capacity() < crate::app::TX_CHANNEL_CAPACITY || app_clone.confirmation_sender.capacity() < crate::app::TX_CHANNEL_CAPACITY {
                            break;
                        }

                        let pending_upload_indices: Vec<usize> = upload_records
                            .iter()
                            .enumerate()
                            .filter(|(_, r)| {
                                r.status == "PENDING_BACKGROUND_RETRY" 
                                     && r.attempts < MAX_BACKGROUND_RETRIES
                                     && crate::utils::is_record_due(&r.next_retry_at)
                            })
                            .map(|(i, _)| i)
                            .collect();

                        if pending_upload_indices.is_empty() {
                            break;
                        }

                        let first_idx = pending_upload_indices[0];
                        let target_contract = upload_records[first_idx].contract_address.clone();
                        let mut batch_indices = Vec::new();
                        for &idx in &pending_upload_indices {
                            if upload_records[idx].contract_address == target_contract {
                                batch_indices.push(idx);
                                if batch_indices.len() >= 10 {
                                    break;
                                }
                            }
                        }

                        if let Ok(addr) = target_contract.parse::<alloy::primitives::Address>() {
                            let batch_keys: Vec<String> = batch_indices.iter().map(|&i| upload_records[i].key.clone()).collect();
                            log::info!("🔄 [TX Manager Background] Server rảnh: đang thử lại {} upload(s) cho contract {}", batch_keys.len(), addr);
                            match crate::ethereum::confirm_upload_batch(app_clone.clone(), addr, batch_keys.clone()).await {
                                Ok(()) => {
                                    log::info!("✅ [TX Manager Background] Thử lại thành công {} upload(s) on-chain!", batch_keys.len());
                                    for &i in &batch_indices {
                                        upload_records[i].status = "RESOLVED".to_string();
                                    }
                                }
                                Err(e) => {
                                    let err_str = e.to_string();
                                    let now = chrono::Local::now().format("%Y-%m-%d %H:%M:%S").to_string();
                                    if is_already_confirmed(&err_str) {
                                        log::info!("✅ [TX Manager Background] Batch {} file(s) đã được confirm trước đó.", batch_keys.len());
                                        for &i in &batch_indices {
                                            upload_records[i].status = "RESOLVED".to_string();
                                        }
                                    } else if is_terminal_contract_revert(&err_str) {
                                        log::error!("❌ [TX Manager Background] Lỗi contract vĩnh viễn: {}. Dừng retry.", err_str);
                                        for &i in &batch_indices {
                                            upload_records[i].status = "TERMINAL_ERROR".to_string();
                                            upload_records[i].reason = err_str.clone();
                                        }
                                    } else if is_transient_rpc_error(&err_str) {
                                        let transient_attempts = upload_records[batch_indices[0]].transient_attempts.saturating_add(1);
                                        let delay = crate::utils::transient_retry_delay_secs(transient_attempts);
                                        let next_retry = crate::utils::compute_next_retry_at(delay);
                                        let is_stuck_long = chrono::NaiveDateTime::parse_from_str(
                                            &upload_records[batch_indices[0]].first_failed_at,
                                            "%Y-%m-%d %H:%M:%S",
                                        )
                                        .map(|dt| (chrono::Local::now().naive_local() - dt).num_seconds() >= 3600)
                                        .unwrap_or(false);

                                        if is_stuck_long {
                                            log::error!(
                                                "🚨 [CRITICAL][TX Manager Background] Upload batch ({} files) kẹt lỗi RPC tạm thời > 1 giờ! first_failed_at: {}, transient_attempts: {}, reason: {}",
                                                batch_keys.len(),
                                                upload_records[batch_indices[0]].first_failed_at,
                                                transient_attempts,
                                                err_str
                                            );
                                        } else {
                                            log::warn!(
                                                "⏳ [TX Manager Background] RPC/mạng tạm thời gián đoạn ({}). Hoãn batch này {}s (transient_attempt: {}, next retry at: {}), KHÔNG tăng attempts để chống Head-of-Line blocking.",
                                                err_str, delay, transient_attempts, next_retry
                                            );
                                        }

                                        for &i in &batch_indices {
                                            upload_records[i].transient_attempts = transient_attempts;
                                            upload_records[i].last_attempt_at = now.clone();
                                            upload_records[i].reason = format!("Transient RPC Error: {}", err_str);
                                            upload_records[i].next_retry_at = Some(next_retry.clone());
                                        }
                                        upload_modified = true;
                                        break; // Mạng/RPC đang gián đoạn, tạm dừng thử các batch khác trong tick này
                                    } else {
                                        for &i in &batch_indices {
                                            upload_records[i].transient_attempts = 0;
                                            upload_records[i].attempts += 1;
                                            upload_records[i].last_attempt_at = now.clone();
                                            upload_records[i].reason = err_str.clone();
                                            if upload_records[i].attempts >= MAX_BACKGROUND_RETRIES {
                                                log::error!(
                                                    "🚨 [TX Manager Background] Upload {} đã thử lại {} lần thất bại. Đánh dấu NEEDS_ADMIN_REVIEW, dừng retry.",
                                                    upload_records[i].key,
                                                    MAX_BACKGROUND_RETRIES
                                                );
                                                upload_records[i].status = "NEEDS_ADMIN_REVIEW".to_string();
                                                upload_records[i].next_retry_at = None;
                                            } else {
                                                let delay = crate::utils::retry_delay_secs(upload_records[i].attempts);
                                                upload_records[i].next_retry_at = Some(crate::utils::compute_next_retry_at(delay));
                                            }
                                        }
                                    }
                                }
                            }
                            upload_modified = true;
                            batches_tried += 1;
                        } else {
                            break;
                        }
                    }

                    if upload_modified {
                        upload_records.retain(|r| r.status != "RESOLVED");
                        let _ = save_failed_records(&failed_upload_json, &upload_records).await;
                    }

                    // 3.2. Retry download confirmations (tối đa 5 download items mỗi tick nếu server rảnh, không bị đói tác vụ)
                    let mut dl_records = load_failed_records(&failed_dl_json).await;
                    let mut dl_modified = false;
                    let mut dls_tried = 0;

                    loop {
                        if dls_tried >= 5 || app_clone.upload_batch_sender.capacity() < crate::app::TX_CHANNEL_CAPACITY || app_clone.confirmation_sender.capacity() < crate::app::TX_CHANNEL_CAPACITY {
                            break;
                        }

                        let pending_dl_idx = dl_records
                            .iter()
                            .position(|r| {
                                r.status == "PENDING_BACKGROUND_RETRY" 
                                     && r.attempts < MAX_BACKGROUND_RETRIES
                                     && crate::utils::is_record_due(&r.next_retry_at)
                            });

                        if let Some(idx) = pending_dl_idx {
                            let dl_key = dl_records[idx].key.clone();
                            let contract_str = dl_records[idx].contract_address.clone();
                            log::info!("🔄 [TX Manager Background] Server rảnh: đang thử lại download key {}...", dl_key);
                            match crate::ethereum::handle_confirm_download(dl_key.clone(), contract_str.clone(), app_clone.clone()).await {
                                Ok(()) => {
                                    log::info!("✅ [TX Manager Background] Thử lại thành công download key: {}", dl_key);
                                    dl_records[idx].status = "RESOLVED".to_string();
                                }
                                Err(e) => {
                                    let err_str = e.to_string();
                                    let now = chrono::Local::now().format("%Y-%m-%d %H:%M:%S").to_string();
                                    if is_already_confirmed(&err_str) {
                                        log::info!("✅ [TX Manager Background] Download key {} đã được confirm trước đó.", dl_key);
                                        dl_records[idx].status = "RESOLVED".to_string();
                                    } else if is_terminal_contract_revert(&err_str) {
                                        log::error!("❌ [TX Manager Background] Download key gặp lỗi contract vĩnh viễn: {}. Dừng retry.", err_str);
                                        dl_records[idx].status = "TERMINAL_ERROR".to_string();
                                        dl_records[idx].reason = err_str;
                                    } else if is_transient_rpc_error(&err_str) {
                                        dl_records[idx].transient_attempts = dl_records[idx].transient_attempts.saturating_add(1);
                                        let delay = crate::utils::transient_retry_delay_secs(dl_records[idx].transient_attempts);
                                        let next_retry = crate::utils::compute_next_retry_at(delay);
                                        let is_stuck_long = chrono::NaiveDateTime::parse_from_str(
                                            &dl_records[idx].first_failed_at,
                                            "%Y-%m-%d %H:%M:%S",
                                        )
                                        .map(|dt| (chrono::Local::now().naive_local() - dt).num_seconds() >= 3600)
                                        .unwrap_or(false);

                                        if is_stuck_long {
                                            log::error!(
                                                "🚨 [CRITICAL][TX Manager Background] Download key {} kẹt lỗi RPC tạm thời > 1 giờ! first_failed_at: {}, transient_attempts: {}, reason: {}",
                                                dl_key,
                                                dl_records[idx].first_failed_at,
                                                dl_records[idx].transient_attempts,
                                                err_str
                                            );
                                        } else {
                                            log::warn!(
                                                "⏳ [TX Manager Background] RPC/mạng tạm thời gián đoạn ({}) khi confirm download {}. Hoãn {}s (transient_attempt: {}, next retry at: {}), KHÔNG tăng attempts.",
                                                err_str, dl_key, delay, dl_records[idx].transient_attempts, next_retry
                                            );
                                        }

                                        dl_records[idx].last_attempt_at = now;
                                        dl_records[idx].reason = format!("Transient RPC Error: {}", err_str);
                                        dl_records[idx].next_retry_at = Some(next_retry);
                                        dl_modified = true;
                                        break; // Mạng/RPC đang gián đoạn, tạm dừng thử các download khác trong tick này
                                    } else {
                                        dl_records[idx].transient_attempts = 0;
                                        dl_records[idx].attempts += 1;
                                        dl_records[idx].last_attempt_at = now;
                                        dl_records[idx].reason = err_str;
                                        if dl_records[idx].attempts >= MAX_BACKGROUND_RETRIES {
                                            log::error!(
                                                "🚨 [TX Manager Background] Download key {} đã thử lại {} lần thất bại. Đánh dấu NEEDS_ADMIN_REVIEW, dừng retry.",
                                                dl_records[idx].key,
                                                MAX_BACKGROUND_RETRIES
                                            );
                                            dl_records[idx].status = "NEEDS_ADMIN_REVIEW".to_string();
                                            dl_records[idx].next_retry_at = None;
                                        } else {
                                            let delay = crate::utils::retry_delay_secs(dl_records[idx].attempts);
                                            dl_records[idx].next_retry_at = Some(crate::utils::compute_next_retry_at(delay));
                                        }
                                    }
                                }
                            }
                            dl_modified = true;
                            dls_tried += 1;
                        } else {
                            break;
                        }
                    }

                    if dl_modified {
                        dl_records.retain(|r| r.status != "RESOLVED");
                        let _ = save_failed_records(&failed_dl_json, &dl_records).await;
                    }
                }
            }
        }
    });

    // Load certificate và private key từ file trong thư mục hiện tại (cùng cấp với src)
    if let Ok(cwd) = env::current_dir() {
        log::info!("🔍 [Debug] Current working directory: {}", cwd.display());
        eprintln!("🔍 [Debug] Current working directory: {}", cwd.display());
    } else {
        log::error!("🔍 [Debug] Failed to get current working directory");
        eprintln!("🔍 [Debug] Failed to get current working directory");
    }

    let cwd = env::current_dir().unwrap_or_else(|_| std::path::PathBuf::from("."));
    let cert_abs_path = cwd.join("certificate.pem");
    let key_abs_path = cwd.join("private.key");

    let cert_path = match fs::metadata("certificate.pem") {
        Ok(_) => {
            log::info!(
                "🔍 [Debug] '{}' exists and is accessible.",
                cert_abs_path.display()
            );
            Some("certificate.pem")
        }
        Err(e) => {
            log::error!(
                "❌ [Debug] Failed to read metadata of '{}': {}",
                cert_abs_path.display(),
                e
            );
            eprintln!(
                "❌ [Debug] Failed to read metadata of '{}': {}",
                cert_abs_path.display(),
                e
            );
            None
        }
    };

    let key_path = match fs::metadata("private.key") {
        Ok(_) => {
            log::info!(
                "🔍 [Debug] '{}' exists and is accessible.",
                key_abs_path.display()
            );
            Some("private.key")
        }
        Err(e) => {
            log::error!(
                "❌ [Debug] Failed to read metadata of '{}': {}",
                key_abs_path.display(),
                e
            );
            eprintln!(
                "❌ [Debug] Failed to read metadata of '{}': {}",
                key_abs_path.display(),
                e
            );
            None
        }
    };
    let transport = if let (Some(cert), Some(key)) = (cert_path, key_path) {
        log::info!("🔐 Loading QUIC certificate from: {}", cert);
        log::info!("🔐 Loading QUIC private key from: {}", key);
        network::quic::QuicTransport::new_with_certs(Some(cert), Some(key))
    } else {
        eprintln!("\n❌ ERROR: QUIC certificate and private TLS key not found!");
        std::process::exit(1);
    };
    let quic_addr: std::net::SocketAddr = listen_addr.parse().expect("Invalid QUIC_ADDR");
    let mut listener = transport
        .listen(quic_addr)
        .await
        .expect("Could not create QUIC listener");

    let mut shutdown_signal = tokio::spawn(async {
        match tokio::signal::ctrl_c().await {
            Ok(()) => {
                log::warn!("🛑 Received CTRL+C signal. Server shutting down gracefully...");
            }
            Err(err) => {
                log::error!("❌ Error waiting for shutdown signal: {}", err);
            }
        }
    });

    let wt_addr_str = app.config.wt_addr.clone();
    let wt_addr: std::net::SocketAddr = wt_addr_str.parse().expect("Invalid WT_ADDR");
    log::info!(
        "🚀 Starting Storage Node on QUIC {} & WebTransport {}",
        quic_addr,
        wt_addr
    );

    crate::sweeper::spawn_background_sweeper(app.clone());

    let app_for_wt = app.clone();
    tokio::spawn(async move {
        wt_server::run_wt_server(app_for_wt, wt_addr, cert_abs_path, key_abs_path).await;
    });

    loop {
        tokio::select! {
            _ = &mut shutdown_signal => {
                log::info!("🛑 Shutdown signal received. Stopping server...");
                break;
            }
            accept_result = listener.accept() => {
                match accept_result {
                    Ok((connection, peer_addr)) => {
                        let app_clone = app.clone();
                        tokio::spawn(async move {
                            if let Err(e) =
                                server::handle_connection(connection, peer_addr, app_clone, peer_addr.ip()).await
                            {
                                log::error!("❌ Error handling connection from {}: {:?}", peer_addr, e);
                            }

                        });
                    }
                    Err(e) => {
                        log::error!("❌ CRITICAL: Connection accept failed: {}", e);
                    }
                }
            }
        }
    }
}

pub(crate) async fn write_atomic(path: &std::path::Path, content: &str) -> std::io::Result<()> {
    crate::utils::write_atomic(path, content).await
}

pub(crate) async fn remove_pending_uploads(
    path: &std::path::Path,
    current_batch: &[String],
    addr_str: &str,
    lock: &tokio::sync::Mutex<()>,
) {
    let _guard = lock.lock().await;
    if let Ok(content) = tokio::fs::read_to_string(path).await {
        let remaining_lines: Vec<&str> = content
            .lines()
            .filter(|line| {
                let parts: Vec<&str> = line.trim().split(',').collect();
                if parts.len() == 2 {
                    let key = parts[0].trim().trim_start_matches("0x");
                    let contract = parts[1].trim();
                    let in_batch = current_batch.iter().any(|b| b.trim_start_matches("0x").eq_ignore_ascii_case(key));
                    !(in_batch && contract.eq_ignore_ascii_case(addr_str))
                } else if !parts.is_empty() {
                    let key = parts[0].trim().trim_start_matches("0x");
                    let in_batch = current_batch.iter().any(|b| b.trim_start_matches("0x").eq_ignore_ascii_case(key));
                    !in_batch
                } else {
                    false
                }
            })
            .collect();
        let new_content = if remaining_lines.is_empty() {
            String::new()
        } else {
            format!("{}\n", remaining_lines.join("\n"))
        };
        let _ = write_atomic(path, &new_content).await;
    }
}

pub(crate) async fn remove_pending_download(
    path: &std::path::Path,
    download_key: &str,
    lock: &tokio::sync::Mutex<()>,
) {
    let _guard = lock.lock().await;
    if let Ok(content) = tokio::fs::read_to_string(path).await {
        let clean_dl_key = download_key.trim_start_matches("0x");
        let remaining_lines: Vec<&str> = content
            .lines()
            .filter(|line| {
                let key_part = line.trim().split(',').next().unwrap_or("").trim_start_matches("0x");
                !key_part.eq_ignore_ascii_case(clean_dl_key)
            })
            .collect();
        let new_content = if remaining_lines.is_empty() {
            String::new()
        } else {
            format!("{}\n", remaining_lines.join("\n"))
        };
        let _ = write_atomic(path, &new_content).await;
    }
}

pub(crate) fn is_already_confirmed(err_str: &str) -> bool {
    let lower = err_str.to_lowercase();
    lower.contains("already confirmed") || lower.contains("confirmed already")
}

pub(crate) fn is_terminal_contract_revert(err_str: &str) -> bool {
    let lower = err_str.to_lowercase();
    lower.contains("invalid key")
        || lower.contains("file does not exist")
        || lower.contains("caller is not a storage server")
        || lower.contains("not authorized")
        || lower.contains("unauthorized")
}

pub(crate) fn is_transient_rpc_error(err_str: &str) -> bool {
    let lower = err_str.to_lowercase();
    lower.contains("connection refused")
        || lower.contains("connection reset")
        || lower.contains("timeout")
        || lower.contains("timed out")
        || lower.contains("deadline has elapsed")
        || lower.contains("transport")
        || lower.contains("failed to send request")
        || lower.contains("error sending request")
        || lower.contains("hyper::error")
        || lower.contains("dns")
        || lower.contains("channel closed")
        || lower.contains("broken pipe")
        || lower.contains("502 bad gateway")
        || lower.contains("503 service unavailable")
        || lower.contains("504 gateway timeout")
        || lower.contains("429 too many requests")
}

pub(crate) async fn load_failed_records(path: &std::path::Path) -> Vec<crate::models::FailedTxRecord> {
    if !path.exists() {
        return Vec::new();
    }
    if let Ok(content) = tokio::fs::read_to_string(path).await {
        // Hỗ trợ cả định dạng JSON array và JSON lines
        if let Ok(records) = serde_json::from_str::<Vec<crate::models::FailedTxRecord>>(&content) {
            return records;
        }
        let mut records = Vec::new();
        for line in content.lines() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            if let Ok(rec) = serde_json::from_str::<crate::models::FailedTxRecord>(line) {
                records.push(rec);
            }
        }
        return records;
    }
    Vec::new()
}

pub(crate) async fn save_failed_records(path: &std::path::Path, records: &[crate::models::FailedTxRecord]) -> std::io::Result<()> {
    let json_str = serde_json::to_string_pretty(records).unwrap_or_else(|_| "[]".to_string());
    write_atomic(path, &json_str).await
}

pub(crate) async fn upsert_failed_record(path: &std::path::Path, record: crate::models::FailedTxRecord) {
    let mut records = load_failed_records(path).await;
    let clean_key = record.key.trim_start_matches("0x");
    if let Some(existing) = records.iter_mut().find(|r| r.key.trim_start_matches("0x").eq_ignore_ascii_case(clean_key)) {
        *existing = record;
    } else {
        records.push(record);
    }
    let _ = save_failed_records(path, &records).await;
}

#[allow(dead_code)]
pub(crate) async fn remove_failed_record(path: &std::path::Path, key: &str) {
    let mut records = load_failed_records(path).await;
    let clean_key = key.trim_start_matches("0x");
    records.retain(|r| !r.key.trim_start_matches("0x").eq_ignore_ascii_case(clean_key));
    let _ = save_failed_records(path, &records).await;
}

/// Ghi nhận hàng loạt (Batch) các file upload thất bại vào JSON trong 1 lần I/O duy nhất (O(n)).
/// Loại bỏ hoàn toàn vòng lặp N lần đọc/ghi đĩa atomic gây nghẽn O(n^2).
pub(crate) async fn record_failed_upload_batch(
    storage_root: &std::path::Path,
    file_keys: &[String],
    contract: &str,
    reason: &str,
    attempts: u32,
    status: &str,
) {
    if file_keys.is_empty() {
        return;
    }
    let now = chrono::Local::now().format("%Y-%m-%d %H:%M:%S").to_string();
    let json_path = storage_root.join("failed_uploads.json");
    let mut records = load_failed_records(&json_path).await;

    for file_key in file_keys {
        let clean_key = file_key.trim_start_matches("0x");
        let new_record = crate::models::FailedTxRecord {
            key: file_key.clone(),
            contract_address: contract.to_string(),
            attempts,
            max_attempts: MAX_BACKGROUND_RETRIES,
            reason: reason.to_string(),
            status: status.to_string(),
            first_failed_at: now.clone(),
            last_attempt_at: now.clone(),
            next_retry_at: None,
            transient_attempts: 0,
        };
        if let Some(existing) = records.iter_mut().find(|r| r.key.trim_start_matches("0x").eq_ignore_ascii_case(clean_key)) {
            *existing = new_record;
        } else {
            records.push(new_record);
        }
    }
    let _ = save_failed_records(&json_path, &records).await;

    let failed_path = storage_root.join("failed_uploads.txt");
    let mut log_lines = String::new();
    for file_key in file_keys {
        log_lines.push_str(&format!(
            "[{}] FileKey: {}, Contract: {}, Status: {}, Attempts: {}, Reason: {}\n",
            now, file_key, contract, status, attempts, reason
        ));
    }
    use tokio::io::AsyncWriteExt;
    if let Ok(mut file) = tokio::fs::OpenOptions::new().create(true).append(true).open(&failed_path).await {
        let _ = file.write_all(log_lines.as_bytes()).await;
        let _ = file.flush().await;
    }
}

pub(crate) async fn record_failed_upload_detailed(
    storage_root: &std::path::Path,
    file_key: &str,
    contract: &str,
    reason: &str,
    attempts: u32,
    status: &str,
) {
    record_failed_upload_batch(storage_root, &[file_key.to_string()], contract, reason, attempts, status).await;
}

#[allow(dead_code)]
pub(crate) async fn record_failed_upload(storage_root: &std::path::Path, file_key: &str, contract: &str, reason: &str) {
    record_failed_upload_detailed(storage_root, file_key, contract, reason, 1, "PENDING_BACKGROUND_RETRY").await;
}

#[allow(dead_code)]
pub(crate) async fn record_failed_download(storage_root: &std::path::Path, download_key: &str, contract: &str, reason: &str) {
    record_failed_download_detailed(storage_root, download_key, contract, reason, MAIN_TX_RETRIES, "PENDING_BACKGROUND_RETRY").await;
}

pub(crate) async fn record_failed_download_detailed(
    storage_root: &std::path::Path,
    download_key: &str,
    contract: &str,
    reason: &str,
    attempts: u32,
    status: &str,
) {
    let now = chrono::Local::now().format("%Y-%m-%d %H:%M:%S").to_string();
    let json_path = storage_root.join("failed_confirmations.json");
    let record = crate::models::FailedTxRecord {
        key: download_key.to_string(),
        contract_address: contract.to_string(),
        attempts,
        max_attempts: MAX_BACKGROUND_RETRIES,
        reason: reason.to_string(),
        status: status.to_string(),
        first_failed_at: now.clone(),
        last_attempt_at: now.clone(),
        next_retry_at: None,
        transient_attempts: 0,
    };
    upsert_failed_record(&json_path, record).await;

    let failed_path = storage_root.join("failed_confirmations.txt");
    let line = format!("[{}] DownloadKey: {}, Contract: {}, Status: {}, Attempts: {}, Reason: {}\n", now, download_key, contract, status, attempts, reason);
    use tokio::io::AsyncWriteExt;
    if let Ok(mut file) = tokio::fs::OpenOptions::new().create(true).append(true).open(&failed_path).await {
        let _ = file.write_all(line.as_bytes()).await;
        let _ = file.flush().await;
    }
}
