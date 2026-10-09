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

pub(crate) const MAX_TX_RETRIES: u32 = 5;
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
        let mut upload_retry_counts: std::collections::HashMap<String, u32> = std::collections::HashMap::new();
        let mut download_retry_counts: std::collections::HashMap<String, u32> = std::collections::HashMap::new();

        // --- 1. STARTUP RECOVERY (Khôi phục các upload/download chưa hoàn thành từ đĩa) ---
        // Chạy trong task riêng biệt (tokio::spawn) để KHÔNG block task chính, tránh deadlock khi pending > 1000 items!
        let app_recovery = app_clone.clone();
        let pending_upload_recovery = pending_upload_path.clone();
        let pending_dl_recovery = pending_dl_path.clone();
        tokio::spawn(async move {
            if tokio::fs::try_exists(&pending_upload_recovery).await.unwrap_or(false) {
                if let Ok(content) = tokio::fs::read_to_string(&pending_upload_recovery).await {
                    let mut count = 0;
                    for line in content.lines() {
                        let parts: Vec<&str> = line.trim().split(',').collect();
                        if parts.len() == 2 {
                            let key = parts[0].trim().to_string();
                            if let Ok(addr) = parts[1].trim().parse::<alloy::primitives::Address>() {
                                if !key.is_empty() {
                                    let _ = app_recovery.upload_batch_sender.send((key, addr)).await;
                                    count += 1;
                                }
                            }
                        }
                    }
                    if count > 0 {
                        log::info!("♻️ [TX Manager Recovery] Khôi phục {} uploads chưa confirm từ đĩa vào hàng đợi.", count);
                    }
                }
            }

            if tokio::fs::try_exists(&pending_dl_recovery).await.unwrap_or(false) {
                if let Ok(content) = tokio::fs::read_to_string(&pending_dl_recovery).await {
                    let mut count = 0;
                    for line in content.lines() {
                        let parts: Vec<&str> = line.trim().split(',').collect();
                        if parts.len() == 2 {
                            let key = parts[0].trim().to_string();
                            let addr = parts[1].trim().to_string();
                            if !key.is_empty() && !addr.is_empty() {
                                let _ = app_recovery.confirmation_sender.send((key, addr)).await;
                                count += 1;
                            }
                        }
                    }
                    if count > 0 {
                        log::info!("♻️ [TX Manager Recovery] Khôi phục {} download confirms chưa hoàn thành từ đĩa.", count);
                    }
                }
            }
        });

        loop {
            tokio::select! {
                // 1. ƯU TIÊN UPLOAD: Xử lý ngay lập tức và gom tất cả những file đang chờ
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

                    // --- PERSIST TO DISK (SINGLE WRITER) ---
                    // Chỉ có duy nhất TX Manager ghi vào pending_uploads.txt nên không có race condition
                    {
                        use tokio::io::AsyncWriteExt;
                        let mut existing_keys = std::collections::HashSet::new();
                        if let Ok(content) = tokio::fs::read_to_string(&pending_upload_path).await {
                            for line in content.lines() {
                                let parts: Vec<&str> = line.trim().split(',').collect();
                                if !parts.is_empty() {
                                    existing_keys.insert(parts[0].trim().to_string());
                                }
                            }
                        }
                        let mut to_append = String::new();
                        for (addr, keys) in &batches {
                            for key in keys {
                                if !existing_keys.contains(key) {
                                    to_append.push_str(&format!("{},{}\n", key, addr));
                                    existing_keys.insert(key.clone());
                                }
                            }
                        }
                        if !to_append.is_empty() {
                            if let Ok(mut file) = tokio::fs::OpenOptions::new().create(true).append(true).open(&pending_upload_path).await {
                                let _ = file.write_all(to_append.as_bytes()).await;
                            }
                        }
                    }
                    
                    for (addr, current_batch) in batches {
                        log::info!("🚀 [TX Manager] Priority Upload Confirm ({} files) for contract {}", current_batch.len(), addr);
                        match crate::ethereum::confirm_upload_batch(app_clone.clone(), addr, current_batch.clone()).await {
                            Ok(()) => {
                                for k in &current_batch {
                                    upload_retry_counts.remove(k);
                                }
                                // CHỈ XOÁ KHỎI ĐĨA KHI TX THÀNH CÔNG TRÊN CHAIN
                                remove_pending_uploads(&pending_upload_path, &current_batch, &addr.to_string()).await;
                            }
                            Err(e) => {
                                let err_str = e.to_string();
                                if is_already_confirmed(&err_str) {
                                    log::info!("✅ [TX Manager] Batch {} file(s) đã được confirm on-chain trước đó. Xóa khỏi pending.", current_batch.len());
                                    for k in &current_batch {
                                        upload_retry_counts.remove(k);
                                    }
                                    remove_pending_uploads(&pending_upload_path, &current_batch, &addr.to_string()).await;
                                } else if is_terminal_contract_revert(&err_str) {
                                    log::error!("❌ [TX Manager] confirm_upload_batch gặp lỗi contract không thể phục hồi: {}. Ghi vào failed_uploads.txt và xóa khỏi pending.", err_str);
                                    for k in &current_batch {
                                        upload_retry_counts.remove(k);
                                        record_failed_upload(&app_clone.storage_root, k, &addr.to_string(), &err_str).await;
                                    }
                                    remove_pending_uploads(&pending_upload_path, &current_batch, &addr.to_string()).await;
                                } else {
                                    // Mọi lỗi mạng / RPC tạm thời (timeout, connection reset, nonce, gas price,...): Retry tối đa 5 lần
                                    let mut to_retry = Vec::new();
                                    let mut exceeded_keys = Vec::new();

                                    for k in current_batch {
                                        let count = upload_retry_counts.entry(k.clone()).or_insert(0);
                                        *count += 1;
                                        if *count <= MAX_TX_RETRIES {
                                            to_retry.push((k, *count));
                                        } else {
                                            upload_retry_counts.remove(&k);
                                            exceeded_keys.push(k);
                                        }
                                    }

                                    if !exceeded_keys.is_empty() {
                                        log::error!(
                                            "🚨 [TX Manager] confirm_upload_batch cho {} file(s) đã THẤT BẠI sau {} lần thử lại: {}. Lưu vào failed_uploads.txt để admin debug và xóa khỏi pending.",
                                            exceeded_keys.len(),
                                            MAX_TX_RETRIES,
                                            err_str
                                        );
                                        for k in &exceeded_keys {
                                            record_failed_upload(
                                                &app_clone.storage_root,
                                                k,
                                                &addr.to_string(),
                                                &format!("Exceeded {} retries. Last error: {}", MAX_TX_RETRIES, err_str),
                                            )
                                            .await;
                                        }
                                        remove_pending_uploads(&pending_upload_path, &exceeded_keys, &addr.to_string()).await;
                                    }

                                    if !to_retry.is_empty() {
                                        let max_attempt = to_retry.iter().map(|(_, c)| *c).max().unwrap_or(1);
                                        // Exponential backoff từ utils module
                                        let delay_secs = crate::utils::retry_delay_secs(max_attempt);
                                        log::warn!(
                                            "⚠️ [TX Manager] confirm_upload_batch gặp lỗi tạm thời: {}. Thử lại {} file(s) sau {}s (lần {}/{})",
                                            err_str,
                                            to_retry.len(),
                                            delay_secs,
                                            max_attempt,
                                            MAX_TX_RETRIES
                                        );
                                        let sender = app_clone.upload_batch_sender.clone();
                                        tokio::spawn(async move {
                                            tokio::time::sleep(tokio::time::Duration::from_secs(delay_secs)).await;
                                            for (k, _) in to_retry {
                                                let _ = sender.send((k, addr)).await;
                                            }
                                        });
                                    }
                                }
                            }
                        }
                    }
                }

                // 2. XỬ LÝ DOWNLOAD
                Some((download_key, contract_addr)) = download_receiver.recv() => {
                    log::info!("⏳ [TX Manager] Processing Download Confirm: {}", download_key);
                    
                    // Ghi nhận vào pending_confirmations.txt trước khi gửi tx nếu chưa có
                    {
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
                            }
                        }
                    }

                    match crate::ethereum::handle_confirm_download(download_key.clone(), contract_addr.clone(), app_clone.clone()).await {
                        Ok(()) => {
                            download_retry_counts.remove(&download_key);
                            // Thành công: Xoá khỏi pending_confirmations.txt
                            remove_pending_download(&pending_dl_path, &download_key).await;
                        }
                        Err(e) => {
                            let err_str = e.to_string();
                            if is_already_confirmed(&err_str) {
                                log::info!("✅ [TX Manager] Download key {} đã được confirm on-chain trước đó. Xóa khỏi pending.", download_key);
                                download_retry_counts.remove(&download_key);
                                remove_pending_download(&pending_dl_path, &download_key).await;
                            } else if is_terminal_contract_revert(&err_str) {
                                log::error!("❌ [TX Manager] Download confirmation gặp lỗi contract không thể phục hồi: {}. Ghi vào failed_confirmations.txt và xóa khỏi pending.", err_str);
                                download_retry_counts.remove(&download_key);
                                record_failed_download(&app_clone.storage_root, &download_key, &contract_addr, &err_str).await;
                                remove_pending_download(&pending_dl_path, &download_key).await;
                            } else {
                                // Mọi lỗi mạng / RPC tạm thời khác -> retry
                                let count = download_retry_counts.entry(download_key.clone()).or_insert(0);
                                *count += 1;
                                if *count <= MAX_TX_RETRIES {
                                    let delay_secs = crate::utils::retry_delay_secs(*count);
                                    log::warn!(
                                        "⚠️ [TX Manager] Download confirmation gặp lỗi tạm thời: {}. Thử lại sau {}s (lần {}/{})",
                                        err_str,
                                        delay_secs,
                                        *count,
                                        MAX_TX_RETRIES
                                    );
                                    let sender = app_clone.confirmation_sender.clone();
                                    let dl_key = download_key.clone();
                                    let c_addr = contract_addr.clone();
                                    tokio::spawn(async move {
                                        tokio::time::sleep(tokio::time::Duration::from_secs(delay_secs)).await;
                                        let _ = sender.send((dl_key, c_addr)).await;
                                    });
                                } else {
                                    download_retry_counts.remove(&download_key);
                                    log::error!(
                                        "🚨 [TX Manager] Download confirmation cho key {} đã THẤT BẠI sau {} lần thử lại: {}. Lưu vào failed_confirmations.txt để admin debug và xóa khỏi pending.",
                                        download_key,
                                        MAX_TX_RETRIES,
                                        err_str
                                    );
                                    record_failed_download(
                                        &app_clone.storage_root,
                                        &download_key,
                                        &contract_addr,
                                        &format!("Exceeded {} retries. Last error: {}", MAX_TX_RETRIES, err_str),
                                    )
                                    .await;
                                    remove_pending_download(&pending_dl_path, &download_key).await;
                                }
                            }
                        }
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
    let tmp_path = path.with_extension("tmp");
    tokio::fs::write(&tmp_path, content).await?;
    tokio::fs::rename(&tmp_path, path).await?;
    Ok(())
}

pub(crate) async fn remove_pending_uploads(path: &std::path::Path, current_batch: &[String], addr_str: &str) {
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

pub(crate) async fn remove_pending_download(path: &std::path::Path, download_key: &str) {
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

pub(crate) async fn record_failed_upload(storage_root: &std::path::Path, file_key: &str, contract: &str, reason: &str) {
    let failed_path = storage_root.join("failed_uploads.txt");
    let now = chrono::Local::now().format("%Y-%m-%d %H:%M:%S").to_string();
    let line = format!("[{}] FileKey: {}, Contract: {}, Reason: {}\n", now, file_key, contract, reason);
    use tokio::io::AsyncWriteExt;
    if let Ok(mut file) = tokio::fs::OpenOptions::new().create(true).append(true).open(&failed_path).await {
        let _ = file.write_all(line.as_bytes()).await;
        let _ = file.flush().await;
    }
}

pub(crate) async fn record_failed_download(storage_root: &std::path::Path, download_key: &str, contract: &str, reason: &str) {
    let failed_path = storage_root.join("failed_confirmations.txt");
    let now = chrono::Local::now().format("%Y-%m-%d %H:%M:%S").to_string();
    let line = format!("[{}] DownloadKey: {}, Contract: {}, Reason: {}\n", now, download_key, contract, reason);
    use tokio::io::AsyncWriteExt;
    if let Ok(mut file) = tokio::fs::OpenOptions::new().create(true).append(true).open(&failed_path).await {
        let _ = file.write_all(line.as_bytes()).await;
        let _ = file.flush().await;
    }
}
