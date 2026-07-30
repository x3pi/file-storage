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
use crate::app::App;
use flexi_logger::{detailed_format, Cleanup, Criterion, FileSpec, Logger, Naming};
use network::transport::Transport;
use rlimit::{getrlimit, Resource};
use std::env;
use std::fs;
use std::sync::Arc;
use sysinfo::System;
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

    if fs::metadata(&log_dir_path).is_ok() {
        if let Err(e) = fs::remove_dir_all(&log_dir_path) {
            // Dùng eprintln! vì logger chưa được khởi tạo
            eprintln!(
                "⚠️ Warning: Could not remove old log directory '{}': {}. Tiếp tục...",
                log_dir_path, e
            );
        } else {
            eprintln!(
                "♻️ Successfully removed old log directory: {}",
                log_dir_path
            );
        }
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
        let mut sys = System::new_all();
        let pid = sysinfo::get_current_pid().expect("Failed to get PID");

        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(15));
        loop {
            interval.tick().await;
            sys.refresh_all();
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

    // (Download confirmation worker has been merged into TX Manager below to prevent nonce collisions)
    let app_clone = app.clone();
    // tokio::spawn(async move {
    //     listener::start_chain_id_monitor(app_clone).await;
    //     log::error!("💀💀💀 CRITICAL: Chain ID monitor died unexpectedly!");
    // });

    // Spawn event listener (WebSocket)
    let app_clone = app.clone();
    tokio::spawn(async move {
        listener::listen_download_confirmed_events(app_clone).await;
        log::error!("💀💀💀 CRITICAL: Event listener died unexpectedly!");
    });

    // ==========================================
    // TRANSACTION MANAGER (SINGLE WORKER)
    // ==========================================
    // Giải quyết triệt để lỗi Nonce Conflict do dùng chung 1 ví
    let app_clone = app.clone();
    tokio::spawn(async move {
        let mut download_receiver = app_clone.confirmation_receiver.lock().await;
        let mut upload_receiver = app_clone.upload_batch_receiver.lock().await;
        
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
                    
                    for (addr, current_batch) in batches {
                        log::info!("🚀 [TX Manager] Priority Upload Confirm ({} files) for contract {}", current_batch.len(), addr);
                        if let Err(e) = crate::ethereum::confirm_upload_batch(app_clone.clone(), addr, current_batch).await {
                            log::error!("❌ [TX Manager] confirm_upload_batch error: {}", e);
                        }
                    }
                }

                // 2. XỬ LÝ DOWNLOAD
                Some((download_key, contract_addr)) = download_receiver.recv() => {
                    log::info!("⏳ [TX Manager] Processing Download Confirm: {}", download_key);
                    if let Err(e) = crate::ethereum::handle_confirm_download(download_key, contract_addr, app_clone.clone()).await {
                        log::error!("❌ Error processing download confirmation: {:?}", e);
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
    crate::sweeper::spawn_confirmation_retry_worker(app.clone());

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
