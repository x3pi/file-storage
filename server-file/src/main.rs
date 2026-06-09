mod app;
mod config;
mod download_manager;
mod ethereum;
mod file_contract;
mod listener;
mod models;
mod retry;
mod server;
use crate::app::App;
use crate::models::ConfirmationReceiver;
use alloy::primitives::B256;
use flexi_logger::{detailed_format, Cleanup, Criterion, FileSpec, Logger, Naming};
use network::quic::QuicTransport;
use network::transport::Transport;
use rlimit::{getrlimit, Resource};
use std::env;
use std::fs;
use std::sync::Arc;
use sysinfo::System;
use tokio::sync::MutexGuard; // Cần thiết để định nghĩa chính xác process_confirmation_queue
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

    let _logger = Logger::try_with_str("info") // Log level debug để xem chi tiết
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

    // Spawn confirmation worker
    let app_clone = app.clone();
    tokio::spawn(async move {
        // Lấy lock cho Mutex<Receiver>
        let mut receiver = app_clone.confirmation_receiver.lock().await;
        process_confirmation_queue(&mut receiver, app_clone.clone()).await;
        log::error!("💀💀💀 CRITICAL: Confirmation worker died unexpectedly!");
    });
    let app_clone = app.clone();
    tokio::spawn(async move {
        listener::start_chain_id_monitor(app_clone).await;
        log::error!("💀💀💀 CRITICAL: Chain ID monitor died unexpectedly!");
    });

    // Spawn event listener (WebSocket)
    let app_clone = app.clone();
    tokio::spawn(async move {
        listener::listen_download_confirmed_events(app_clone).await;
        log::error!("💀💀💀 CRITICAL: Event listener died unexpectedly!");
    });

    // Load certificate và private key từ file trong thư mục hiện tại (cùng cấp với src)
    let cert_path = if fs::metadata("certificate.pem").is_ok() {
        Some("certificate.pem")
    } else {
        None
    };

    let key_path = if fs::metadata("private.key").is_ok() {
        Some("private.key")
    } else {
        None
    };
    let transport = if let (Some(cert), Some(key)) = (cert_path, key_path) {
        log::info!("🔐 Loading QUIC certificate from: {}", cert);
        log::info!("🔐 Loading QUIC private key from: {}", key);
        QuicTransport::new_with_certs(Some(cert), Some(key))
    } else {
        eprintln!("\n❌ ERROR: QUIC certificate and private TLS key not found!");
        std::process::exit(1);
    };
    let addr: std::net::SocketAddr = listen_addr.parse().expect("Invalid listen address");
    let mut listener = transport
        .listen(addr)
        .await
        .expect("Could not create QUIC listener");

    // THÊM: Signal handler để bắt shutdown
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
async fn process_confirmation_queue(
    receiver: &mut MutexGuard<'_, ConfirmationReceiver>,
    app: Arc<App>,
) {
    while let Some(download_key) = receiver.recv().await {
        let app_clone = app.clone();
        if let Err(e) = handle_single_confirmation(download_key, app_clone).await {
            log::error!("❌ Error processing single confirmation: {:?}", e);
        }
    }
}

async fn handle_single_confirmation(
    download_key: String,
    app: Arc<App>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let download_key_bytes = hex::decode(&download_key)?;
    let download_key_b256 = B256::from_slice(&download_key_bytes);
    let contract = app.contract_with_signer().await?;
    let pending_tx = contract
        .confirmServerDownload(download_key_b256)
        .send()
        .await?;

    // Đợi transaction được mine
    pending_tx.get_receipt().await?;
    Ok(())
}
