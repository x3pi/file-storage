mod app;
mod config;
mod download_manager;
mod ethereum;
mod file_contract;
mod listener;
mod models;
mod retry;
mod server;

use std::env;
use std::fs;
use std::sync::Arc;

use crate::app::App;
use crate::models::ConfirmationReceiver;
use alloy::primitives::B256;
use tokio::sync::MutexGuard; // Cần thiết để định nghĩa chính xác process_confirmation_queue
use flexi_logger::{detailed_format, Cleanup, Criterion, FileSpec, Logger, Naming};
use network::quic::QuicTransport;
use network::transport::Transport;
#[tokio::main]
async fn main() {
    let _logger = Logger::try_with_str("info") // Log level mặc định
        .unwrap()
        .log_to_file(
            FileSpec::default()
                .directory("log") 
                .basename("app")
            )
        .append() // <--- THÊM DÒNG NÀY
        .format_for_files(detailed_format) // Format chi tiết cho file
        .format_for_stdout(detailed_format) // Format chi tiết cho console
        .rotate(
            Criterion::Size(2_000_000), 
            Naming::Numbers,        // Đặt tên file xoay vòng là .1, .2
            Cleanup::KeepLogFiles(40), // Chỉ giữ 2 file log
        )
        .duplicate_to_stdout(flexi_logger::Duplicate::All) // Hiển thị log ra cả console
        .start()
        .expect("Could not start logger");
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: {} <listen_address:port>", args[0]);
        eprintln!("Example: {} 127.0.0.1:8001", args[0]);
        return;
    }
    let listen_addr = &args[1];
    let app = Arc::new(App::setup().await.expect("Failed to initialize app"));
    // Tạo storage directory từ config
    if let Err(e) = fs::create_dir_all(&app.storage_root) {

        panic!(
            "Could not create storage directory '{}': {}",
            app.storage_root.display(),
            e
        );
    }
    // Spawn confirmation worker
    let app_clone = app.clone();
    tokio::spawn(async move {
        // Lấy lock cho Mutex<Receiver>
        let mut receiver = app_clone.confirmation_receiver.lock().await;
        process_confirmation_queue(&mut receiver, app_clone.clone()).await;
    });

    // Spawn event listener (WebSocket)
    let app_clone = app.clone();
    tokio::spawn(async move {
        if let Err(e) = listener::listen_download_confirmed_events(app_clone).await {
            log::error!("❌ Event listener failed: {:?}", e)
        }
    });
    let transport = QuicTransport::new();
    let addr: std::net::SocketAddr = listen_addr.parse().expect("Invalid listen address");
    let mut listener = transport
        .listen(addr)
        .await
        .expect("Could not create QUIC listener");

    loop {
        match listener.accept().await {
            Ok((connection, peer_addr)) => {
                let app_clone = app.clone();
                log::info!("✅ New conn IP from {}", peer_addr.ip());
                // ✅ Spawn async task cho mỗi kết nối
                tokio::spawn(async move {
                    if let Err(e) =
                        server::handle_connection(connection, peer_addr, app_clone, peer_addr.ip()).await
                    {
                        log::error!("❌ Error handling connection from {}: {:?}", peer_addr, e);
                    }
                });
            }
            Err(e) => {
                log::error!("❌ Connection failed: {}", e);
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
    // Giải mã chuỗi hex sang bytes
    let download_key_bytes = hex::decode(&download_key)?;
    // Chuyển bytes sang B256
    let download_key_b256 = B256::from_slice(&download_key_bytes);
    // Tạo contract instance
    let contract = app.contract_with_signer().await?;
    // Gọi confirmServerDownload
    let pending_tx = contract
        .confirmServerDownload(download_key_b256)
        .send()
        .await?;

    // Đợi transaction được mine
    pending_tx.get_receipt().await?;
    Ok(())
}
