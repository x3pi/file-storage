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
use chrono::Local;
use tokio::sync::MutexGuard; // Cần thiết để định nghĩa chính xác process_confirmation_queue

use network::quic::QuicTransport;
use network::transport::Transport;
#[tokio::main]
async fn main() {
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
            println!("❌ Event listener failed: {:?}", e)
        }
    });
    // ✅ QUIC Transport and Listener
    let transport = QuicTransport::new();
    let addr: std::net::SocketAddr = listen_addr.parse().expect("Invalid listen address");
    let mut listener = transport
        .listen(addr)
        .await
        .expect("Could not create QUIC listener");

    println!("🚀 QUIC server listening on {}", listen_addr);

    // ✅ Async accept loop
    loop {
        match listener.accept().await {
            Ok((connection, peer_addr)) => {
                let app_clone = app.clone();

                // ✅ Spawn async task cho mỗi kết nối
                tokio::spawn(async move {
                    // println!("🚀 Starting handler for connection from {}", peer_addr);
                    if let Err(e) =
                        server::handle_connection(connection, peer_addr, app_clone).await
                    {
                        eprintln!("❌ Error handling connection from {}: {:?}", peer_addr, e);
                    }
                });
            }
            Err(e) => {
                eprintln!("❌ Connection failed: {}", e);
            }
        }
    }
}
async fn process_confirmation_queue(
    receiver: &mut MutexGuard<'_, ConfirmationReceiver>,
    app: Arc<App>,
) {
    while let Some(download_key) = receiver.recv().await {
        println!(
            "📤 Confirmation task received downloadKey: {}",
            download_key
        );
        let app_clone = app.clone();
        if let Err(e) = handle_single_confirmation(download_key, app_clone).await {
            eprintln!("❌ Error processing single confirmation: {:?}", e);
        }
    }
}

async fn handle_single_confirmation(
    download_key: String,
    app: Arc<App>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    println!(
        "   - Processing confirmation for downloadKey: {}",
        download_key
    );
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
    let receipt = pending_tx.get_receipt().await?;

    println!(
        "✅ Successfully confirmed downloadKey: {}, tx: {:?}",
        download_key, receipt.transaction_hash
    );

    Ok(())
}
