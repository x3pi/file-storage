/// wt_server.rs
/// WebTransport server cho Web Client (trình duyệt).
/// Thay thế hoàn toàn HTTPS Axum cũ.
///
/// API dựa trên wtransport 0.5 (https://crates.io/crates/wtransport).
///
/// Frame format (Request từ Client):
///   [4 byte BE uint32 length][JSON payload]
///   JSON: { "id": "<uuid>", "command": "download_chunk",
///            "payload": { "download_key": "...", "chunk_index": 0, "signature": "..." } }
///
/// Frame format (Response thành công):
///   [4 byte BE uint32 length][2 byte JSON length][JSON header bytes][raw chunk bytes]
///   JSON: { "id": "...", "command": "...", "status": "success", "chunk_index": 0 }
///
/// Frame format (Response lỗi):
///   [4 byte BE uint32 length][2 byte JSON length][JSON header bytes]
///   JSON: { "id": "...", "command": "...", "status": "error", "message": "..." }
use crate::app::App;
use crate::download_manager;
use crate::ethereum::{handle_download_request, verify_download_chunk};
use crate::models::DownloadChunkPayload;

use serde::{Deserialize, Serialize};

use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use wtransport::endpoint::IncomingSession;
use wtransport::{Endpoint, Identity, ServerConfig};

/// Request JSON từ browser gửi lên qua WebTransport stream.
#[derive(Deserialize, Debug)]
struct WtDownloadRequest {
    /// UUID do client tự sinh — server phải echo lại trong response
    pub id: String,
    /// Phải là "download_chunk"
    pub command: String,
    pub payload: WtDownloadPayload,
}

#[derive(Deserialize, Debug)]
struct WtDownloadPayload {
    pub download_key: String,
    pub chunk_index: u64,
    pub signature: String,
}

/// Khởi động WebTransport Server lắng nghe tại `addr`.
pub async fn run_wt_server(
    app: Arc<App>,
    addr: SocketAddr,
    cert_path: std::path::PathBuf,
    key_path: std::path::PathBuf,
) {
    let identity = match Identity::load_pemfiles(&cert_path, &key_path).await {
        Ok(id) => id,
        Err(e) => {
            log::error!("❌ [WT] Failed to load TLS identity: {}", e);
            return;
        }
    };

    // ServerConfig::builder() returns ServerConfig directly in wtransport 0.5
    let config = ServerConfig::builder()
        .with_bind_address(addr)
        .with_identity(identity)
        .build();

    let endpoint = match Endpoint::server(config) {
        Ok(ep) => ep,
        Err(e) => {
            log::error!("❌ [WT] Failed to create server endpoint: {}", e);
            return;
        }
    };

    log::info!("🚀 [WT] WebTransport server listening on {}", addr);

    loop {
        // accept() returns IncomingSession
        let incoming = endpoint.accept().await;
        let app_clone = app.clone();
        tokio::spawn(async move {
            handle_incoming(incoming, app_clone).await;
        });
    }
}

/// Xử lý một IncomingSession: hoàn tất HTTP/3 handshake, sau đó xử lý streams.
async fn handle_incoming(incoming: IncomingSession, app: Arc<App>) {
    // Bước 1: Chờ session request (HTTP/3 CONNECT upgrade)
    let session_request = match incoming.await {
        Ok(req) => req,
        Err(e) => {
            log::warn!("[WT] Session request failed: {}", e);
            return;
        }
    };

    let peer_str = format!(
        "{}:{}",
        session_request.authority(),
        session_request.path()
    );

    // Bước 2: Accept session → lấy Connection
    let connection = match session_request.accept().await {
        Ok(conn) => conn,
        Err(e) => {
            log::warn!("[WT][{}] Accept session failed: {}", peer_str, e);
            return;
        }
    };

    // Lấy IP của peer từ connection
    let peer_ip: IpAddr = connection.remote_address().ip();
    log::info!("[WT] New connection from {}", connection.remote_address());

    // Bước 3: Vòng lặp chấp nhận Bidirectional Streams
    loop {
        let (send_stream, recv_stream) = match connection.accept_bi().await {
            Ok(s) => s,
            Err(e) => {
                log::info!("[WT][{}] Connection closed: {}", peer_ip, e);
                break;
            }
        };

        let app_inner = app.clone();
        tokio::spawn(async move {
            handle_stream(send_stream, recv_stream, app_inner, peer_ip).await;
        });
    }
}

/// Xử lý 1 BiStream = 1 download_chunk request
async fn handle_stream(
    mut send: wtransport::SendStream,
    mut recv: wtransport::RecvStream,
    app: Arc<App>,
    peer_ip: IpAddr,
) {
    // --- Đọc Request Frame ---
    let payload_bytes = match read_frame(&mut recv).await {
        Ok(b) => b,
        Err(e) => {
            log::warn!("[WT][{}] Failed to read frame: {}", peer_ip, e);
            let _ = send_error_frame(&mut send, "", "", &format!("frame_read_error: {}", e)).await;
            return;
        }
    };

    let req: WtDownloadRequest = match serde_json::from_slice(&payload_bytes) {
        Ok(r) => r,
        Err(e) => {
            log::warn!("[WT][{}] Invalid JSON: {}", peer_ip, e);
            let _ = send_error_frame(&mut send, "", "", &format!("invalid_json: {}", e)).await;
            return;
        }
    };

    if req.command != "download_chunk" {
        let _ = send_error_frame(&mut send, &req.id, &req.command, "unsupported command").await;
        return;
    }

    let resp_command = "chunk_response";

    // --- Xây dựng payload để tái dụng business logic hiện có ---
    let mut dl_payload = DownloadChunkPayload {
        file_key: String::new(), // Sẽ điền sau verify
        download_key: req.payload.download_key.clone(),
        chunk_index: req.payload.chunk_index,
        signature: req.payload.signature.clone(),
    };

    // --- Verify signature và session ---
    match verify_download_chunk(&dl_payload, &app, peer_ip).await {
        Ok(true) => {}
        Ok(false) => {
            let _ = send_error_frame(&mut send, &req.id, resp_command, "verification failed").await;
            return;
        }
        Err(e) => {
            let _ = send_error_frame(&mut send, &req.id, resp_command, &e).await;
            return;
        }
    }

    // Điền file_key từ session cache (bắt buộc trước khi gọi handle_download_request)
    match app.download_cache.get(&req.payload.download_key) {
        Some(session) => {
            dl_payload.file_key = session.file_key.clone();
        }
        None => {
            let _ =
                send_error_frame(&mut send, &req.id, resp_command, "session not found after verify").await;
            return;
        }
    }

    // --- Lấy chunk data (tái dụng logic hiện có) ---
    let (response, chunk_data_opt) = handle_download_request(&dl_payload, &app).await;
    if response.status != "SUCCESS" {
        let _ = send_error_frame(&mut send, &req.id, resp_command, &response.message).await;
        return;
    }

    // Lấy raw bytes trực tiếp (không còn base64)
    let chunk_data = match chunk_data_opt {
        Some(data) => data,
        None => {
            let _ = send_error_frame(&mut send, &req.id, resp_command, "failed to get chunk data").await;
            return;
        }
    };

    // --- Gửi Response: [4 byte length][2 byte JSON len][JSON bytes][raw chunk] ---
    if let Err(e) = send_chunk_frame(&mut send, &req.id, resp_command, req.payload.chunk_index, &chunk_data).await {
        log::error!("[WT][{}] Failed to send chunk frame: {}", peer_ip, e);
        return;
    }

    // Cập nhật số chunk còn lại trong session
    let _ = download_manager::descrease_chunk_count(&req.payload.download_key, &app).await;

    log::info!(
        "[WT][{}] ✅ chunk_index={} size={}B key={}",
        peer_ip,
        req.payload.chunk_index,
        chunk_data.len(),
        &req.payload.download_key[..8.min(req.payload.download_key.len())]
    );
    // Đóng luồng gửi tử tế (Graceful Shutdown)
    let _ = send.finish().await;

    // Đọc cạn luồng nhận để không quăng lỗi STOP_SENDING
    let mut buf = [0u8; 128];
    while let Ok(Some(_)) = recv.read(&mut buf).await {
        // Đọc cho đến khi EOF (None) hoặc lỗi
    }
}

// ─── Framing Helpers ────────────────────────────────────────────────────────

const MAX_REQUEST_FRAME: usize = 64 * 1024; // 64KB max cho request JSON

/// Đọc frame: [4 byte BE uint32 length][payload bytes]
async fn read_frame(stream: &mut wtransport::RecvStream) -> Result<Vec<u8>, String> {

    let mut len_buf = [0u8; 4];
    stream
        .read_exact(&mut len_buf)
        .await
        .map_err(|e| format!("read length: {}", e))?;

    let length = u32::from_be_bytes(len_buf) as usize;
    if length == 0 || length > MAX_REQUEST_FRAME {
        return Err(format!("invalid frame length: {}", length));
    }

    let mut buf = vec![0u8; length];
    stream
        .read_exact(&mut buf)
        .await
        .map_err(|e| format!("read payload: {}", e))?;
    Ok(buf)
}

#[derive(Serialize)]
struct ResponseHeader<'a> {
    id: &'a str,
    command: &'a str,
    status: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    chunk_index: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    message: Option<&'a str>,
}

/// Gửi response chunk thành công:
/// [4 byte BE uint32 length][2 byte JSON length][JSON bytes][raw chunk bytes]
async fn send_chunk_frame(
    stream: &mut wtransport::SendStream,
    id: &str,
    command: &str,
    chunk_index: u64,
    data: &[u8],
) -> Result<(), String> {
    use tokio::io::AsyncWriteExt;

    let header = ResponseHeader {
        id,
        command,
        status: "success",
        chunk_index: Some(chunk_index),
        message: None,
    };
    let json_bytes = serde_json::to_vec(&header).map_err(|e| format!("serialize json: {}", e))?;
    
    let json_len = json_bytes.len() as u16;
    let payload_len = 2 + json_bytes.len() + data.len();

    stream
        .write_all(&(payload_len as u32).to_be_bytes())
        .await
        .map_err(|e| format!("write len: {}", e))?;
    stream
        .write_all(&json_len.to_be_bytes())
        .await
        .map_err(|e| format!("write json_len: {}", e))?;
    stream
        .write_all(&json_bytes)
        .await
        .map_err(|e| format!("write json: {}", e))?;
    stream
        .write_all(data)
        .await
        .map_err(|e| format!("write data: {}", e))?;
    stream
        .flush()
        .await
        .map_err(|e| format!("flush: {}", e))?;
    Ok(())
}

/// Gửi error frame:
/// [4 byte BE uint32 length][2 byte JSON length][JSON bytes]
async fn send_error_frame(
    stream: &mut wtransport::SendStream,
    id: &str,
    command: &str,
    msg: &str,
) -> Result<(), String> {
    use tokio::io::AsyncWriteExt;

    let header = ResponseHeader {
        id,
        command,
        status: "error",
        chunk_index: None,
        message: Some(msg),
    };
    let json_bytes = serde_json::to_vec(&header).map_err(|e| format!("serialize json: {}", e))?;
    
    let json_len = json_bytes.len() as u16;
    let payload_len = 2 + json_bytes.len();
    
    stream
        .write_all(&(payload_len as u32).to_be_bytes())
        .await
        .map_err(|e| format!("write err len: {}", e))?;
    stream
        .write_all(&json_len.to_be_bytes())
        .await
        .map_err(|e| format!("write json_len: {}", e))?;
    stream
        .write_all(&json_bytes)
        .await
        .map_err(|e| format!("write json: {}", e))?;
    let _ = stream.flush().await;
    Ok(())
}
