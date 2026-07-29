/// wt_server.rs
/// WebTransport server cho Web Client (trình duyệt).
/// Thay thế hoàn toàn HTTPS Axum cũ.
///
/// API dựa trên wtransport 0.5 (https://crates.io/crates/wtransport).
///
/// Frame format (Request từ Client):
///   [4 byte BE uint32 payload_len][2 byte BE uint16 json_len][JSON header bytes][Optional Raw Chunk Data]
///   JSON: { "id": "<uuid>", "command": "download_chunk", ... }
///
/// Frame format (Response thành công):
///   [4 byte BE uint32 payload_len][2 byte BE json_len][JSON header bytes][Optional raw chunk bytes]
///   JSON: { "id": "...", "command": "...", "status": "success", "chunk_index": 0 }
///
/// Frame format (Response lỗi):
///   [4 byte BE uint32 payload_len][2 byte BE json_len][JSON header bytes]
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
struct WtRequest {
    pub id: String,
    pub command: String,
    pub payload: serde_json::Value,
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
    let (json_bytes, chunk_data_in) = match read_frame(&mut recv).await {
        Ok(res) => res,
        Err(e) => {
            log::warn!("[WT][{}] Failed to read frame: {}", peer_ip, e);
            let _ = send_error_frame(&mut send, "", "", &format!("frame_read_error: {}", e)).await;
            return;
        }
    };

    let req: WtRequest = match serde_json::from_slice(&json_bytes) {
        Ok(r) => r,
        Err(e) => {
            log::warn!("[WT][{}] Invalid JSON: {}", peer_ip, e);
            let _ = send_error_frame(&mut send, "", "", &format!("invalid_json: {}", e)).await;
            return;
        }
    };

    if req.command == "download_chunk" {
        handle_download_chunk(send, recv, app, peer_ip, req, chunk_data_in).await;
    } else if req.command == "upload_chunk" {
        handle_upload_chunk(send, recv, app, peer_ip, req, chunk_data_in).await;
    } else {
        let _ = send_error_frame(&mut send, &req.id, &req.command, "unsupported command").await;
    }
}

async fn handle_download_chunk(
    mut send: wtransport::SendStream,
    mut recv: wtransport::RecvStream,
    app: Arc<App>,
    peer_ip: IpAddr,
    req: WtRequest,
    _chunk_data_in: Vec<u8>,
) {
    let resp_command = "chunk_response";
    let payload: WtDownloadPayload = match serde_json::from_value(req.payload.clone()) {
        Ok(p) => p,
        Err(e) => {
            let _ = send_error_frame(&mut send, &req.id, resp_command, &format!("invalid payload: {}", e)).await;
            return;
        }
    };

    // --- Xây dựng payload để tái dụng business logic hiện có ---
    let mut dl_payload = DownloadChunkPayload {
        file_key: String::new(), // Sẽ điền sau verify
        download_key: payload.download_key.clone(),
        chunk_index: payload.chunk_index,
        signature: payload.signature.clone(),
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
    match app.download_cache.get(&payload.download_key) {
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
    if let Err(e) = send_chunk_frame(&mut send, &req.id, resp_command, payload.chunk_index, &chunk_data).await {
        log::error!("[WT][{}] Failed to send chunk frame: {}", peer_ip, e);
        return;
    }

    // Cập nhật số chunk còn lại trong session
    let _ = download_manager::descrease_chunk_count(&payload.download_key, &app).await;

    // Đóng luồng gửi tử tế (Graceful Shutdown)
    let _ = send.finish().await;

    // Đọc cạn luồng nhận để không quăng lỗi STOP_SENDING
    let mut buf = [0u8; 128];
    while let Ok(Some(_)) = recv.read(&mut buf).await {
        // Đọc cho đến khi EOF (None) hoặc lỗi
    }
}

async fn handle_upload_chunk(
    mut send: wtransport::SendStream,
    mut recv: wtransport::RecvStream,
    app: Arc<App>,
    peer_ip: IpAddr,
    req: WtRequest,
    chunk_data_in: Vec<u8>,
) {
    let resp_command = "chunk_response";
    let mut payload: crate::models::UploadChunkPayload = match serde_json::from_value(req.payload.clone()) {
        Ok(p) => p,
        Err(e) => {
            let _ = send_error_frame(&mut send, &req.id, resp_command, &format!("invalid payload: {}", e)).await;
            return;
        }
    };
    
    // Đảm bảo file_key thống nhất không có '0x' ở đầu
    payload.file_key = payload.file_key.trim_start_matches("0x").to_string();

    if chunk_data_in.is_empty() {
        let _ = send_error_frame(&mut send, &req.id, resp_command, "chunk data is empty").await;
        return;
    }

    // ✅ DUPLICATE CHUNK EARLY CHECK
    let is_duplicate = {
        if let Some(set) = app.chunk_tracker.get(&payload.file_key) {
            set.contains(&payload.chunk_index)
        } else {
            false
        }
    };

    if is_duplicate {
        let _ = send_chunk_frame(&mut send, &req.id, resp_command, payload.chunk_index, &[]).await;
        return; // Thoát luôn, tiết kiệm CPU và Ổ cứng!
    }

    match crate::ethereum::verify_upload_chunk(&payload, &chunk_data_in, &app).await {
        Ok(_) => {
            match app.write_chunk(&payload.file_key, payload.chunk_index, &chunk_data_in).await {
                Ok(_) => {
                    let _ = send_chunk_frame(&mut send, &req.id, resp_command, payload.chunk_index, &[]).await;
                    
                    // --- THÊM LOGIC TRACKING CHUNKS GIỐNG NHƯ RAW QUIC ---
                    if let Some(cache_entry) = app.upload_file_cache.get(&payload.file_key) {
                        let total_chunks = cache_entry.total_chunks;
                        let expected_chunks = if payload.chunk_index % 2 == 0 {
                            (total_chunks + 1) / 2
                        } else {
                            total_chunks / 2
                        };

                        let is_completed = {
                            let mut set = app.chunk_tracker
                                .entry(payload.file_key.clone())
                                .or_insert_with(std::collections::HashSet::new);
                            set.insert(payload.chunk_index);
                            set.len() as u64 == expected_chunks
                        };

                        if is_completed {
                            log::info!("[WT][{}] 🎯 File {} fully received for this node ({} / {} total chunks). Queueing for confirm.", peer_ip, payload.file_key, expected_chunks, total_chunks);
                            let _ = app.upload_batch_sender.send(payload.file_key.clone()).await;
                            app.chunk_tracker.remove(&payload.file_key);
                        }
                    }
                    // ----------------------------------------------------
                }
                Err(e) => {
                    log::error!("[WT][{}] Write chunk failed: {}", peer_ip, e);
                    let _ = send_error_frame(&mut send, &req.id, resp_command, "disk write error").await;
                }
            }
        }
        Err(e) => {
            log::error!("[WT][{}] Verify upload failed: {}", peer_ip, e);
            let _ = send_error_frame(&mut send, &req.id, resp_command, &e).await;
        }
    }

    let _ = send.finish().await;
    let mut buf = [0u8; 128];
    while let Ok(Some(_)) = recv.read(&mut buf).await {
        // Drain
    }
}

// ─── Framing Helpers ────────────────────────────────────────────────────────

const MAX_REQUEST_FRAME: usize = 64 * 1024; // 64KB max cho request JSON

/// Đọc frame: [4 byte BE uint32 length][2 byte BE json len][JSON][DATA]
async fn read_frame(stream: &mut wtransport::RecvStream) -> Result<(Vec<u8>, Vec<u8>), String> {
    let mut len_buf = [0u8; 4];
    stream
        .read_exact(&mut len_buf)
        .await
        .map_err(|e| format!("read length: {}", e))?;

    let length = u32::from_be_bytes(len_buf) as usize;
    if length < 2 || length > 10 * 1024 * 1024 { // 10MB max
        return Err(format!("invalid frame length: {}", length));
    }

    let mut json_len_buf = [0u8; 2];
    stream
        .read_exact(&mut json_len_buf)
        .await
        .map_err(|e| format!("read json length: {}", e))?;
    
    let json_len = u16::from_be_bytes(json_len_buf) as usize;
    if json_len + 2 > length {
        return Err(format!("invalid json length: {}", json_len));
    }

    let mut json_bytes = vec![0u8; json_len];
    stream
        .read_exact(&mut json_bytes)
        .await
        .map_err(|e| format!("read json payload: {}", e))?;

    let data_len = length - 2 - json_len;
    let mut data_bytes = vec![0u8; data_len];
    if data_len > 0 {
        stream
            .read_exact(&mut data_bytes)
            .await
            .map_err(|e| format!("read data payload: {}", e))?;
    }
    
    Ok((json_bytes, data_bytes))
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
