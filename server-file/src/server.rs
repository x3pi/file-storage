// src/server.rs

use crate::app::App;
use crate::download_manager;
use crate::ethereum::{handle_download_request, verify_download_chunk, verify_upload_chunk};
use crate::models::{
    Command, DownloadResponse, GenericResponse, ListChunksResponse, LogFileContent,
    LogsContentResponse, LogsListResponse,
};
use base64::{engine::general_purpose, Engine as _};
use chrono::Local;
use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;
use tokio::fs; // <--- THÊM: Import tokio::fs cho I/O bất đồng bộ
               // QUIC imports
use bytes::Bytes;
use network::quic::{QuicConnection, QuicStreamHandler};
use network::transport::Connection;
async fn send_error_response(
    stream: &mut QuicStreamHandler, // Nhận stream
    message: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let response = GenericResponse {
        status: "ERROR".to_string(),
        message: message.to_string(),
    };
    let mut response_json = serde_json::to_vec(&response)?;
    response_json.push(b'\n');
    stream.send(Bytes::from(response_json)).await?;
    Ok(())
}

async fn send_download_response(
    stream: &mut QuicStreamHandler, // Nhận stream
    response: &DownloadResponse,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut response_json = serde_json::to_vec(response)?;
    response_json.push(b'\n');
    // ✅ GỌI stream.send
    stream.send(Bytes::from(response_json)).await?;
    Ok(())
}
async fn send_list_chunks_response(
    stream: &mut QuicStreamHandler,
    response: &ListChunksResponse,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut response_json = serde_json::to_vec(response)?;
    response_json.push(b'\n');
    stream.send(Bytes::from(response_json)).await?;
    Ok(())
}
async fn send_logs_content_response(
    stream: &mut QuicStreamHandler,
    response: &LogsContentResponse, // <-- Đổi struct
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut response_json = serde_json::to_vec(response)?;
    response_json.push(b'\n');
    stream.send(Bytes::from(response_json)).await?;
    Ok(())
}

// (THÊM HÀM MỚI NÀY)
async fn send_logs_list_response(
    stream: &mut QuicStreamHandler,
    response: &LogsListResponse, // <-- Struct mới
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut response_json = serde_json::to_vec(response)?;
    response_json.push(b'\n');
    stream.send(Bytes::from(response_json)).await?;
    Ok(())
}
fn get_file_key_path(storage_root: &Path, file_key: &str) -> PathBuf {
    let level1 = &file_key[0..2];
    let level2 = &file_key[2..4];
    storage_root
        .join(level1) // Cấp 1
        .join(level2) // Cấp 2
        .join(file_key)
}
pub async fn handle_connection(
    mut connection: Box<dyn Connection>,
    peer: std::net::SocketAddr,
    app: Arc<App>,
    request_ip: IpAddr,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let quic_conn = match connection.as_any_mut().downcast_mut::<QuicConnection>() {
        Some(conn) => conn,
        None => {
            log::error!("[{}] ❌ Failed to downcast to QuicConnection", peer);
            return Err("Failed to downcast to QuicConnection".into());
        }
    };
    loop {
        let mut stream_handler = match quic_conn.accept_stream().await {
            Ok(handler) => handler,
            Err(e) => {
                // Kiểm tra xem có phải lỗi đóng kết nối bình thường không
                if let Some(io_err) = e.downcast_ref::<std::io::Error>() {
                    if io_err.kind() == std::io::ErrorKind::ConnectionAborted {
                        break; // Thoát vòng lặp, kết thúc handle_connection
                    }
                }
                log::error!("[{}] ❌ Error accepting stream: {}", peer, e);
                break; // Thoát vòng lặp
            }
        };
        match stream_handler.recv().await {
            Ok(Some(data)) => {
                log::debug!("[{}] 📥 Received {} bytes from client", peer, data.len());
                let line = String::from_utf8_lossy(&data).trim().to_string();
                if line.is_empty() {
                    log::warn!("[{}] ⚠️ Received empty stream, skipping.", peer);
                    continue; // Chờ stream tiếp theo
                }
                log::debug!("[{}] 🔍 Parsing command...", peer);

                let command: Command = match serde_json::from_str(&line) {
                    Ok(cmd) => cmd,
                    Err(e) => {
                        log::error!("[{}] ❌ Failed to parse command: {}", peer, e);
                        log::error!("[{}] Raw data: {}", peer, line);
                        // Gửi lỗi TRÊN STREAM NÀY
                        if let Err(e) =
                            send_error_response(&mut stream_handler, "Invalid command format").await
                        {
                            log::error!("[{}] Error sending error response: {}", peer, e);
                        }
                        continue; // Chờ stream tiếp theo
                    }
                };
                let app_clone = app.clone();
                let peer_clone = peer; // SocketAddr là Copy
                let start_time = Instant::now();
                let start_time_wall_clock = Local::now();
                tokio::spawn(async move {
                    let mut stream_handler = stream_handler;
                    let semaphore = app_clone.task_semaphore.clone();
                    match command {
                        Command::UploadChunk { payload } => {
                            let log_file_key = payload.file_key.clone();
                            let log_chunk_index = payload.chunk_index;
                            let _permit = match semaphore.acquire().await {
                                Ok(permit) => permit,
                                Err(e) => {
                                    log::error!(
                                        "[{}] Semaphore closed, cannot process upload: {}",
                                        peer_clone,
                                        e
                                    );

                                    _ = send_error_response(
                                        &mut stream_handler,
                                        "Server is shutting down",
                                    )
                                    .await;
                                    return; // Thoát task này
                                }
                            };
                            
                            // ✅ RECEIVE FRAME 2: Binary Chunk Data
                            let chunk_data = match stream_handler.recv().await {
                                Ok(Some(data)) => data.to_vec(),
                                _ => {
                                    log::error!(
                                        "[{}] ❌ Failed to receive binary chunk data (Frame 2) for chunk {} -k {}",
                                        peer_clone,
                                        log_chunk_index,
                                        log_file_key
                                    );
                                    let _ = send_error_response(
                                        &mut stream_handler,
                                        "Failed to receive binary chunk data",
                                    )
                                    .await;
                                    return;
                                }
                            };
                            
                            // ✅ UNIFIED VERIFICATION: Signature + Merkle Proof in one call
                            match verify_upload_chunk(&payload, &chunk_data, &app_clone).await {
                                Ok(()) => {
                                    log::debug!(
                                        "[{}] ✅ Upload verified (signature + merkle proof) for chunk {} -k {}",
                                        peer_clone,
                                        payload.chunk_index,
                                        payload.file_key
                                    );
                                }
                                Err(e) => {
                                    log::error!(
                                        "[{}] ❌ Upload verification FAILED for chunk {} -k {}: {}",
                                        peer_clone,
                                        payload.chunk_index,
                                        payload.file_key,
                                        e
                                    );
                                    if let Err(send_err) = send_error_response(
                                        &mut stream_handler,
                                        &format!("Verification failed: {}", e),
                                    )
                                    .await
                                    {
                                        log::error!(
                                            "[{}] Error sending error response: {}",
                                            peer_clone,
                                            send_err
                                        );
                                    }
                                    return; // Thoát task
                                }
                            }
                            
                            let file_key = payload.file_key.clone();
                            let chunk_index = payload.chunk_index;
                            let storage_root = app_clone.storage_root.clone();
                            let store_result: Result<PathBuf, std::io::Error> =
                                tokio::task::spawn_blocking(move || {
                                    let level1 = &file_key[0..2];
                                    let level2 = &file_key[2..4];
                                    let file_dir: PathBuf =
                                        storage_root.join(level1).join(level2).join(&file_key);
                                    std::fs::create_dir_all(&file_dir)?;
                                    
                                    // CÁCH MỚI: Ghi vào [file_key].bin
                                    let bin_path = file_dir.join(format!("{}.bin", file_key));
                                    let meta_path = file_dir.join(format!("{}.meta", file_key));
                                    
                                    use std::fs::OpenOptions;
                                    use std::io::{Seek, SeekFrom, Write};
                                    
                                    let mut file = OpenOptions::new()
                                        .write(true)
                                        .create(true)
                                        .open(&bin_path)?;
                                        
                                    // Chunk size luôn là 250KB = 256000 bytes
                                    let offset = (chunk_index as u64) * 256000;
                                    file.seek(SeekFrom::Start(offset))?;
                                    file.write_all(&chunk_data)?;
                                    
                                    // Ghi index vào file meta
                                    let mut meta_file = OpenOptions::new()
                                        .append(true)
                                        .create(true)
                                        .open(&meta_path)?;
                                    meta_file.write_all(format!("{}\n", chunk_index).as_bytes())?;
                                    
                                    Ok(bin_path)
                                })
                                .await
                                .map_err(|e| {
                                    log::error!(
                                        "[{}] ❌ Spawn_blocking task panicked: {}",
                                        peer_clone,
                                        e
                                    );
                                    std::io::Error::new(
                                        std::io::ErrorKind::Other,
                                        format!("Task join error: {}", e),
                                    )
                                })
                                .and_then(|inner_result| inner_result);
                            let processing_done_time = Instant::now();
                            let processing_done_wall_clock = Local::now();
                            match store_result {
                                Ok(_chunk_path) => {
                                    let response = GenericResponse {
                                        status: "SUCCESS".to_string(),
                                        message: "Chunk stored successfully".to_string(),
                                    };
                                    let mut response_json: Vec<u8> =
                                        serde_json::to_vec(&response).unwrap_or_default(); // Sửa lỗi unwrap
                                    response_json.push(b'\n');

                                    log::debug!(
                                        "[{}] 📤 Sending SUCCESS response for chunk {} -k {} ({} bytes)",
                                        peer_clone,
                                        log_chunk_index,
                                        log_file_key,
                                        response_json.len()
                                    );

                                    // THÊM: Ghi lại thời điểm gửi xong
                                    let send_start_time = Instant::now();
                                    
                                    if let Err(e) =
                                        stream_handler.send(Bytes::from(response_json)).await
                                    {
                                        log::error!(
                                            "[{}] ❌ Error sending success response: {}",
                                            peer_clone,
                                            e
                                        );
                                    } else {
                                        log::debug!(
                                            "[{}] ✅ SUCCESS response sent for chunk {} -k {}",
                                            peer_clone,
                                            log_chunk_index,
                                            log_file_key
                                        );
                                    }

                                    let send_done_time = Instant::now();

                                    // THÊM: Tính toán và log thời gian
                                    let processing_duration =
                                        processing_done_time.duration_since(start_time);
                                    let send_duration =
                                        send_done_time.duration_since(send_start_time);
                                    let total_duration = send_done_time.duration_since(start_time);
                                    let processing_time_formatted = processing_done_wall_clock
                                        .format("%H:%M:%S.%3f")
                                        .to_string();
                                    let start_time_formatted =
                                        start_time_wall_clock.format("%H:%M:%S.%3f").to_string();
                                    log::info!(
                                "[{}] 📈 [UPLOAD_TIMING] Chunk {} -k {}, .Time: Receive:{:?}, Process: {:?} | Duration: Processing: {:?}, Send: {:?}, Total: {:?}",
                                peer_clone,
                                log_chunk_index,
                                log_file_key,
                                start_time_formatted,
                                processing_time_formatted,
                                processing_duration,
                                send_duration,
                                total_duration
                            );
                                }
                                Err(e) => {
                                    log::error!("[{}] ❌ Failed to store chunk: {}", peer_clone, e);
                                    if let Err(e) = send_error_response(
                                        &mut stream_handler,
                                        "Failed to store chunk on disk",
                                    )
                                    .await
                                    {
                                        log::error!(
                                            "[{}] Error sending error response: {}",
                                            peer_clone,
                                            e
                                        );
                                    }
                                }
                            }
                        }
                        Command::DownloadChunkRequest { payload } => {
                            let log_file_key = payload.file_key.clone();
                            let log_chunk_index = payload.chunk_index;
                            let _permit = match semaphore.acquire().await {
                                Ok(permit) => permit,
                                Err(e) => {
                                    log::error!(
                                        "[{}] Semaphore closed, cannot process download: {}",
                                        peer_clone,
                                        e
                                    );
                                    _ = send_error_response(
                                        &mut stream_handler,
                                        "Server is shutting down",
                                    )
                                    .await;
                                    return; // Thoát task này
                                }
                            };
                            let verify_result =
                                verify_download_chunk(&payload, &app_clone, request_ip).await;
                            log::info!(
                                "[{}] ✅ Download reuqest Chunk {} -k {}",
                                peer_clone,
                                payload.chunk_index,
                                payload.file_key
                            );
                            match verify_result {
                                Ok(true) => {
                                    let response =
                                        handle_download_request(&payload, &app_clone).await;

                                    let processing_done_time = Instant::now();
                                    let start_time_formatted =
                                        start_time_wall_clock.format("%H:%M:%S.%3f").to_string();

                                    let processing_done_wall_clock = Local::now();
                                    let send_result =
                                        send_download_response(&mut stream_handler, &response)
                                            .await;
                                    let send_done_time = Instant::now();

                                    // Nhả semaphore permit ở đây để không làm nghẽn node nếu confirmation_sender bị chặn
                                    drop(_permit);

                                    if send_result.is_ok() && response.status == "SUCCESS" {
                                        match download_manager::descrease_chunk_count(
                                            &payload.download_key,
                                            &app_clone,
                                        ).await {
                                            Ok(_) => {}
                                            Err(e) => {
                                                log::error!(
                                                    "[{}] ⚠️  Failed to decrease chunk count for {}: {}. Client already received chunk.",
                                                    peer_clone, payload.download_key, e
                                                );
                                            }
                                        }
                                    } else if let Err(e) = send_result {
                                        log::error!(
                                            "[{}] ❌ Failed to send download response: {}.",
                                            peer_clone,
                                            e
                                        );
                                    } else {
                                        log::warn!(
                                            "[{}] ⚠️  Sent error response to client: {}",
                                            peer_clone,
                                            response.message
                                        );
                                    }
                                    let processing_duration =
                                        processing_done_time.duration_since(start_time);
                                    let send_duration =
                                        send_done_time.duration_since(processing_done_time);
                                    let total_duration = send_done_time.duration_since(start_time);
                                    let processing_time_formatted = processing_done_wall_clock
                                        .format("%H:%M:%S.%3f")
                                        .to_string();
                                    log::info!(
                                "[{}] 📈 [DOWNLOAD_TIMING] Chunk {} -k {}, .Time: Receive:{:?}, Process: {:?} | Duration: Processing: {:?},  Send: {:?}, Total: {:?}",
                                peer_clone,
                                log_chunk_index,
                                log_file_key,
                                start_time_formatted,
                                processing_time_formatted,
                                processing_duration,
                                send_duration,
                                total_duration
                            );
                                }

                                Ok(false) => {
                                    let error_message =
                                        "Ownership or signature verification failed".to_string();
                                    let response = DownloadResponse {
                                        status: "ERROR".to_string(),
                                        message: error_message,
                                        chunk_data_base64: None,
                                    };
                                    log::error!(
                                        "❌ Download verification FAILED: {}",
                                        response.message
                                    );
                                    if let Err(e) =
                                        send_download_response(&mut stream_handler, &response).await
                                    {
                                        log::error!(
                                            "[{}] Error sending error response: {}",
                                            peer_clone,
                                            e
                                        );
                                    }
                                }
                                Err(er) => {
                                    // log::error!("-[{}] ❌ Verification error: {}", peer_clone, er);
                                    let response = DownloadResponse {
                                        status: "ERROR".to_string(),
                                        message: format!("Verification error: {}", er),
                                        chunk_data_base64: None,
                                    };
                                    if let Err(e) =
                                        send_download_response(&mut stream_handler, &response).await
                                    {
                                        log::error!(
                                            "[{}] Error sending error response: {}",
                                            peer_clone,
                                            e
                                        );
                                    }
                                }
                            }
                        }
                        Command::ListChunksRequest { payload } => {
                            // --- TOÀN BỘ LOGIC ListChunksRequest CŨ CỦA BẠN VÀO ĐÂY ---
                            // (Sử dụng app_clone và peer_clone)
                            let file_path =
                                get_file_key_path(&app_clone.storage_root, &payload.file_key);
                            match download_manager::list_chunks(&file_path).await {
                                Ok(indices) => {
                                    let response = ListChunksResponse {
                                        status: "SUCCESS".to_string(),
                                        message: format!("Found {} chunks", indices.len()),
                                        chunk_indices: indices,
                                    };
                                    if let Err(e) =
                                        send_list_chunks_response(&mut stream_handler, &response)
                                            .await
                                    {
                                        log::error!(
                                            "[{}] Error sending list chunks response: {}",
                                            peer_clone,
                                            e
                                        );
                                    }
                                }
                                Err(e) => {
                                    log::error!("[{}] Failed to list chunks: {}", peer_clone, e);
                                    if let Err(e) =
                                        send_error_response(&mut stream_handler, &format!("{}", e))
                                            .await
                                    {
                                        log::error!(
                                            "[{}] Error sending error response: {}",
                                            peer_clone,
                                            e
                                        );
                                    }
                                }
                            }
                        }
                        Command::GetLogList { payload: _ } => {
                            log::debug!("[{}] Handling GetLogList (no semaphore)", peer_clone);

                            let logs_dir = app_clone.log_dir.clone();
                            let mut files_with_meta = Vec::new();

                            let mut entries = match fs::read_dir(&logs_dir).await {
                                Ok(entries) => entries,
                                Err(e) => {
                                    log::error!(
                                        "[{}] Failed to read log directory: {}",
                                        peer_clone,
                                        e
                                    );
                                    let response = LogsListResponse {
                                        status: "ERROR".to_string(),
                                        message: format!("Failed to read log directory: {}", e),
                                        available_files: vec![],
                                    };
                                    if let Err(e) =
                                        send_logs_list_response(&mut stream_handler, &response)
                                            .await
                                    {
                                        log::error!(
                                            "[{}] Error sending logs list response: {}",
                                            peer_clone,
                                            e
                                        );
                                    }
                                    return; // Thoát task
                                }
                            };

                            while let Ok(Some(entry)) = entries.next_entry().await {
                                let path = entry.path();
                                if path.is_file() {
                                    if let Ok(meta) = fs::metadata(&path).await {
                                        if let Ok(modified) = meta.modified() {
                                            files_with_meta.push((path, modified));
                                        }
                                    }
                                }
                            }

                            // Sắp xếp: file mới nhất lên đầu
                            files_with_meta.sort_by(|a, b| b.1.cmp(&a.1));

                            let all_file_names: Vec<String> = files_with_meta
                                .iter()
                                .map(|(path, _)| {
                                    path.file_name()
                                        .unwrap_or_default()
                                        .to_string_lossy()
                                        .to_string()
                                })
                                .collect();

                            let message = format!("Found {} log files.", all_file_names.len());

                            let response = LogsListResponse {
                                status: "SUCCESS".to_string(),
                                message,
                                available_files: all_file_names,
                            };

                            // Gửi response (payload nhỏ, sẽ chạy nhanh)
                            if let Err(e) =
                                send_logs_list_response(&mut stream_handler, &response).await
                            {
                                log::error!(
                                    "[{}] Error sending logs list response: {}",
                                    peer_clone,
                                    e
                                );
                            }
                        }

                        // --- API 2: Lấy nội dung file ---
                        Command::GetLogContent { payload } => {
                            log::debug!("[{}] Handling GetLogContent (no semaphore)", peer_clone);

                            let logs_dir = app_clone.log_dir.clone();
                            let file_to_read_path = logs_dir.join(&payload.file_name);

                            let message: String;
                            let mut response_status = "SUCCESS".to_string();
                            let mut final_log_content: Option<LogFileContent> = None;

                            if !file_to_read_path.starts_with(&logs_dir)
                                || !file_to_read_path.is_file()
                            {
                                // Ngăn chặn tấn công (directory traversal) và kiểm tra file tồn tại
                                message = format!(
                                    "Error: File '{}' not found or invalid.",
                                    payload.file_name
                                );
                                response_status = "ERROR".to_string();
                            } else {
                                // File hợp lệ -> Đọc nội dung
                                match fs::read_to_string(&file_to_read_path).await {
                                    Ok(content) => {
                                        final_log_content = Some(LogFileContent {
                                            file_name: payload.file_name.clone(),
                                            content,
                                        });
                                        message = format!(
                                            "Retrieved content for file: {}",
                                            payload.file_name
                                        );
                                    }
                                    Err(e) => {
                                        log::warn!(
                                            "[{}] Failed to read log file {:?}: {}",
                                            peer_clone,
                                            file_to_read_path,
                                            e
                                        );
                                        message = format!(
                                            "Found file '{}', but failed to read its content: {}",
                                            payload.file_name, e
                                        );
                                        response_status = "ERROR".to_string();
                                    }
                                }
                            }

                            let response = LogsContentResponse {
                                status: response_status,
                                message,
                                log_content: final_log_content,
                            };

                            // Gửi response (payload LỚN, 500KB+)
                            if let Err(e) =
                                send_logs_content_response(&mut stream_handler, &response).await
                            {
                                log::error!(
                                    "[{}] Error sending logs content response: {}",
                                    peer_clone,
                                    e
                                );
                            }
                        }
                    }
                }); // Kết thúc tokio::spawn
            }
            Ok(None) => {
                // Stream đóng trước khi có data
                log::debug!("[{}] ⚠️ Stream closed prematurely (no data).", peer);
                continue; // Chờ stream tiếp theo
            }
            Err(e) => {
                // Lỗi đọc trên stream
                log::error!("[{}] ❌ Error reading from stream: {}", peer, e);
                continue; // Chờ stream tiếp theo
            }
        }
    } // Kết thúc vòng lặp loop (chờ stream mới)

    Ok(())
}
