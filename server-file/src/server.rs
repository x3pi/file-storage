// src/server.rs

use crate::app::App;
use crate::download_manager;
use crate::ethereum::{handle_download_request, verify_download_chunk, verify_upload_chunk};
use crate::models::{
    Command, DownloadResponse, GenericResponse, ListChunksResponse, LogFileContent, LogsContentResponse, LogsListResponse
};
use base64::{engine::general_purpose, Engine as _};
use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
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
        None => return Err("Failed to downcast to QuicConnection".into()),
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
                break; // Thoát vòng lặp
            }
        };
        
        match stream_handler.recv().await {
            Ok(Some(data)) => {
                let line = String::from_utf8_lossy(&data).trim().to_string();
                if line.is_empty() {
                    log::warn!("[{}] ⚠️ Nhận được stream rỗng, bỏ qua.", peer);
                    continue; // Chờ stream tiếp theo
                }
                let command: Command = match serde_json::from_str(&line) {
                    Ok(cmd) => cmd,
                    Err(e) => {
                        log::error!("[{}] ❌ Failed to parse command: {}", peer, e);
                        log::error!("Raw data: {}", line);
                        // Gửi lỗi TRÊN STREAM NÀY
                        if let Err(e) =
                            send_error_response(&mut stream_handler, "Invalid command format").await
                        {
                            eprintln!("[{}] Error sending error response: {}", peer, e);
                        }
                        continue; // Chờ stream tiếp theo
                    }
                };
                let app_clone = app.clone();
                let peer_clone = peer; // SocketAddr là Copy

                tokio::spawn(async move {
                    let mut stream_handler = stream_handler;
                    let semaphore = app_clone.task_semaphore.clone();
                    match command {
                        Command::UploadChunk { payload } => {
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
                            match verify_upload_chunk(&payload, &app_clone).await {
                                Ok(true) => {
                                    // log::info!(
                                    //     "[{}] ✅ Upload signature verified for chunk {}",
                                    //     peer_clone,
                                    //     payload.chunk_index
                                    // );
                                }
                                Ok(false) | Err(_) => {
                                    log::error!(
                                        "[{}] ❌ Upload signature verification FAILED for chunk {}",
                                        peer_clone,
                                        payload.chunk_index
                                    );
                                    if let Err(e) = send_error_response(
                                        &mut stream_handler,
                                        "Invalid upload signature or permission denied",
                                    )
                                    .await
                                    {
                                        log::error!(
                                            "[{}] Error sending error response: {}",
                                            peer_clone,
                                            e
                                        );
                                    }
                                    return; // Thoát task
                                }
                            }
                            let chunk_data = match general_purpose::STANDARD
                                .decode(&payload.chunk_data_base64)
                            {
                                Ok(data) => data,
                                Err(e) => {
                                    log::error!(
                                        "[{}] Failed to decode chunk data: {}",
                                        peer_clone,
                                        e
                                    );
                                    if let Err(e) = send_error_response(
                                        &mut stream_handler,
                                        "Invalid Base64 data",
                                    )
                                    .await
                                    {
                                        log::error!(
                                            "[{}] Error sending error response: {}",
                                            peer_clone,
                                            e
                                        );
                                    }
                                    return; // Thoát task
                                }
                            };
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
                                    let chunk_path = file_dir.join(chunk_index.to_string());
                                    if chunk_path.exists() {
                                        return Err(std::io::Error::new(
                                            std::io::ErrorKind::AlreadyExists,
                                            format!("Chunk {} already exists on disk", chunk_index),
                                        ));
                                    }
                                    std::fs::write(&chunk_path, chunk_data)?;
                                    Ok(chunk_path)
                                })
                                .await
                                .map_err(|e| {
                                    std::io::Error::new(
                                        std::io::ErrorKind::Other,
                                        format!("Task join error: {}", e),
                                    )
                                })
                                .and_then(|inner_result| inner_result);
                            match store_result {
                                Ok((_chunk_path)) => {
                                    let response = GenericResponse {
                                        status: "SUCCESS".to_string(),
                                        message: "Chunk stored successfully".to_string(),
                                    };
                                    let mut response_json =
                                        serde_json::to_vec(&response).unwrap_or_default(); // Sửa lỗi unwrap
                                    response_json.push(b'\n');
                                    if let Err(e) =
                                        stream_handler.send(Bytes::from(response_json)).await
                                    {
                                        log::error!(
                                            "[{}] Error sending success response: {}",
                                            peer_clone,
                                            e
                                        );
                                    }
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
                            let verify_result = verify_download_chunk(&payload, &app_clone,request_ip).await;
                           
                            match verify_result {
                                Ok(true) => {
                                    let response =
                                        handle_download_request(&payload, &app_clone).await;
                                    let send_result =
                                        send_download_response(&mut stream_handler, &response)
                                            .await;
                                    if send_result.is_ok() && response.status == "SUCCESS" {
                                        match download_manager::descrease_chunk_count(
                                            &payload.download_key,
                                            &app_clone,
                                        ) {
                                            Ok(remaining) => {
                                                // log::info!(
                                                //     "[{}] ✅ Chunk decreased. Remaining: {}",
                                                //     peer_clone,
                                                //     remaining
                                                // );
                                            }
                                            Err(e) => {
                                                log::error!(
                                                    "[{}] ⚠️  Failed to decrease chunk count for {}: {}. Client already received chunk.",
                                                    peer_clone, payload.download_key, e
                                                );
                                            }
                                        }
                                    } else if let Err(e) = send_result {
                                        // log::error!("[{}] ❌ Failed to send download response: {}.", peer_clone, e);
                                    } else {
                                        log::warn!(
                                            "[{}] ⚠️  Sent error response to client: {}",
                                            peer_clone,
                                            response.message
                                        );
                                    }
                                }
                                Ok(false) => {
                                    let error_message =
                                        "Ownership or signature verification failed".to_string();
                                    // log::error!(
                                    //     "[{}] ❌ Verification failed: {}",
                                    //     peer_clone,
                                    //     error_message
                                    // );
                                    let response = DownloadResponse {
                                        status: "ERROR".to_string(),
                                        message: error_message,
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
                            
                            let logs_dir = PathBuf::from("./log");
                            let mut files_with_meta = Vec::new();

                            let mut entries = match fs::read_dir(&logs_dir).await {
                                Ok(entries) => entries,
                                Err(e) => {
                                    log::error!("[{}] Failed to read log directory: {}", peer_clone, e);
                                    let response = LogsListResponse {
                                        status: "ERROR".to_string(),
                                        message: format!("Failed to read log directory: {}", e),
                                        available_files: vec![],
                                    };
                                    if let Err(e) = send_logs_list_response(&mut stream_handler, &response).await {
                                        log::error!("[{}] Error sending logs list response: {}", peer_clone, e);
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
                            
                            let all_file_names: Vec<String> = files_with_meta.iter()
                                .map(|(path, _)| path.file_name().unwrap_or_default().to_string_lossy().to_string())
                                .collect();

                            let message = format!("Found {} log files.", all_file_names.len());
                            
                            let response = LogsListResponse {
                                status: "SUCCESS".to_string(),
                                message,
                                available_files: all_file_names,
                            };
                            
                            // Gửi response (payload nhỏ, sẽ chạy nhanh)
                            if let Err(e) = send_logs_list_response(&mut stream_handler, &response).await {
                                log::error!("[{}] Error sending logs list response: {}", peer_clone, e);
                            }
                        }

                        // --- API 2: Lấy nội dung file ---
                        Command::GetLogContent { payload } => {
                            log::debug!("[{}] Handling GetLogContent (no semaphore)", peer_clone);

                            let logs_dir = PathBuf::from("./log");
                            let file_to_read_path = logs_dir.join(&payload.file_name);
                            
                            let mut message: String;
                            let mut response_status = "SUCCESS".to_string();
                            let mut final_log_content: Option<LogFileContent> = None;

                            if !file_to_read_path.starts_with(&logs_dir) || !file_to_read_path.is_file() {
                                // Ngăn chặn tấn công (directory traversal) và kiểm tra file tồn tại
                                message = format!("Error: File '{}' not found or invalid.", payload.file_name);
                                response_status = "ERROR".to_string();
                            } else {
                                // File hợp lệ -> Đọc nội dung
                                match fs::read_to_string(&file_to_read_path).await {
                                    Ok(content) => {
                                        final_log_content = Some(LogFileContent { 
                                            file_name: payload.file_name.clone(), 
                                            content 
                                        });
                                        message = format!("Retrieved content for file: {}", payload.file_name);
                                    }
                                    Err(e) => {
                                        log::warn!("[{}] Failed to read log file {:?}: {}", peer_clone, file_to_read_path, e);
                                        message = format!("Found file '{}', but failed to read its content: {}", payload.file_name, e);
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
                            if let Err(e) = send_logs_content_response(&mut stream_handler, &response).await {
                                log::error!("[{}] Error sending logs content response: {}", peer_clone, e);
                            }
                        }
                    }
                }); // Kết thúc tokio::spawn
            }
            Ok(None) => {
                // Stream đóng trước khi có data
                println!("[{}] ⚠️ Stream closed prematurely (no data).", peer);
                continue; // Chờ stream tiếp theo
            }
            Err(e) => {
                // Lỗi đọc trên stream
                eprintln!("[{}] ❌ Error reading from stream: {}", peer, e);
                continue; // Chờ stream tiếp theo
            }
        }
    } // Kết thúc vòng lặp loop (chờ stream mới)

    Ok(())
}
