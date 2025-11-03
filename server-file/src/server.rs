// src/server.rs

use crate::app::App;
use crate::download_manager;
use crate::ethereum::{handle_download_request, verify_download_chunk, verify_upload_chunk};
use crate::models::{Command, DownloadResponse, GenericResponse};
use base64::{engine::general_purpose, Engine as _};
use chrono::Local;
use std::path::PathBuf;
use std::sync::Arc;
// QUIC imports
use bytes::Bytes;
use network::quic::{QuicConnection, QuicStreamHandler}; // ✅ THÊM Imports
use network::transport::Connection;

// 🔥 THAY ĐỔI: Các hàm này giờ nhận QuicStreamHandler
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
    // ✅ GỌI stream.send
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

pub async fn handle_connection(
    mut connection: Box<dyn Connection>,
    peer: std::net::SocketAddr,
    app: Arc<App>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // println!(
    //     "\n[{}] 🔵 New connection accepted (Sẵn sàng nhận nhiều stream)",
    //     peer
    // );
    // 🔥 THAY ĐỔI: Downcast `Box<dyn Connection>` về `QuicConnection`
    let quic_conn = match connection.as_any_mut().downcast_mut::<QuicConnection>() {
        Some(conn) => conn,
        None => return Err("Failed to downcast to QuicConnection".into()),
    };
    // Vòng lặp này chấp nhận MỘT STREAM MỚI mỗi lần
    loop {
        let mut stream_handler = match quic_conn.accept_stream().await {
            Ok(handler) => handler,
            Err(e) => {
                // Kiểm tra xem có phải lỗi đóng kết nối bình thường không
                if let Some(io_err) = e.downcast_ref::<std::io::Error>() {
                    if io_err.kind() == std::io::ErrorKind::ConnectionAborted {
                        println!("[{}] 📭 Connection closed by client.", peer);
                        break; // Thoát vòng lặp, kết thúc handle_connection
                    }
                }
                // Lỗi khác
                eprintln!("[{}] ❌ Error accepting stream: {}", peer, e);
                break; // Thoát vòng lặp
            }
        };
        // Đọc MỘT request từ stream này
        match stream_handler.recv().await {
            Ok(Some(data)) => {
                // Client Go gửi kèm \n, nên ta trim nó đi
                let line = String::from_utf8_lossy(&data).trim().to_string();
                if line.is_empty() {
                    println!("[{}] ⚠️  Nhận được stream rỗng, bỏ qua.", peer);
                    continue; // Chờ stream tiếp theo
                }
                let command: Command = match serde_json::from_str(&line) {
                    Ok(cmd) => cmd,
                    Err(e) => {
                        eprintln!("[{}] ❌ Failed to parse command: {}", peer, e);
                        eprintln!("Raw data: {}", line);
                        // Gửi lỗi TRÊN STREAM NÀY
                        if let Err(e) =
                            send_error_response(&mut stream_handler, "Invalid command format").await
                        {
                            eprintln!("[{}] Error sending error response: {}", peer, e);
                        }
                        continue; // Chờ stream tiếp theo
                    }
                };
                // (Logic match command bên trong giữ nguyên từ code gốc của bạn)
                match command {
                    Command::UploadChunk { payload } => {
                        println!(
                            "[{}] UploadChunk - file: {}, chunk: {}",
                            peer, payload.file_key, payload.chunk_index
                        );
                        match verify_upload_chunk(&payload, &app).await {
                            Ok(true) => {
                                println!("[{}] ✅ Upload signature verified for chunk {}", peer, payload.chunk_index);
                            }
                            Ok(false) | Err(_) => {
                                // Xác minh thất bại
                                eprintln!("[{}] ❌ Upload signature verification FAILED for chunk {}", peer, payload.chunk_index);
                                if let Err(e) = send_error_response(
                                    &mut stream_handler,
                                    "Invalid upload signature or permission denied",
                                )
                                .await
                                {
                                    eprintln!("[{}] Error sending error response: {}", peer, e);
                                }
                                continue; // Bỏ qua, chờ stream tiếp theo
                            }
                        }
                       
                        let chunk_data = match general_purpose::STANDARD
                            .decode(&payload.chunk_data_base64)
                        {
                            Ok(data) => data,
                            Err(e) => {
                                eprintln!("[{}] Failed to decode chunk data: {}", peer, e);
                                if let Err(e) =
                                    send_error_response(&mut stream_handler, "Invalid Base64 data")
                                        .await
                                {
                                    eprintln!("[{}] Error sending error response: {}", peer, e);
                                }
                                continue;
                            }
                        };
                        let file_key = payload.file_key.clone();
                        let chunk_index = payload.chunk_index;
                        let storage_root = app.storage_root.clone();

                        let store_result: Result<PathBuf, std::io::Error> =
                            tokio::task::spawn_blocking(move || {
                                let level1 = &file_key[0..2];
                                let level2 = &file_key[2..4];
                                let file_dir: PathBuf = storage_root
                                    .join(level1)
                                    .join(level2)
                                    .join(&file_key);
                                std::fs::create_dir_all(&file_dir)?;
                                let chunk_path = file_dir.join(chunk_index.to_string());
                                if chunk_path.exists() {
                                    return Err(std::io::Error::new(
                                        std::io::ErrorKind::AlreadyExists,
                                        format!("Chunk {} already exists on disk", chunk_index)
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
                            })?;
                      
                        // 🔥 THAY ĐỔI: Gửi phản hồi trên stream
                        match store_result {
                            Ok((chunk_path)) => {
                                let response = GenericResponse {
                                    status: "SUCCESS".to_string(),
                                    message: "Chunk stored successfully".to_string(),
                                };

                                let mut response_json = serde_json::to_vec(&response)?;
                                response_json.push(b'\n');
                                stream_handler.send(Bytes::from(response_json)).await?;
                            }
                            Err(e) => {
                                eprintln!("[{}] ❌ Failed to store chunk: {}", peer, e);
                                if let Err(e) = send_error_response(
                                    &mut stream_handler,
                                    "Failed to store chunk on disk",
                                )
                                .await
                                {
                                    eprintln!("[{}] Error sending error response: {}", peer, e);
                                }
                            }
                        }
                    }

                    Command::DownloadChunkRequest { payload } => {
                        println!(
                            "[{}] 📥 DownloadChunkRequest - downloadKey: {}, chunk: {}",
                            peer, payload.download_key, payload.chunk_index
                        );
                        match verify_download_chunk(&payload, &app).await {
                            Ok(true) => {
                                let response = handle_download_request(&payload, &app).await;
                                let send_result =
                                    send_download_response(&mut stream_handler, &response).await;

                                if send_result.is_ok() && response.status == "SUCCESS" {
                                    match download_manager::descrease_chunk_count(
                                        &payload.download_key,
                                        &app,
                                    ) {
                                        Ok(remaining) => {
                                            println!(
                                                "[{}] ✅ Chunk decreased. Remaining: {}",
                                                peer, remaining
                                            );
                                        }
                                        Err(e) => {
                                            eprintln!(
                                                "[{}] ⚠️  Failed to decrease chunk count for {}: {}. Client already received chunk.",
                                                peer, payload.download_key, e
                                            );
                                        }
                                    }
                                } else if let Err(e) = send_result {
                                    eprintln!("[{}] ❌ Failed to send download response: {}. Chunk count not decreased.", peer, e);
                                } else {
                                    println!(
                                        "[{}] ⚠️  Sent error response to client: {}",
                                        peer, response.message
                                    );
                                }
                            }
                            Ok(false)  => {
                                let error_message =
                                    "Ownership or signature verification failed".to_string();
                                eprintln!("[{}] ❌ Verification failed: {}", peer, error_message);
                                let response = DownloadResponse {
                                    status: "ERROR".to_string(),
                                    message: error_message,
                                    chunk_data_base64: None,
                                };
                                // 🔥 THAY ĐỔI: Gửi phản hồi lỗi trên stream
                                if let Err(e) =
                                    send_download_response(&mut stream_handler, &response).await
                                {
                                    eprintln!("[{}] Error sending error response: {}", peer, e);
                                }
                            }
                            Err(er) => {
                                eprintln!("_____--[{}] ❌ Verification error: {}", peer, er);
                                let response = DownloadResponse {
                                    status: "ERROR".to_string(),
                                    message: format!("Verification error: {}", er),
                                    chunk_data_base64: None,
                                };
                                // 🔥 THAY ĐỔI: Gửi phản hồi lỗi trên stream
                                if let Err(e) =
                                    send_download_response(&mut stream_handler, &response).await
                                {
                                    eprintln!("[{}] Error sending error response: {}", peer, e);
                                }
                            }
                        }
                    }
                }
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

    println!("[{}] 🔵 Connection handler finished.", peer);
    Ok(())
}