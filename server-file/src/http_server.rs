use crate::app::App;
use crate::ethereum::{handle_download_request, verify_download_chunk};
use crate::models::DownloadChunkPayload;
use axum::{
    extract::{ConnectInfo, Path, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
    Router,
};
use axum_server::tls_rustls::RustlsConfig;
use base64::{engine::general_purpose, Engine as _};
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use tower_http::cors::{Any, CorsLayer};
use tower_http::limit::RequestBodyLimitLayer;

pub async fn run_http_server(
    app: Arc<App>,
    addr_str: &str,
    cert_path: std::path::PathBuf,
    key_path: std::path::PathBuf,
) {
    // 1. Cấu hình CORS
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    // 2. Định nghĩa Router
    let router = Router::new()
        .route("/download/:download_key/:chunk_index", get(download_chunk_handler))
        .layer(cors)
        .layer(RequestBodyLimitLayer::new(1024 * 1024 * 1)) // Giới hạn body 1MB
        .with_state(app);

    let addr: SocketAddr = addr_str.parse().expect("Invalid HTTP_ADDR");

    // 3. Load TLS config
    match RustlsConfig::from_pem_file(&cert_path, &key_path).await {
        Ok(tls_config) => {
            log::info!("🚀 Starting HTTPS Server on https://{}", addr);
            if let Err(e) = axum_server::bind_rustls(addr, tls_config)
                .serve(router.into_make_service_with_connect_info::<SocketAddr>())
                .await
            {
                log::error!("HTTPS Server error: {}", e);
            }
        }
        Err(e) => {
            log::error!("Failed to load TLS config for HTTPS: {}. Fallback to HTTP.", e);
            log::info!("🚀 Starting HTTP Server on http://{}", addr);
            let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
            if let Err(e) = axum::serve(
                listener,
                router.into_make_service_with_connect_info::<SocketAddr>(),
            )
            .await
            {
                log::error!("HTTP Server error: {}", e);
            }
        }
    }
}

async fn download_chunk_handler(
    State(app): State<Arc<App>>,
    Path((download_key, chunk_index)): Path<(String, u64)>,
    headers: HeaderMap,
    ConnectInfo(peer_addr): ConnectInfo<SocketAddr>,
) -> Response {
    // Lấy IP của Client (Hỗ trợ proxy X-Forwarded-For)
    let request_ip = headers
        .get("x-forwarded-for")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.split(',').next())
        .and_then(|s| s.trim().parse::<IpAddr>().ok())
        .unwrap_or(peer_addr.ip());

    // Lấy signature từ header
    let signature = match headers.get("x-signature").and_then(|v| v.to_str().ok()) {
        Some(sig) => sig.to_string(),
        None => return (StatusCode::UNAUTHORIZED, "Missing X-Signature header").into_response(),
    };

    let mut payload = DownloadChunkPayload {
        file_key: "".to_string(), // Tạm thời rỗng, sẽ cập nhật sau khi verify
        download_key: download_key.clone(),
        chunk_index,
        signature,
    };

    // Verify chunk (hàm này sẽ nạp session vào cache nếu chưa có)
    match verify_download_chunk(&payload, &app, request_ip).await {
        Ok(true) => {}
        Ok(false) => return (StatusCode::UNAUTHORIZED, "Verification failed").into_response(),
        Err(e) => return (StatusCode::FORBIDDEN, e).into_response(),
    }

    // [QUAN TRỌNG] Phải lấy file_key từ session cache để ghi vào payload.
    // Nếu để rỗng, hàm handle_download_request sẽ panic khi slice chuỗi &payload.file_key[0..2]
    if let Some(session) = app.download_cache.get(&download_key) {
        payload.file_key = session.file_key.clone();
    } else {
        return (StatusCode::INTERNAL_SERVER_ERROR, "Session not found after verify").into_response();
    }

    // Đọc data
    let response = handle_download_request(&payload, &app).await;
    match response.status.as_str() {
        "SUCCESS" => {
            let chunk_data = general_purpose::STANDARD
                .decode(response.chunk_data_base64.unwrap_or_default())
                .unwrap_or_default();

            // Cập nhật số chunk (giống hệt QUIC server)
            let _ = crate::download_manager::descrease_chunk_count(&download_key, &app).await;

            (
                StatusCode::OK,
                [
                    ("Content-Type", "application/octet-stream"),
                    ("Cache-Control", "no-store"),
                ],
                chunk_data,
            )
                .into_response()
        }
        _ => (StatusCode::INTERNAL_SERVER_ERROR, response.message).into_response(),
    }
}
