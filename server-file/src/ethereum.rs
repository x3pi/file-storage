use crate::app::App;
use crate::download_manager;
use crate::models::{DownloadChunkPayload, DownloadResponse, UploadChunkPayload};
use alloy::primitives::{keccak256, Address};

use alloy::signers::Signature;
use base64::{engine::general_purpose, Engine as _};
use std::fs;
use std::net::IpAddr;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
// FIX: Dùng tokio::time::sleep thay vì std::thread::sleep (đã được khôi phục)

// 🔥 FIX: Wrapper task bất đồng bộ cho tác vụ nặng CPU
async fn run_recover_address_blocking(
    download_key: String,
    signature_str: String,
) -> Result<Address, String> {
    tokio::task::spawn_blocking(move || {
        recover_address_from_signature(&download_key, &signature_str)
    })
    .await
    .map_err(|e| format!("Task join failed for signature recovery: {}", e))?
}

// Helper function để recover address (tác vụ nặng CPU, sẽ được gọi trong spawn_blocking)
fn recover_address_from_signature(
    download_key: &str,
    signature_str: &str,
) -> Result<Address, String> {
    // Message chính là download_key (hex string KHÔNG có prefix 0x)
    let message_str = download_key.trim_start_matches("0x");

    // Decode signature
    let signature_bytes = hex::decode(signature_str.trim_start_matches("0x"))
        .map_err(|e| format!("Invalid signature: {}", e))?;

    if signature_bytes.len() != 65 {
        return Err(format!(
            "Invalid signature length: expected 65, got {}",
            signature_bytes.len()
        ));
    }

    let signature = Signature::try_from(signature_bytes.as_slice())
        .map_err(|e| format!("Invalid signature format: {:?}", e))?;

    let prefix = format!("\x19Ethereum Signed Message:\n{}", message_str.len());
    let full_message = format!("{}{}", prefix, message_str);
    let message_hash = keccak256(full_message.as_bytes());

    // Recover address
    let recovered_address = signature
        .recover_address_from_prehash(&message_hash)
        .map_err(|e| format!("Recover address failed: {}", e))?;

    Ok(recovered_address)
}
pub async fn verify_upload_chunk(
    payload: &UploadChunkPayload,
    app: &Arc<App>,
) -> Result<bool, String> {
    let admin_uploader_address = Address::from_str(app.config.address_sign_admin.as_str())
        .map_err(|_| "Invalid Admin Uploader Address".to_string())?;
    if let Some(cached_addr) = app.verified_upload_cache.get(&payload.file_key) {
        if *cached_addr == admin_uploader_address {
            return Ok(true);
        } else {
            return Err("Uploader is not validator".to_string());
        }
    }
    let recovered_address: Address =
        run_recover_address_blocking(payload.file_key.clone(), payload.signature.clone()).await?;
    if recovered_address != admin_uploader_address {
        println!(
            "❌ Upload Rejected: Signer is {:?}, expected Admin {:?}",
            recovered_address, admin_uploader_address
        );
        return Err("Signer is not the authorized admin uploader".to_string());
    }
    app.verified_upload_cache
        .insert(payload.file_key.clone(), recovered_address);
    Ok(true)
}

pub async fn verify_download_chunk(
    payload: &DownloadChunkPayload,
    app: &Arc<App>,
    request_ip: IpAddr,
) -> Result<bool, String> {
    let session_ref =
        download_manager::initialize_download_session(&payload.download_key, app, request_ip)
            .await?;
    let session_first_ip = session_ref.first_ip;
    let session_owner = session_ref.file_owner;
    let signature_cache = session_ref.verified_signature.clone();
    drop(session_ref);

    if session_first_ip != request_ip {
        return Err("IP address mismatch".to_string());
    }
    // 1. Lấy cache chữ ký của session
    let cache_guard = signature_cache.lock().await;
    if let Some(cached_sig) = cache_guard.as_ref() {
        // Cache hit - verify với owner đã cache
        if *cached_sig == payload.signature {
            return Ok(true);
        } else {
            log::warn!(
                "Signature mismatch for {}: expected (cached) {}, got {}",
                payload.download_key,
                cached_sig,
                payload.signature
            );
            return Err("Invalid signature (mismatch with cached)".to_string());
        }
    }
    // 3. (CACHE MISS) - 'cache_guard' vẫn đang giữ lock, và cache đang là None
    // Chúng ta phải thả lock để chạy hàm blocking
    drop(cache_guard);
    let recovered_address =
        run_recover_address_blocking(payload.download_key.clone(), payload.signature.clone())
            .await?;

    // Verify owner
    if recovered_address != session_owner {
        println!(
            "❌ Not match file owner: {}, recovered address: {}",
            session_owner, recovered_address
        );
        return Err("Signer address does not match file owner".to_string());
    }

    let mut cache_guard = signature_cache.lock().await;
    if cache_guard.is_none() {
        *cache_guard = Some(payload.signature.clone());
    }
    Ok(true)
}

pub async fn handle_download_request(
    payload: &DownloadChunkPayload,
    app: &Arc<App>,
) -> DownloadResponse {
    let level1 = &payload.file_key[0..2];
    let level2 = &payload.file_key[2..4];
    let chunk_path = app
        .storage_root
        .join(level1) // Cấp 1
        .join(level2) // Cấp 2
        .join(&payload.file_key)
        .join(payload.chunk_index.to_string());
    // Initialize download session
    let session = match app.download_cache.get(&payload.download_key) {
        Some(s) => s,
        None => {
            return DownloadResponse {
                status: "ERROR".to_string(),
                message: "Download session not found, please verify first.".to_string(),
                chunk_data_base64: None,
            };
        }
    };

    // Check permission
    if session.remaining_chunks == 0 && session.retry_remaining == 0 {
        return DownloadResponse {
            status: "ERROR".to_string(),
            message: "No remaining downloads for this key".to_string(),
            chunk_data_base64: None,
        };
    }
    // Loại bỏ sleep và retry như yêu cầu
    // let chunk_path_clone: PathBuf = chunk_path.clone();
    // let read_result = tokio::task::spawn_blocking(move || fs::read(&chunk_path_clone)).await;
    let chunk_data = match fs::read(&chunk_path) {
        Ok(data) => data,
        Err(e) => {
            println!("❌ Failed to read chunk: {}", e);
            return DownloadResponse {
                status: "ERROR".to_string(),
                message: format!("Failed to read chunk: {}", e),
                chunk_data_base64: None,
            };
        }
    };

    DownloadResponse {
        status: "SUCCESS".to_string(),
        message: "Chunk retrieved successfully".to_string(),
        chunk_data_base64: Some(general_purpose::STANDARD.encode(chunk_data)),
    }
}
