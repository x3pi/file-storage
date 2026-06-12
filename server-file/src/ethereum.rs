use crate::app::App;
use crate::download_manager;
use crate::models::{DownloadChunkPayload, DownloadResponse, UploadChunkPayload, UploadFileInfo};
use alloy::primitives::{keccak256, Address};

use alloy::signers::Signature;
use base64::{engine::general_purpose, Engine as _};
use tokio::fs;
use std::net::IpAddr;
use std::str::FromStr;
use std::sync::Arc;

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

    let prefix = format!("0x00");
    let full_message = format!("{}{}", prefix, message_str);
    let message_hash = keccak256(full_message.as_bytes());

    // Recover address
    let recovered_address = signature
        .recover_address_from_prehash(&message_hash)
        .map_err(|e| format!("Recover address failed: {}", e))?;

    Ok(recovered_address)
}
/// Verify upload chunk signature and merkle proof
/// Combines both signature verification and merkle proof verification
pub async fn verify_upload_chunk(
    payload: &UploadChunkPayload,
    chunk_data: &[u8],
    app: &Arc<App>,
) -> Result<(), String> {
    let admin_uploader_address = Address::from_str(app.config.address_sign_admin.as_str())
        .map_err(|_| "Invalid Admin Uploader Address".to_string())?;
    
    // 1. Check cache first
    if let Some(cached_info) = app.upload_file_cache.get(&payload.file_key) {
        // Verify cached address matches admin
        if cached_info.verified_address != admin_uploader_address {
            return Err("Uploader is not validator".to_string());
        }
        
        // Verify merkle root matches cached value
        if cached_info.merkle_root != payload.merkle_root {
            return Err(format!(
                "Merkle root mismatch: expected {}, got {}",
                cached_info.merkle_root, payload.merkle_root
            ));
        }
        
        // Cache hit - skip signature verification, proceed to merkle proof
    } else {
        // 2. First chunk for this file - verify signature with fileKey + merkleRoot
        let message_to_sign = format!("{}{}", payload.file_key, payload.merkle_root);
        let recovered_address: Address =
            run_recover_address_blocking(message_to_sign, payload.signature.clone()).await?;
        
        if recovered_address != admin_uploader_address {
            log::error!(
                "❌ Upload Rejected: Signer is {:?}, expected Admin {:?}",
                recovered_address, admin_uploader_address
            );
            return Err("Signer is not the authorized admin uploader".to_string());
        }
        
        // 3. Cache both address and merkle root
        app.upload_file_cache.insert(
            payload.file_key.clone(),
            UploadFileInfo {
                verified_address: recovered_address,
                merkle_root: payload.merkle_root.clone(),
            },
        );
    }
    
    // 4. Verify Merkle Proof (ALWAYS - even for cached files)
    // Compute leaf hash from chunk data
    let leaf_hash = keccak256(chunk_data);
    let mut computed_hash = leaf_hash.to_vec();

    // Iterate through proof levels
    for (level, sibling_hex) in payload.merkle_proof_hashes.iter().enumerate() {
        // Decode sibling hash from hex
        let sibling_hash = hex::decode(sibling_hex.trim_start_matches("0x"))
            .map_err(|e| format!("Invalid sibling hash at level {}: {}", level, e))?;

        if sibling_hash.len() != 32 {
            return Err(format!(
                "Invalid sibling hash length at level {}: expected 32, got {}",
                level,
                sibling_hash.len()
            ));
        }

        // Determine position in tree
        let level_index = payload.chunk_index >> level;
        let combined = if level_index % 2 == 0 {
            // Current hash is on the left
            [computed_hash.as_slice(), sibling_hash.as_slice()].concat()
        } else {
            // Current hash is on the right
            [sibling_hash.as_slice(), computed_hash.as_slice()].concat()
        };

        // Hash the combined value
        computed_hash = keccak256(&combined).to_vec();
    }

    // Compare with expected merkle root
    let expected_root = hex::decode(payload.merkle_root.trim_start_matches("0x"))
        .map_err(|e| format!("Invalid merkle root: {}", e))?;

    if computed_hash != expected_root {
        log::error!(
            "❌ INVALID Merkle Proof for chunk {} -k {}. Computed: {}, Expected: {}",
            payload.chunk_index,
            payload.file_key,
            hex::encode(&computed_hash),
            payload.merkle_root
        );
        return Err("Merkle proof verification failed".to_string());
    }
    
    Ok(())
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
    let is_public = session_ref.is_public;
    let whitelist = session_ref.whitelist.clone();
    drop(session_ref);

    if session_first_ip != request_ip {
        return Err("IP address mismatch".to_string());
    }
    
    // Nếu file là public, cho phép tải ngay
    if is_public {
        log::info!("File is public, allowing download without signature verification");
        return Ok(true);
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

    // Verify owner hoặc kiểm tra whitelist
    if recovered_address != session_owner {
        // Nếu không phải owner, kiểm tra xem có trong whitelist không
        if whitelist.contains(&recovered_address) {
            log::info!(
                "✅ Download allowed: Address {:?} is in whitelist for file",
                recovered_address
            );
        } else {
            log::error!(
                "❌ Not match file owner: {}, recovered address: {}, and not in whitelist",
                session_owner, recovered_address
            );
            return Err("Signer address does not match file owner and is not in whitelist".to_string());
        }
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
    // Initialize download session and check permissions (scope limits lock lifetime)
    let has_permission = {
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
        session.remaining_chunks > 0 || session.retry_remaining > 0
    };

    // Check permission
    if !has_permission {
        return DownloadResponse {
            status: "ERROR".to_string(),
            message: "No remaining downloads for this key".to_string(),
            chunk_data_base64: None,
        };
    }
    // Loại bỏ sleep và retry như yêu cầu
    // let chunk_path_clone: PathBuf = chunk_path.clone();
    // let read_result = tokio::task::spawn_blocking(move || fs::read(&chunk_path_clone)).await;
    let chunk_data = match fs::read(&chunk_path).await {
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
