use crate::app::App;
use crate::download_manager;
use crate::models::{CHUNK_SIZE, DownloadChunkPayload, DownloadResponse, UploadChunkPayload, UploadFileInfo};
use alloy::primitives::{keccak256, Address};
use alloy::signers::Signature;
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
        if cached_info.verified_address != admin_uploader_address {
            return Err("Uploader is not validator".to_string());
        }
        if cached_info.merkle_root != payload.merkle_root {
            return Err(format!(
                "Merkle root mismatch: expected {}, got {}",
                cached_info.merkle_root, payload.merkle_root
            ));
        }
    } else {
        let lock_key = format!("upload_{}", payload.file_key);
        let lock_arc = {
            let entry = app
                .init_locks
                .entry(lock_key.clone())
                .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())));
            Arc::clone(entry.value())
        };
        let _lock = lock_arc.lock().await;

        // Double check cache
        if let Some(cached_info) = app.upload_file_cache.get(&payload.file_key) {
            if cached_info.verified_address != admin_uploader_address {
                return Err("Uploader is not validator".to_string());
            }
            if cached_info.merkle_root != payload.merkle_root {
                return Err(format!(
                    "Merkle root mismatch: expected {}, got {}",
                    cached_info.merkle_root, payload.merkle_root
                ));
            }
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

            // Fetch total_chunks from SC
            let contract = app.contract().await.map_err(|e| format!("Contract err: {}", e))?;
            let decoded = hex::decode(payload.file_key.trim_start_matches("0x"))
                .map_err(|e| format!("Invalid file_key hex: {}", e))?;
            let mut key_bytes = [0u8; 32];
            if decoded.len() == 32 {
                key_bytes.copy_from_slice(&decoded);
            } else {
                return Err("Invalid file_key length".to_string());
            }
            
            let result = contract.getFileInfo(alloy::primitives::B256::from(key_bytes)).call().await
                .map_err(|e| format!("getFileInfo failed: {}", e))?;
                
            let total_chunks = result.totalChunks;
            
            // 3. Cache both address and merkle root
            app.upload_file_cache.insert(
                payload.file_key.clone(),
                UploadFileInfo {
                    verified_address: recovered_address,
                    merkle_root: payload.merkle_root.clone(),
                    total_chunks,
                },
            );
        }
        app.init_locks.remove(&lock_key);
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
) -> (DownloadResponse, Option<Vec<u8>>) {
    // Initialize download session and check permissions (scope limits lock lifetime)
    let (has_permission, file_handle) = {
        let session = match app.download_cache.get(&payload.download_key) {
            Some(s) => s,
            None => {
                return (DownloadResponse {
                    status: "ERROR".to_string(),
                    message: "Download session not found, please verify first.".to_string(),
                }, None);
            }
        };
        (session.remaining_chunks > 0 || session.retry_remaining > 0, session.file_handle.clone())
    };

    // Check permission
    if !has_permission {
        return (DownloadResponse {
            status: "ERROR".to_string(),
            message: "No remaining downloads for this key".to_string(),
        }, None);
    }

    let offset = (payload.chunk_index as u64) * CHUNK_SIZE;

    let chunk_data_result: Result<Vec<u8>, std::io::Error> = tokio::task::spawn_blocking(move || {
        use std::os::unix::fs::FileExt;

        let mut buf = vec![0u8; CHUNK_SIZE as usize];
        let n = file_handle.read_at(&mut buf, offset)?;
        buf.truncate(n);
        
        Ok(buf)
    }).await.unwrap_or_else(|e| Err(std::io::Error::new(std::io::ErrorKind::Other, e)));

    let chunk_data = match chunk_data_result {
        Ok(data) => data,
        Err(_) => {
            return (DownloadResponse {
                status: "ERROR".to_string(),
                message: "Chunk data not found".to_string(),
            }, None);
        }
    };

    (DownloadResponse {
        status: "SUCCESS".to_string(),
        message: String::new(),
    }, Some(chunk_data))
}

pub async fn confirm_upload_batch(app: Arc<App>, file_keys: Vec<String>) -> Result<(), String> {
    if file_keys.is_empty() {
        return Ok(());
    }

    let contract = app.contract_with_signer().await.map_err(|e| e.to_string())?;

    let mut keys = Vec::new();
    for key_hex in file_keys {
        let decoded = hex::decode(key_hex.trim_start_matches("0x"))
            .unwrap_or_default();
        if decoded.len() == 32 {
            let mut byte_array = [0u8; 32];
            byte_array.copy_from_slice(&decoded);
            keys.push(alloy::primitives::B256::from(byte_array));
        }
    }

    if keys.is_empty() {
        return Ok(());
    }

    let pending_tx = contract.confirmServerUploadBatch(keys).send().await
        .map_err(|e| format!("Failed to send tx: {}", e))?;

    let receipt = pending_tx.get_receipt().await
        .map_err(|e| format!("Failed to get receipt: {}", e))?;

    if receipt.status() {
        log::info!("✅ confirmServerUploadBatch success! TxHash: {}", receipt.transaction_hash);
    } else {
        log::error!("❌ confirmServerUploadBatch failed! TxHash: {}", receipt.transaction_hash);
    }

    Ok(())
}

pub async fn handle_confirm_download(
    download_key: String,
    app: std::sync::Arc<crate::app::App>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let download_key_bytes = hex::decode(&download_key)?;
    let download_key_b256 = alloy::primitives::B256::from_slice(&download_key_bytes);
    let contract = app.contract_with_signer().await?;
    let pending_tx = contract
        .confirmServerDownload(download_key_b256)
        .send()
        .await?;

    // Đợi transaction được mine
    pending_tx.get_receipt().await?;
    Ok(())
}
