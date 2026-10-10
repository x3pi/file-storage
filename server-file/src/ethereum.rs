use crate::app::App;
use crate::download_manager;
use crate::models::{CHUNK_SIZE, DownloadChunkPayload, GenericResponse, UploadChunkPayload, UploadFileInfo};
use alloy::primitives::{keccak256, Address};
use alloy::signers::Signature;
use std::net::IpAddr;

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
    let c_addr = payload.contract_address.parse::<alloy::primitives::Address>()
        .map_err(|e| format!("Invalid contract_address: {}", e))?;

    // 0. Chuẩn hóa & kiểm tra tính hợp lệ của file_key (phải là 64 ký tự hex thường, không có 0x)
    let clean_file_key = payload.file_key.trim_start_matches("0x");
    let key_bytes = match hex::decode(clean_file_key) {
        Ok(b) if b.len() == 32 => {
            let mut arr = [0u8; 32];
            arr.copy_from_slice(&b);
            arr
        }
        Ok(b) => return Err(format!("Invalid file_key length: expected 32 bytes (64 hex chars), got {}", b.len())),
        Err(e) => return Err(format!("Invalid file_key hex: {}", e)),
    };

    let canonical_file_key = hex::encode(&key_bytes);
    if clean_file_key != canonical_file_key {
        return Err(format!(
            "File key format mismatch: '{}' must match on-chain canonical hex (lowercase 64 chars)",
            clean_file_key
        ));
    }

    // 1. Validate chunk size (<= 1MB limit and non-empty)
    if chunk_data.is_empty() {
        return Err("Chunk data cannot be empty".to_string());
    }
    if chunk_data.len() > CHUNK_SIZE as usize {
        return Err(format!(
            "Chunk size exceeds 1MB limit: got {} bytes, maximum allowed is {}",
            chunk_data.len(),
            CHUNK_SIZE
        ));
    }

    let clean_merkle_root = payload.merkle_root.trim_start_matches("0x");

    // 2. Check cache first
    let total_chunks = if let Some(cached_info) = app.upload_file_cache.get(&canonical_file_key) {
        if cached_info.contract_address != c_addr {
            return Err(format!(
                "File key {} belongs to contract {}, but request specified {}",
                canonical_file_key, cached_info.contract_address, c_addr
            ));
        }
        if payload.chunk_index >= cached_info.total_chunks {
            return Err(format!(
                "Chunk index {} exceeds total_chunks {}",
                payload.chunk_index, cached_info.total_chunks
            ));
        }
        if cached_info.signature != payload.signature {
            return Err("Signature mismatch with cached signature".to_string());
        }
        if cached_info.merkle_root != clean_merkle_root {
            return Err(format!(
                "Merkle root mismatch: expected {}, got {}",
                cached_info.merkle_root, clean_merkle_root
            ));
        }
        cached_info.total_chunks
    } else {
        let lock_key = format!("upload_{}", canonical_file_key);
        let lock_arc = {
            let entry = app
                .init_locks
                .entry(lock_key.clone())
                .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())));
            Arc::clone(entry.value())
        };
        let _lock = lock_arc.lock().await;

        struct LockGuard<'b> {
            locks: &'b dashmap::DashMap<String, Arc<tokio::sync::Mutex<()>>>,
            key: &'b str,
        }
        impl<'b> Drop for LockGuard<'b> {
            fn drop(&mut self) {
                self.locks.remove(self.key);
            }
        }
        let _cleanup_guard = LockGuard {
            locks: &app.init_locks,
            key: &lock_key,
        };

        // Double check cache
        if let Some(cached_info) = app.upload_file_cache.get(&canonical_file_key) {
            if cached_info.contract_address != c_addr {
                return Err(format!(
                    "File key {} belongs to contract {}, but request specified {}",
                    canonical_file_key, cached_info.contract_address, c_addr
                ));
            }
            if payload.chunk_index >= cached_info.total_chunks {
                return Err(format!(
                    "Chunk index {} exceeds total_chunks {}",
                    payload.chunk_index, cached_info.total_chunks
                ));
            }
            if cached_info.signature != payload.signature {
                return Err("Signature mismatch with cached signature".to_string());
            }
            if cached_info.merkle_root != clean_merkle_root {
                return Err(format!(
                    "Merkle root mismatch: expected {}, got {}",
                    cached_info.merkle_root, clean_merkle_root
                ));
            }
            cached_info.total_chunks
        } else {
            // SC validation
            if !app.is_valid_contract(c_addr).await {
                return Err(format!("Contract {} is not registered or invalid", c_addr));
            }

            let contract = app
                .contract(c_addr)
                .await.map_err(|e| format!("Contract err: {}", e))?;
            
            let result = contract.getFileInfo(alloy::primitives::B256::from(key_bytes)).call().await
                .map_err(|e| format!("getFileInfo failed: {}", e))?;

            // Kiểm tra file thực sự tồn tại trên SC
            if result.owner == Address::ZERO {
                return Err(format!("File key '{}' does not exist on contract {}", canonical_file_key, c_addr));
            }

            // Kiểm tra trạng thái và hạn dùng của file trên chain
            let current_time_secs = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_err(|e| format!("System time error: {}", e))?
                .as_secs();

            if result.expireTime > 0 && result.expireTime <= current_time_secs {
                return Err("File has expired on-chain".to_string());
            }

            use crate::contracts::file_contract::Files::FileStatus;
            if result.status == FileStatus::Deleted {
                return Err("File has been deleted on-chain".to_string());
            }

            if result.status != FileStatus::Processing {
                return Err(format!("File status is not Processing: {:?}", result.status));
            }

            // Kiểm tra merkle_root gửi lên phải khớp với on-chain
            let onchain_merkle_root = hex::encode(result.merkleRoot);
            if onchain_merkle_root != clean_merkle_root {
                return Err(format!(
                    "Merkle root mismatch with contract: expected {}, got {}",
                    onchain_merkle_root, clean_merkle_root
                ));
            }
                
            let file_owner = result.owner;
            let total_chunks = result.totalChunks;

            if payload.chunk_index >= total_chunks {
                return Err(format!(
                    "Chunk index {} exceeds total_chunks {}",
                    payload.chunk_index, total_chunks
                ));
            }

            // 2. First chunk for this file - verify signature with canonical_file_key (Owner authorization)
            let recovered_address: Address =
                run_recover_address_blocking(canonical_file_key.clone(), payload.signature.clone()).await?;
            
            if recovered_address != file_owner {
                log::error!(
                    "❌ Upload Rejected: Signer is {:?}, expected File Owner {:?}",
                    recovered_address, file_owner
                );
                return Err("Signer is not the file owner".to_string());
            }

            // 3. Cache both address, signature and merkle root
            app.upload_file_cache.insert(
                canonical_file_key.clone(),
                UploadFileInfo {
                    contract_address: c_addr,
                    signature: payload.signature.clone(),
                    merkle_root: clean_merkle_root.to_string(),
                    total_chunks,
                    created_at: std::time::Instant::now(),
                },
            );

            total_chunks
        }
    };

    // 3. Verify chunk index boundary
    if payload.chunk_index >= total_chunks {
        return Err(format!(
            "Invalid chunk_index {}: exceeds total_chunks {}",
            payload.chunk_index, total_chunks
        ));
    }

    // 4. Verify Merkle proof depth to prevent second-preimage attack (dùng hàm utils dùng chung)
    let expected_depth = crate::utils::merkle_tree_depth(total_chunks);

    if payload.merkle_proof_hashes.len() != expected_depth {
        return Err(format!(
            "Invalid Merkle proof depth: expected {} siblings for total_chunks {}, got {}",
            expected_depth, total_chunks, payload.merkle_proof_hashes.len()
        ));
    }
    
    // 4. Verify Merkle Proof (ALWAYS - even for cached files)
    let chunk_data_owned = chunk_data.to_vec();
    let merkle_proof_hashes = payload.merkle_proof_hashes.clone();
    let chunk_index = payload.chunk_index;
    let merkle_root_expected = payload.merkle_root.clone();
    let file_key = payload.file_key.clone();

    tokio::task::spawn_blocking(move || {
        // Compute leaf hash from chunk data
        let leaf_hash = keccak256(&chunk_data_owned);
        let mut computed_hash = leaf_hash.to_vec();

        // Iterate through proof levels
        for (level, sibling_hex) in merkle_proof_hashes.iter().enumerate() {
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
            let level_index = chunk_index >> level;
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
        let expected_root = hex::decode(merkle_root_expected.trim_start_matches("0x"))
            .map_err(|e| format!("Invalid merkle root: {}", e))?;

        if computed_hash != expected_root {
            log::error!(
                "❌ INVALID Merkle Proof for chunk {} -k {}. Computed: {}, Expected: {}",
                chunk_index,
                file_key,
                hex::encode(&computed_hash),
                merkle_root_expected
            );
            return Err("Merkle proof verification failed".to_string());
        }
        
        Ok::<(), String>(())
    })
    .await
    .map_err(|e| format!("Task join error: {}", e))??;

    Ok(())
}

pub async fn verify_download_chunk(
    payload: &DownloadChunkPayload,
    app: &Arc<App>,
    request_ip: IpAddr,
) -> Result<bool, String> {
    let c_addr = payload.contract_address.parse::<alloy::primitives::Address>()
        .map_err(|e| format!("Invalid contract_address: {}", e))?;
    let session_ref =
        download_manager::initialize_download_session(&payload.download_key, c_addr, app, request_ip)
            .await?;
    let session_first_ip_arc = session_ref.first_ip.clone();
    let session_owner = session_ref.file_owner;
    let session_user = session_ref.session_user;
    let total_chunks = session_ref.total_chunks;
    let signature_cache = session_ref.verified_signature.clone();
    let is_public = session_ref.is_public;
    let whitelist = session_ref.whitelist.clone();
    drop(session_ref);

    // 1. Kiểm tra giới hạn chunk_index so với total_chunks
    if payload.chunk_index >= total_chunks {
        return Err(format!(
            "Invalid chunk_index {}: exceeds total_chunks {}",
            payload.chunk_index, total_chunks
        ));
    }

    // 2. Xác thực chữ ký
    let mut signature_valid = false;
    if is_public {
        log::debug!("File is public download for key: {}", payload.download_key);
        signature_valid = true;
    } else {
        // Fast path: Kiểm tra cache chữ ký
        let cache_guard = signature_cache.lock().await;
        if let Some(cached_sig) = cache_guard.as_ref() {
            if *cached_sig == payload.signature {
                signature_valid = true;
            }
        }
        drop(cache_guard);

        if !signature_valid {
            match run_recover_address_blocking(payload.download_key.clone(), payload.signature.clone()).await {
                Ok(recovered_address) => {
                    let is_authorized = if is_public {
                        recovered_address == session_owner || recovered_address == session_user || whitelist.contains(&recovered_address)
                    } else {
                        // File private: Người ký PHẢI là owner hoặc trong whitelist, VÀ phải là session_user hoặc session_owner
                        (recovered_address == session_owner || whitelist.contains(&recovered_address))
                            && (recovered_address == session_user || recovered_address == session_owner)
                    };

                    if is_authorized {
                        log::info!(
                            "✅ Download authorized: Signer address {:?} verified for download key {}",
                            recovered_address, payload.download_key
                        );
                        let mut cache_guard = signature_cache.lock().await;
                        *cache_guard = Some(payload.signature.clone());
                        signature_valid = true;
                    } else {
                        log::warn!(
                            "❌ Signature mismatch / unauthorized for download key: {}. Recovered signer: {:?}, Expected owner: {:?} or session user: {:?}",
                            payload.download_key,
                            recovered_address,
                            session_owner,
                            session_user
                        );
                        return Err(format!(
                            "Signer {:?} not authorized for download. Expected owner {:?} or authorized session user {:?}",
                            recovered_address, session_owner, session_user
                        ));
                    }
                }
                Err(e) => {
                    log::warn!(
                        "❌ Signature recovery failed for download key: {}. Error: '{}'. Expected signer: owner {:?} or session user: {:?}",
                        payload.download_key,
                        e,
                        session_owner,
                        session_user
                    );
                    return Err(format!("Signature recovery failed: {}", e));
                }
            }
        }
    }

    if !signature_valid {
        return Err("Signature verification failed".to_string());
    }

    // 3. CHỈ KHOÁ/CHECK IP SAU KHI ĐÃ VERIFY CHỮ KÝ THÀNH CÔNG!
    // Sử dụng OnceLock: lock-free, atomic, không mutex, không lo deadlock!
    if !is_public {
        let cached_ip = session_first_ip_arc.get_or_init(|| request_ip);
        if *cached_ip != request_ip {
            return Err(format!(
                "IP address mismatch: session bound to {}, request from {}",
                cached_ip, request_ip
            ));
        }
    }

    Ok(true)
}

pub async fn handle_download_request(
    payload: &DownloadChunkPayload,
    app: &Arc<App>,
) -> (GenericResponse, Option<Vec<u8>>) {
    // Initialize download session and check permissions (scope limits lock lifetime)
    let (has_permission, file_handle, is_available) = {
        let session = match app.download_cache.get(&payload.download_key) {
            Some(s) => s,
            None => {
                return (GenericResponse {
                    status: "ERROR".to_string(),
                    message: "Download session not found, please verify first.".to_string(),
                }, None);
            }
        };
        (
            session.remaining_chunks > 0 || session.retry_remaining > 0,
            session.file_handle.clone(),
            crate::utils::is_chunk_in_set(&session.available_chunks, payload.chunk_index),
        )
    };

    // Check permission
    if !has_permission {
        return (GenericResponse {
            status: "ERROR".to_string(),
            message: "No remaining downloads for this key".to_string(),
        }, None);
    }

    // Bảo vệ file thưa (sparse file): chỉ phục vụ chunk thực sự có trong .meta, từ chối chunk của node khác
    if !is_available {
        return (GenericResponse {
            status: "ERROR".to_string(),
            message: format!("Chunk {} is not stored on this node", payload.chunk_index),
        }, None);
    }

    let offset = (payload.chunk_index as u64) * CHUNK_SIZE;

    let chunk_data_result: Result<Vec<u8>, std::io::Error> = tokio::task::spawn_blocking(move || {
        use std::os::unix::fs::FileExt;

        let mut buf = vec![0u8; CHUNK_SIZE as usize];
        let n = file_handle.read_at(&mut buf, offset)?;
        if n == 0 {
            return Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "Chunk out of bounds or empty"));
        }
        buf.truncate(n);
        
        Ok(buf)
    }).await.unwrap_or_else(|e| Err(std::io::Error::new(std::io::ErrorKind::Other, e)));

    let chunk_data = match chunk_data_result {
        Ok(data) => data,
        Err(_) => {
            return (GenericResponse {
                status: "ERROR".to_string(),
                message: "Chunk data not found".to_string(),
            }, None);
        }
    };

    (GenericResponse {
        status: "SUCCESS".to_string(),
        message: String::new(),
    }, Some(chunk_data))
}

pub async fn confirm_upload_batch(app: Arc<App>, contract_address: alloy::primitives::Address, file_keys: Vec<String>) -> Result<(), String> {
    if file_keys.is_empty() {
        return Ok(());
    }

    let contract = app.contract_with_signer(contract_address).await.map_err(|e| e.to_string())?;

    let mut keys = Vec::new();
    for key_hex in file_keys {
        let decoded = hex::decode(key_hex.trim_start_matches("0x"))
            .unwrap_or_default();
        if decoded.len() == 32 {
            let mut byte_array = [0u8; 32];
            byte_array.copy_from_slice(&decoded);
            keys.push(alloy::primitives::B256::from(byte_array));
        } else {
            log::error!("❌ Corrupted file_key in batch confirmation: '{}' (len: {})", key_hex, decoded.len());
        }
    }

    if keys.is_empty() {
        return Ok(());
    }
    
    let pending_tx = contract.confirmServerUploadBatch(keys).send().await
        .map_err(|e| format!("Failed to send tx: {}", e))?;

    let receipt = tokio::time::timeout(
        tokio::time::Duration::from_secs(60),
        pending_tx.get_receipt()
    ).await
    .map_err(|_| "Timeout waiting for confirmServerUploadBatch receipt (60s)".to_string())?
    .map_err(|e| format!("Failed to get receipt: {}", e))?;

    if receipt.status() {
        log::info!("✅ confirmServerUploadBatch success! TxHash: {}", receipt.transaction_hash);
        Ok(())
    } else {
        log::error!("❌ confirmServerUploadBatch reverted on-chain! TxHash: {}", receipt.transaction_hash);
        Err(format!("confirmServerUploadBatch reverted on-chain! TxHash: {}", receipt.transaction_hash))
    }
}

pub async fn handle_confirm_download(
    download_key: String,
    contract_address: String,
    app: std::sync::Arc<crate::app::App>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let download_key_clean = download_key.trim_start_matches("0x");
    let download_key_bytes = hex::decode(download_key_clean)?;
    let download_key_b256 = alloy::primitives::B256::from_slice(&download_key_bytes);
    let c_addr = contract_address.parse::<alloy::primitives::Address>()?;
    let contract = app.contract_with_signer(c_addr).await?;
    let pending_tx = contract
        .confirmServerDownload(download_key_b256)
        .send()
        .await?;

    let receipt = tokio::time::timeout(
        tokio::time::Duration::from_secs(60),
        pending_tx.get_receipt()
    ).await
    .map_err(|_| "Timeout waiting for confirmServerDownload receipt (60s)")??;
    
    if receipt.status() {
        log::info!("✅ confirmServerDownload success! TxHash: {}", receipt.transaction_hash);
        Ok(())
    } else {
        log::error!("❌ confirmServerDownload reverted on-chain! TxHash: {}", receipt.transaction_hash);
        Err(format!("confirmServerDownload reverted on-chain! TxHash: {}", receipt.transaction_hash).into())
    }
}
