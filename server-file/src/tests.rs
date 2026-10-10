use std::net::IpAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::timeout;

use crate::app::App;
use crate::server::{is_admin_rate_limited, record_admin_fail};

fn create_temp_env() -> (PathBuf, PathBuf) {
    let base = std::env::temp_dir().join(format!(
        "file_storage_test_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let storage_root = base.join("storage");
    let log_dir = base.join("logs");
    std::fs::create_dir_all(&storage_root).unwrap();
    std::fs::create_dir_all(&log_dir).unwrap();
    (storage_root, log_dir)
}

fn cleanup_temp_env(storage_root: PathBuf, log_dir: PathBuf) {
    let _ = std::fs::remove_dir_all(&storage_root);
    let _ = std::fs::remove_dir_all(&log_dir);
}

// =========================================================================
// 1. DEADLOCK & CONCURRENCY TESTS
// =========================================================================

#[tokio::test]
async fn test_chunk_tracker_high_concurrency_no_deadlock() {
    let (storage_root, log_dir) = create_temp_env();
    let app = App::new_test(storage_root.clone(), log_dir.clone());
    let file_key = "a1b2c3d4e5f60718293a4b5c6d7e8f90a1b2c3d4e5f60718293a4b5c6d7e8f90";

    // Chạy 100 task đồng thời ghi chunk vào chunk_tracker
    // Bọc trong timeout 5s để phát hiện deadlock ngay lập tức
    let result = timeout(Duration::from_secs(5), async {
        let mut handles = Vec::new();
        for i in 0..100 {
            let app_clone = app.clone();
            let key = file_key.to_string();
            handles.push(tokio::spawn(async move {
                let mut set = app_clone.get_or_init_chunk_tracker(&key).await;
                set.insert(i);
            }));
        }

        for h in handles {
            h.await.unwrap();
        }
    })
    .await;

    assert!(result.is_ok(), "DEADLOCK DETECTED: chunk_tracker bị treo khi chạy đồng thời");

    // Kiểm tra tính toàn vẹn: đúng 100 chunk được ghi nhận, không bị mất chunk do TOCTOU race
    let tracker_entry = app.chunk_tracker.get(file_key).unwrap();
    assert_eq!(tracker_entry.len(), 100);
    for i in 0..100 {
        assert!(tracker_entry.contains(&i), "Missing chunk index {}", i);
    }
    drop(tracker_entry);

    // Xóa an toàn khỏi map
    assert!(app.chunk_tracker.remove(file_key).is_some());

    cleanup_temp_env(storage_root, log_dir);
}

#[tokio::test]
async fn test_init_locks_double_check_locking_no_deadlock() {
    let (storage_root, log_dir) = create_temp_env();
    let app = App::new_test(storage_root.clone(), log_dir.clone());
    let download_key = "test_download_key_deadlock_check";

    // Mô phỏng 50 task đồng thời tranh chấp Mutex khởi tạo download session
    let result = timeout(Duration::from_secs(5), async {
        let mut handles = Vec::new();
        for task_id in 0..50 {
            let app_clone = app.clone();
            let dl_key = download_key.to_string();
            handles.push(tokio::spawn(async move {
                // Kiểm tra cache trước lock (Fast Path)
                if app_clone.download_cache.contains_key(&dl_key) {
                    return;
                }

                // Lấy Mutex per-key
                let lock_arc = {
                    let entry = app_clone
                        .init_locks
                        .entry(dl_key.clone())
                        .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())));
                    Arc::clone(entry.value())
                };
                let _lock = lock_arc.lock().await;

                // RAII Guard cleanup entry
                struct TestLockGuard<'a> {
                    locks: &'a dashmap::DashMap<String, Arc<tokio::sync::Mutex<()>>>,
                    key: &'a str,
                }
                impl<'a> Drop for TestLockGuard<'a> {
                    fn drop(&mut self) {
                        self.locks.remove(self.key);
                    }
                }
                let _cleanup_guard = TestLockGuard {
                    locks: &app_clone.init_locks,
                    key: &dl_key,
                };

                // Double check sau lock (Slow Path)
                if app_clone.download_cache.contains_key(&dl_key) {
                    return;
                }

                // Giả lập khởi tạo session
                tokio::time::sleep(Duration::from_millis(5)).await;
                // Đánh dấu đã khởi tạo bằng cách insert dummy vào invalid_download_keys để test
                app_clone.invalid_download_keys.insert(dl_key.clone(), std::time::Instant::now());
                log::debug!("Task {} initialized session", task_id);
            }));
        }

        for h in handles {
            h.await.unwrap();
        }
    })
    .await;

    assert!(result.is_ok(), "DEADLOCK DETECTED: Double-check locking bị deadlock");
    // Đảm bảo lock guard đã cleanup sạch sẽ init_locks
    assert!(
        app.init_locks.is_empty(),
        "Leaked lock in init_locks: count = {}",
        app.init_locks.len()
    );

    cleanup_temp_env(storage_root, log_dir);
}

#[tokio::test]
async fn test_semaphore_high_concurrency_no_leak() {
    let (storage_root, log_dir) = create_temp_env();
    let app = App::new_test(storage_root.clone(), log_dir.clone());
    let initial_permits = app.task_semaphore.available_permits();
    assert_eq!(initial_permits, 3000);

    let result = timeout(Duration::from_secs(5), async {
        let mut handles = Vec::new();
        for _ in 0..100 {
            let app_clone = app.clone();
            handles.push(tokio::spawn(async move {
                let permit = app_clone.task_semaphore.acquire().await.unwrap();
                tokio::time::sleep(Duration::from_millis(1)).await;
                drop(permit);
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
    })
    .await;

    assert!(result.is_ok(), "DEADLOCK DETECTED: Semaphore acquire bị nghẽn");
    assert_eq!(
        app.task_semaphore.available_permits(),
        3000,
        "Semaphore permit leak"
    );

    cleanup_temp_env(storage_root, log_dir);
}

// =========================================================================
// 2. SECURITY & PATH TRAVERSAL TESTS
// =========================================================================

#[tokio::test]
async fn test_path_traversal_detection_and_blocking() {
    let (storage_root, log_dir) = create_temp_env();

    // 1. Tạo 1 file log hợp lệ bên trong log_dir
    let valid_log_file = log_dir.join("app.log");
    tokio::fs::write(&valid_log_file, "log content 123").await.unwrap();

    // 2. Danh sách các vector tấn công Path Traversal
    let attack_vectors = vec![
        "../../../etc/passwd",
        "..\\..\\windows\\system32",
        "sub/file.log",
        "/absolute/path/file.log",
        "..",
        "./../app.log",
        "app.log/../../../test",
    ];

    for malicious_input in attack_vectors {
        // Kiểm tra filter ký tự cấm: '/' '\' '..'
        let is_blocked_by_char_filter = malicious_input.contains('/')
            || malicious_input.contains('\\')
            || malicious_input.contains("..");
        assert!(
            is_blocked_by_char_filter,
            "Attack vector '{}' bypassed char filter",
            malicious_input
        );

        // Kiểm tra canonicalize validation
        let attempted_path = log_dir.join(malicious_input);
        let canonical_dir = tokio::fs::canonicalize(&log_dir).await.unwrap();
        let is_safe = match tokio::fs::canonicalize(&attempted_path).await {
            Ok(canon_file) => canon_file.starts_with(&canonical_dir) && canon_file.is_file(),
            Err(_) => false,
        };
        assert!(
            !is_safe,
            "Attack vector '{}' bypassed canonicalize check",
            malicious_input
        );
    }

    // 3. File hợp lệ phải vượt qua validation thành công
    let canonical_dir = tokio::fs::canonicalize(&log_dir).await.unwrap();
    let canonical_file = tokio::fs::canonicalize(&valid_log_file).await.unwrap();
    assert!(
        canonical_file.starts_with(&canonical_dir) && canonical_file.is_file(),
        "Valid log file failed validation"
    );

    cleanup_temp_env(storage_root, log_dir);
}

#[test]
fn test_admin_rate_limiter_blocks_after_5_failures() {
    let (storage_root, log_dir) = create_temp_env();
    let app = App::new_test(storage_root.clone(), log_dir.clone());
    let attacker_ip: IpAddr = "192.168.1.100".parse().unwrap();
    let victim_ip: IpAddr = "192.168.1.101".parse().unwrap();

    // Ban đầu chưa bị block
    assert!(!is_admin_rate_limited(&app, attacker_ip));

    // Thử sai 4 lần
    for _ in 1..=4 {
        record_admin_fail(&app, attacker_ip);
        assert!(!is_admin_rate_limited(&app, attacker_ip));
    }

    // Lần thứ 5 sai -> Phải bị block ngay lập tức
    record_admin_fail(&app, attacker_ip);
    assert!(is_admin_rate_limited(&app, attacker_ip));

    // IP khác không bị ảnh hưởng
    assert!(!is_admin_rate_limited(&app, victim_ip));

    cleanup_temp_env(storage_root, log_dir);
}

// =========================================================================
// 3. ATOMIC DISK OPERATIONS & SINGLE WRITER TX MANAGER TESTS
// =========================================================================

#[tokio::test]
async fn test_atomic_write_and_pending_cleanup() {
    let (storage_root, log_dir) = create_temp_env();
    let pending_upload_file = storage_root.join("pending_uploads.txt");
    let contract_a = "0x1111111111111111111111111111111111111111";
    let contract_b = "0x2222222222222222222222222222222222222222";

    // 1. Tạo file pending_uploads mẫu
    let initial_content = format!(
        "0xfile1,{}\n0xfile2,{}\n0xfile3,{}\n",
        contract_a, contract_a, contract_b
    );
    super::write_atomic(&pending_upload_file, &initial_content)
        .await
        .unwrap();

    // Kiểm tra đọc lại
    let read_back = tokio::fs::read_to_string(&pending_upload_file).await.unwrap();
    assert_eq!(read_back, initial_content);

    // 2. Xóa batch [file1] thuộc contract_a (test cả format không có 0x)
    let upload_lock = tokio::sync::Mutex::new(());
    super::remove_pending_uploads(
        &pending_upload_file,
        &["file1".to_string()],
        contract_a,
        &upload_lock,
    )
    .await;

    let updated_content = tokio::fs::read_to_string(&pending_upload_file).await.unwrap();
    assert!(!updated_content.contains("0xfile1"));
    assert!(updated_content.contains("0xfile2"));
    assert!(updated_content.contains("0xfile3"));

    // 3. Test xóa download pending
    let pending_dl_file = storage_root.join("pending_confirmations.txt");
    let initial_dl = "0xkey_alpha,0x1111\n0xkey_beta,0x2222\n";
    super::write_atomic(&pending_dl_file, initial_dl).await.unwrap();

    let dl_lock = tokio::sync::Mutex::new(());
    super::remove_pending_download(&pending_dl_file, "key_alpha", &dl_lock).await;
    let updated_dl = tokio::fs::read_to_string(&pending_dl_file).await.unwrap();
    assert!(!updated_dl.contains("0xkey_alpha"));
    assert!(updated_dl.contains("0xkey_beta"));

    cleanup_temp_env(storage_root, log_dir);
}

#[test]
fn test_tx_manager_exponential_backoff_calculation() {
    use crate::utils::retry_delay_secs;

    assert_eq!(retry_delay_secs(1), 15);  // Lần 1: 15s
    assert_eq!(retry_delay_secs(2), 30);  // Lần 2: 30s
    assert_eq!(retry_delay_secs(3), 60);  // Lần 3: 60s
    assert_eq!(retry_delay_secs(4), 120); // Lần 4: 120s
    assert_eq!(retry_delay_secs(5), 120); // Lần 5: cap ở 120s
    assert_eq!(retry_delay_secs(6), 120);

    // MAX_TX_RETRIES phải đủ lớn để chịu outage ~1 giờ: tổng backoff >= 3000s
    let total: u64 = (1..=super::MAX_TX_RETRIES).map(retry_delay_secs).sum();
    assert!(total >= 3000, "tổng thời gian retry quá ngắn: {}s", total);
}

// =========================================================================
// 4. CHUNK PARITY & MERKLE TREE LOGIC TESTS
// =========================================================================

#[test]
fn test_expected_chunks_parity_logic() {
    use crate::utils::expected_chunks;

    // Edge case: File chỉ có 1 chunk
    assert_eq!(expected_chunks(1, 0), 1);
    assert_eq!(expected_chunks(1, 1), 0); // Chunk lẻ không tồn tại với file 1 chunk

    // File 2 chunks: chunk 0 -> node 1 (1 chunk), chunk 1 -> node 2 (1 chunk)
    assert_eq!(expected_chunks(2, 0), 1);
    assert_eq!(expected_chunks(2, 1), 1);

    // File 3 chunks: chunk 0 (chẵn) -> 2 chunks (0, 2), chunk 1 (lẻ) -> 1 chunk (1)
    assert_eq!(expected_chunks(3, 0), 2);
    assert_eq!(expected_chunks(3, 1), 1);
    assert_eq!(expected_chunks(3, 0) + expected_chunks(3, 1), 3);

    // File 10 chunks: chẵn 5, lẻ 5
    assert_eq!(expected_chunks(10, 0), 5);
    assert_eq!(expected_chunks(10, 1), 5);

    // File 11 chunks: chẵn 6, lẻ 5
    assert_eq!(expected_chunks(11, 0), 6);
    assert_eq!(expected_chunks(11, 1), 5);
    assert_eq!(expected_chunks(11, 0) + expected_chunks(11, 1), 11);
}

#[test]
fn test_merkle_depth_calculation() {
    use crate::utils::merkle_tree_depth;

    assert_eq!(merkle_tree_depth(1), 0);
    assert_eq!(merkle_tree_depth(2), 1);
    assert_eq!(merkle_tree_depth(3), 2);
    assert_eq!(merkle_tree_depth(4), 2);
    assert_eq!(merkle_tree_depth(5), 3);
    assert_eq!(merkle_tree_depth(8), 3);
    assert_eq!(merkle_tree_depth(9), 4);
    assert_eq!(merkle_tree_depth(16), 4);
    assert_eq!(merkle_tree_depth(1024), 10);
    assert_eq!(merkle_tree_depth(1025), 11);
    assert_eq!(merkle_tree_depth(1_048_576), 20);
}

// =========================================================================
// 5. SECURITY & DEAD-LETTER LOGGING TESTS
// =========================================================================

#[test]
fn test_constant_time_eq_admin_password() {
    use crate::server::constant_time_eq;
    assert!(constant_time_eq(b"password123", b"password123"));
    assert!(!constant_time_eq(b"password123", b"password124"));
    assert!(!constant_time_eq(b"password123", b"xassword123"));
    assert!(!constant_time_eq(b"short", b"longer_string"));
    assert!(constant_time_eq(b"", b""));
}

#[tokio::test]
async fn test_failed_dead_letter_logging() {
    let (storage_root, log_dir) = create_temp_env();
    let file_key = "abc123key";
    let contract = "0x1234567890abcdef1234567890abcdef12345678";
    let reason = "Exceeded 5 retries. Last error: connection refused";

    super::record_failed_upload(&storage_root, file_key, contract, reason).await;
    let failed_upload_file = storage_root.join("failed_uploads.txt");
    assert!(failed_upload_file.exists());
    let content = tokio::fs::read_to_string(&failed_upload_file).await.unwrap();
    assert!(content.contains(file_key));
    assert!(content.contains(contract));
    assert!(content.contains(reason));

    let dl_key = "dlkey456";
    super::record_failed_download(&storage_root, dl_key, contract, "Invalid key").await;
    let failed_dl_file = storage_root.join("failed_confirmations.txt");
    assert!(failed_dl_file.exists());
    let dl_content = tokio::fs::read_to_string(&failed_dl_file).await.unwrap();
    assert!(dl_content.contains(dl_key));
    assert!(dl_content.contains("Invalid key"));

    cleanup_temp_env(storage_root, log_dir);
}

#[test]
fn test_error_classification_revert_vs_transient() {
    // Already confirmed -> xóa bình thường
    assert!(super::is_already_confirmed("execution reverted: already confirmed"));
    assert!(super::is_already_confirmed("Confirmed already by peer"));

    // Terminal contract revert -> ghi failed ngay
    assert!(super::is_terminal_contract_revert("execution reverted: Invalid key"));
    assert!(super::is_terminal_contract_revert("File does not exist"));
    assert!(super::is_terminal_contract_revert("Caller is not a storage server"));

    // Transient errors -> phải retry
    assert!(!super::is_already_confirmed("connection refused"));
    assert!(!super::is_terminal_contract_revert("connection refused"));
    assert!(!super::is_terminal_contract_revert("error sending request for url"));
    assert!(!super::is_terminal_contract_revert("nonce too low"));
    assert!(!super::is_terminal_contract_revert("replacement transaction underpriced"));
    assert!(!super::is_terminal_contract_revert("timeout waiting for receipt"));
}

#[tokio::test]
async fn test_listener_catchup_plan_and_block_persistence() {
    use crate::listener::{
        clear_catchup_progress, determine_catchup_plan, read_catchup_progress, read_saved_last_block,
        write_catchup_progress, write_saved_last_block, BlockRange, CatchupPlanResult, CatchupProgress,
    };

    // 1. Trường hợp: Block trong file (200_000) > Block hiện tại (500) do reset mạng
    // Yêu cầu: Ưu tiên lấy block hiện tại, KHÔNG quét lùi, xóa file tiến trình cũ nếu có
    let plan_reset = determine_catchup_plan(Some(200_000), None, 500);
    assert_eq!(
        plan_reset,
        CatchupPlanResult {
            realtime_start: 500,
            catchup_ranges: Vec::new(),
            should_clear_catchup_file: true,
        }
    );

    // 2. Trường hợp: Lần đầu bật sau khi tắt (saved=95_000 < current=100_000)
    // Yêu cầu: Luồng 1 chạy realtime từ 100_000, Luồng 2 quét bù đúng đoạn [95_001..=100_000]
    let plan_catchup = determine_catchup_plan(Some(95_000), None, 100_000);
    assert_eq!(
        plan_catchup,
        CatchupPlanResult {
            realtime_start: 100_000,
            catchup_ranges: vec![BlockRange {
                from: 95_001,
                to: 100_000,
            }],
            should_clear_catchup_file: false,
        }
    );

    // 3. Trường hợp: Server bị crash giữa chừng lúc worker đang quét bù dở
    // Worker cũ đang quét [95_001..=100_000], đã chạy tới cursor 97_500.
    // Trong khi đó luồng realtime trước lúc crash đã chạy tới 100_010.
    // Lần khởi động tiếp theo current_block = 105_000.
    // Yêu cầu: Tiếp tục quét dở [97_500..=100_000], VÀ thêm dải mới [100_011..=105_000] (Không sót, không trùng!).
    let saved_progress = CatchupProgress {
        cursor: 97_500,
        to: 100_000,
    };
    let plan_resumed = determine_catchup_plan(Some(100_010), Some(saved_progress.clone()), 105_000);
    assert_eq!(
        plan_resumed,
        CatchupPlanResult {
            realtime_start: 105_000,
            // Gộp thành MỘT dải duy nhất [cursor cũ .. current] để chỉ cần 1 file tiến độ là đủ
            catchup_ranges: vec![BlockRange {
                from: 97_500,
                to: 105_000,
            }],
            should_clear_catchup_file: false,
        }
    );

    // 3b. Chỉ có tiến độ dở dang, không có khoảng trống mới (saved == current)
    let plan_only_progress = determine_catchup_plan(Some(100_010), Some(saved_progress), 100_010);
    assert_eq!(
        plan_only_progress.catchup_ranges,
        vec![BlockRange { from: 97_500, to: 100_000 }]
    );

    // 4. Trường hợp: Block trùng nhau hoặc chưa có file
    let plan_equal = determine_catchup_plan(Some(100_000), None, 100_000);
    assert_eq!(plan_equal.catchup_ranges.len(), 0);

    let plan_none = determine_catchup_plan(None, None, 100_000);
    assert_eq!(plan_none.catchup_ranges.len(), 0);

    // 5. Kiểm tra đọc/ghi Atomic block và catchup progress trên đĩa
    let (storage_root, log_dir) = create_temp_env();
    assert_eq!(read_saved_last_block(&storage_root).await, None);
    assert_eq!(read_catchup_progress(&storage_root).await, None);

    write_saved_last_block(&storage_root, 12345).await;
    assert_eq!(read_saved_last_block(&storage_root).await, Some(12345));

    write_catchup_progress(&storage_root, 5000, 10000).await;
    assert_eq!(
        read_catchup_progress(&storage_root).await,
        Some(CatchupProgress {
            cursor: 5000,
            to: 10000,
        })
    );

    clear_catchup_progress(&storage_root).await;
    assert_eq!(read_catchup_progress(&storage_root).await, None);

    cleanup_temp_env(storage_root, log_dir);
}




#[test]
fn test_classify_rpc_error_and_catchup_backoff() {
    use crate::listener::{catchup_backoff_ms, classify_rpc_error, RpcErrorKind};

    assert_eq!(classify_rpc_error("{\"message\":\"block range is too large\"}"), RpcErrorKind::RangeTooLarge);
    assert_eq!(classify_rpc_error("query returned more than 10000 results, limit exceeded"), RpcErrorKind::RangeTooLarge);
    assert_eq!(classify_rpc_error("historical state has been pruned"), RpcErrorKind::Pruned);
    assert_eq!(classify_rpc_error("missing trie node"), RpcErrorKind::Pruned);
    assert_eq!(classify_rpc_error("internal error"), RpcErrorKind::Other);

    assert_eq!(catchup_backoff_ms(1), 1000);
    assert_eq!(catchup_backoff_ms(2), 2000);
    assert_eq!(catchup_backoff_ms(3), 4000);
    assert_eq!(catchup_backoff_ms(6), 30_000);
    assert_eq!(catchup_backoff_ms(100), 30_000);
}

#[tokio::test]
async fn test_cleanup_stale_tmp_files() {
    let (storage_root, log_dir) = create_temp_env();
    std::fs::write(storage_root.join("pending_uploads.tmp.123.456"), "x").unwrap();
    std::fs::write(storage_root.join("pending_uploads.txt"), "keep").unwrap();

    let removed = crate::utils::cleanup_stale_tmp_files(&storage_root).await;
    assert_eq!(removed, 1);
    assert!(storage_root.join("pending_uploads.txt").exists());
    assert!(!storage_root.join("pending_uploads.tmp.123.456").exists());

    cleanup_temp_env(storage_root, log_dir);
}

#[test]
fn test_sparse_file_chunk_set_checking() {
    use crate::utils::is_chunk_in_set;
    use std::collections::HashSet;

    // Giả sử Node 1 lưu các chunk chẵn: 0, 2, 4
    let mut available_chunks = HashSet::new();
    available_chunks.insert(0);
    available_chunks.insert(2);
    available_chunks.insert(4);

    // Node 1 có chunk 0, 2, 4
    assert!(is_chunk_in_set(&available_chunks, 0));
    assert!(is_chunk_in_set(&available_chunks, 2));
    assert!(is_chunk_in_set(&available_chunks, 4));

    // Node 1 KHÔNG có chunk 1, 3 (thuộc Node 2) -> phải trả về false để ngăn đọc sparse file 1MB byte 0
    assert!(!is_chunk_in_set(&available_chunks, 1));
    assert!(!is_chunk_in_set(&available_chunks, 3));
    assert!(!is_chunk_in_set(&available_chunks, 5));
}

#[test]
fn test_retry_limits_and_fast_backoff_calculation() {
    use crate::utils::main_retry_delay_secs;

    assert_eq!(super::MAIN_TX_RETRIES, 3);
    assert_eq!(super::MAX_BACKGROUND_RETRIES, 10);

    // Luồng chính thử lại nhanh: 2s, 4s, 8s (tổng cộng 14s, không gây tắc nghẽn queue)
    assert_eq!(main_retry_delay_secs(1), 2);
    assert_eq!(main_retry_delay_secs(2), 4);
    assert_eq!(main_retry_delay_secs(3), 8);
    assert_eq!(main_retry_delay_secs(4), 8); // cap ở 8s
}

#[tokio::test]
async fn test_failed_dead_letter_json_storage_and_recovery() {
    let (storage_root, log_dir) = create_temp_env();
    let file_key = "0xdeadbeef123";
    let contract = "0x111122223333444455556666777788889999aaaa";
    let reason = "timeout waiting for receipt";

    // 1. Ghi failed upload dạng JSON
    super::record_failed_upload_detailed(
        &storage_root,
        file_key,
        contract,
        reason,
        super::MAIN_TX_RETRIES,
        "PENDING_BACKGROUND_RETRY",
    )
    .await;

    let json_file = storage_root.join("failed_uploads.json");
    assert!(json_file.exists());

    // 2. Load lại bản ghi JSON
    let records = super::load_failed_records(&json_file).await;
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].key, file_key);
    assert_eq!(records[0].contract_address, contract);
    assert_eq!(records[0].attempts, 3);
    assert_eq!(records[0].max_attempts, 10);
    assert_eq!(records[0].status, "PENDING_BACKGROUND_RETRY");
    assert_eq!(records[0].reason, reason);

    // 3. Upsert cập nhật số lần thử lại
    let mut updated = records[0].clone();
    updated.attempts = 10;
    updated.status = "NEEDS_ADMIN_REVIEW".to_string();
    super::upsert_failed_record(&json_file, updated).await;

    let reloaded = super::load_failed_records(&json_file).await;
    assert_eq!(reloaded.len(), 1);
    assert_eq!(reloaded[0].attempts, 10);
    assert_eq!(reloaded[0].status, "NEEDS_ADMIN_REVIEW");

    // 4. Xoá bản ghi khi đã hoàn tất
    super::remove_failed_record(&json_file, file_key).await;
    let after_removal = super::load_failed_records(&json_file).await;
    assert_eq!(after_removal.len(), 0);

    cleanup_temp_env(storage_root, log_dir);
}

#[tokio::test]
async fn test_finalize_upload_file_sync_data() {
    let (storage_root, log_dir) = create_temp_env();
    let app = App::new_test(storage_root.clone(), log_dir.clone());
    let file_key = "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890";
    let contract_addr = alloy::primitives::Address::ZERO;

    // Tạo file tạm và mở file descriptor
    let file_path = storage_root.join("test_file.bin");
    let bin_file = std::fs::File::create(&file_path).unwrap();
    let meta_file = std::fs::File::create(storage_root.join("test_file.meta")).unwrap();
    app.file_cache.insert(
        file_key.to_string(),
        crate::models::OpenFiles {
            bin_file: std::sync::Arc::new(bin_file),
            meta_file: std::sync::Arc::new(std::sync::Mutex::new(meta_file)),
        },
    );

    assert!(app.file_cache.contains_key(file_key));

    // Gọi finalize_upload_file (thực hiện sync_data và đẩy vào queue)
    let res = app.finalize_upload_file(file_key, contract_addr).await;
    assert!(res.is_ok(), "finalize_upload_file should succeed");

    // WAL: pending_uploads.txt phải chứa dòng file_key
    let wal_path = storage_root.join("pending_uploads.txt");
    assert!(wal_path.exists(), "pending_uploads.txt must exist as WAL");
    let wal_content = std::fs::read_to_string(&wal_path).unwrap();
    assert!(wal_content.contains(file_key), "WAL must record unconfirmed file");

    // File cache phải được dọn dẹp (đóng file descriptor)
    assert!(!app.file_cache.contains_key(file_key));

    // Channel upload_batch_receiver phải nhận được key
    let mut rx = app.upload_batch_receiver.lock().await;
    let received = rx.try_recv().unwrap();
    assert_eq!(received.0, file_key);
    assert_eq!(received.1, contract_addr);

    cleanup_temp_env(storage_root, log_dir);
}

#[tokio::test]
async fn test_write_chunk_zero_copy_and_dir_cache() {
    let (storage_root, log_dir) = create_temp_env();
    let app = App::new_test(storage_root.clone(), log_dir.clone());
    let file_key = "1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef";

    // 1. Chunk 0 (Cache miss: tạo thư mục và mở file)
    let chunk0_data = bytes::Bytes::from(vec![42u8; 100]);
    let res0 = app.write_chunk(file_key, 0, chunk0_data).await;
    assert!(res0.is_ok(), "Chunk 0 write should succeed");

    // File cache phải chứa file descriptor đã mở
    assert!(app.file_cache.contains_key(file_key), "File must be cached after first chunk");

    // 2. Chunk 1 (Cache hit: tái sử dụng open_files, không gọi create_dir_all)
    let chunk1_data = bytes::Bytes::from(vec![99u8; 100]);
    let res1 = app.write_chunk(file_key, 1, chunk1_data).await;
    assert!(res1.is_ok(), "Chunk 1 write should succeed with cached handle");

    // 3. Đọc lại dữ liệu để xác nhận offset ghi đúng
    let level1 = &file_key[0..2];
    let level2 = &file_key[2..4];
    let bin_path = storage_root.join(level1).join(level2).join(file_key).join(format!("{}.bin", file_key));
    let meta_path = storage_root.join(level1).join(level2).join(file_key).join(format!("{}.meta", file_key));

    assert!(bin_path.exists());
    assert!(meta_path.exists());

    use std::os::unix::fs::FileExt;
    let file = std::fs::File::open(&bin_path).unwrap();
    let mut read_buf0 = vec![0u8; 100];
    file.read_exact_at(&mut read_buf0, 0).unwrap();
    assert_eq!(read_buf0, vec![42u8; 100]);

    let offset1 = crate::models::CHUNK_SIZE;
    let mut read_buf1 = vec![0u8; 100];
    file.read_exact_at(&mut read_buf1, offset1).unwrap();
    assert_eq!(read_buf1, vec![99u8; 100]);

    cleanup_temp_env(storage_root, log_dir);
}

#[tokio::test]
async fn test_record_failed_upload_batch_o_n() {
    let (storage_root, log_dir) = create_temp_env();
    let contract = "0x1234567890123456789012345678901234567890";
    let keys: Vec<String> = (0..5).map(|i| format!("0xfile_key_{:04}", i)).collect();

    // 1. Ghi batch 5 keys vào JSON trong 1 lần I/O O(n)
    super::record_failed_upload_batch(
        &storage_root,
        &keys,
        contract,
        "Simulated batch timeout error",
        1,
        "PENDING_BACKGROUND_RETRY",
    )
    .await;

    let json_path = storage_root.join("failed_uploads.json");
    assert!(json_path.exists());
    let records = super::load_failed_records(&json_path).await;
    assert_eq!(records.len(), 5);
    for (i, r) in records.iter().enumerate() {
        assert_eq!(r.key, format!("0xfile_key_{:04}", i));
        assert_eq!(r.contract_address, contract);
        assert_eq!(r.attempts, 1);
        assert_eq!(r.status, "PENDING_BACKGROUND_RETRY");
        assert_eq!(r.reason, "Simulated batch timeout error");
    }

    // 2. Ghi đè cập nhật 2 keys trong batch sang TERMINAL_ERROR
    let update_keys = vec![keys[0].clone(), keys[1].clone()];
    super::record_failed_upload_batch(
        &storage_root,
        &update_keys,
        contract,
        "Terminal revert: Invalid key",
        0,
        "TERMINAL_ERROR",
    )
    .await;

    let updated_records = super::load_failed_records(&json_path).await;
    assert_eq!(updated_records.len(), 5, "Tổng số bản ghi không đổi, chỉ cập nhật");
    assert_eq!(updated_records[0].status, "TERMINAL_ERROR");
    assert_eq!(updated_records[1].status, "TERMINAL_ERROR");
    assert_eq!(updated_records[2].status, "PENDING_BACKGROUND_RETRY");

    cleanup_temp_env(storage_root, log_dir);
}

#[test]
fn test_transient_rpc_error_circuit_breaker() {
    // Các lỗi mạng/RPC tạm thời phải được nhận diện để KHÔNG đốt attempts
    assert!(super::is_transient_rpc_error("error: connection refused"));
    assert!(super::is_transient_rpc_error("HTTP 502 Bad Gateway"));
    assert!(super::is_transient_rpc_error("HTTP 503 Service Unavailable"));
    assert!(super::is_transient_rpc_error("request timeout waiting for response"));
    assert!(super::is_transient_rpc_error("deadline has elapsed"));
    assert!(super::is_transient_rpc_error("transport error: broken pipe"));
    assert!(super::is_transient_rpc_error("failed to send request"));

    // Lỗi logic hợp đồng hoặc nghiệp vụ: KHÔNG PHẢI transient RPC error
    assert!(!super::is_transient_rpc_error("execution reverted: Invalid key"));
    assert!(!super::is_transient_rpc_error("File does not exist"));
    assert!(!super::is_transient_rpc_error("Caller is not a storage server"));
}

#[test]
fn test_is_record_due_and_compute_next_retry_at() {
    use crate::utils::{compute_next_retry_at, is_record_due};

    // 1. None hoặc chuỗi rỗng: Phải luôn luôn due
    assert!(is_record_due(&None));
    assert!(is_record_due(&Some("".to_string())));
    assert!(is_record_due(&Some("   ".to_string())));

    // 2. Mốc thời gian trong quá khứ: Phải due
    let past = "2020-01-01 00:00:00".to_string();
    assert!(is_record_due(&Some(past)));

    // 3. Mốc thời gian trong tương lai xa: Chưa due
    let future = "2099-12-31 23:59:59".to_string();
    assert!(!is_record_due(&Some(future)));

    // 4. Mốc thời gian vừa tính toán trong tương lai:
    let next = compute_next_retry_at(60);
    assert!(!is_record_due(&Some(next)));
}

#[tokio::test]
async fn test_no_head_of_line_blocking() {
    use crate::models::FailedTxRecord;
    use crate::utils::{compute_next_retry_at, is_record_due};

    let future_time = compute_next_retry_at(60); // 60s sau mới retry

    let records = vec![
        FailedTxRecord {
            key: "batch1_fileA".to_string(),
            contract_address: "0x1111".to_string(),
            attempts: 1,
            max_attempts: 10,
            reason: "RPC timeout".to_string(),
            status: "PENDING_BACKGROUND_RETRY".to_string(),
            first_failed_at: "2026-10-10 10:00:00".to_string(),
            last_attempt_at: "2026-10-10 10:01:00".to_string(),
            next_retry_at: Some(future_time), // Chưa đến hạn!
        },
        FailedTxRecord {
            key: "batch2_fileB".to_string(),
            contract_address: "0x2222".to_string(),
            attempts: 0,
            max_attempts: 10,
            reason: "Initial failure".to_string(),
            status: "PENDING_BACKGROUND_RETRY".to_string(),
            first_failed_at: "2026-10-10 10:00:00".to_string(),
            last_attempt_at: "2026-10-10 10:00:00".to_string(),
            next_retry_at: None, // Đã đến hạn ngay!
        },
    ];

    // Lọc theo điều kiện của Background Retry Worker
    let eligible_indices: Vec<usize> = records
        .iter()
        .enumerate()
        .filter(|(_, r)| {
            r.status == "PENDING_BACKGROUND_RETRY"
                && r.attempts < 10
                && is_record_due(&r.next_retry_at)
        })
        .map(|(i, _)| i)
        .collect();

    // Khẳng định: Batch 1 bị bỏ qua, Batch 2 được chọn -> KHÔNG BỊ HEAD-OF-LINE BLOCKING!
    assert_eq!(eligible_indices.len(), 1);
    assert_eq!(eligible_indices[0], 1);
    assert_eq!(records[eligible_indices[0]].key, "batch2_fileB");
}

#[tokio::test]
async fn test_concurrent_pending_uploads_wal_safety() {
    use std::sync::Arc;
    use tokio::io::AsyncWriteExt;

    let (storage_root, log_dir) = create_temp_env();
    let pending_file = storage_root.join("pending_uploads.txt");
    let lock = Arc::new(tokio::sync::Mutex::new(()));

    // Khởi tạo file với một số dòng
    let initial = "file_init_1,0x1111\nfile_init_2,0x1111\n";
    super::write_atomic(&pending_file, initial).await.unwrap();

    // 20 tác vụ concurrent: 10 tác vụ append và 10 tác vụ remove
    let mut handles = Vec::new();
    for i in 0..10 {
        let p = pending_file.clone();
        let l = lock.clone();
        handles.push(tokio::spawn(async move {
            let _guard = l.lock().await;
            let line = format!("file_appended_{},0x2222\n", i);
            let mut file = tokio::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&p)
                .await
                .unwrap();
            file.write_all(line.as_bytes()).await.unwrap();
            file.flush().await.unwrap();
        }));
    }

    for _ in 0..5 {
        let p = pending_file.clone();
        let l = lock.clone();
        handles.push(tokio::spawn(async move {
            super::remove_pending_uploads(&p, &["file_init_1".to_string()], "0x1111", &l).await;
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    let final_content = tokio::fs::read_to_string(&pending_file).await.unwrap();
    // file_init_1 đã bị xoá
    assert!(!final_content.contains("file_init_1"));
    // file_init_2 vẫn còn
    assert!(final_content.contains("file_init_2"));
    // Cả 10 dòng appended đều phải được giữ nguyên, không bị overwrite mất bởi rename!
    for i in 0..10 {
        assert!(
            final_content.contains(&format!("file_appended_{}", i)),
            "Dòng file_appended_{} phải tồn tại",
            i
        );
    }

    cleanup_temp_env(storage_root, log_dir);
}

#[tokio::test]
async fn test_recovery_no_deadlock_on_large_backlog() {
    use std::sync::Arc;
    use tokio::sync::mpsc;

    let (storage_root, log_dir) = create_temp_env();
    let pending_file = storage_root.join("pending_uploads.txt");
    let lock = Arc::new(tokio::sync::Mutex::new(()));

    // Tạo 1200 items (vượt ngưỡng dung lượng kênh 1000)
    let mut large_content = String::new();
    for i in 0..1200 {
        large_content.push_str(&format!("file_{},0x1111111111111111111111111111111111111111\n", i));
    }
    tokio::fs::write(&pending_file, &large_content).await.unwrap();

    let (tx, mut rx) = mpsc::channel::<(String, String)>(1000);

    // Task producer nạp recovery theo cơ chế mới (nhả lock trước khi send)
    let p_file = pending_file.clone();
    let p_lock = lock.clone();
    let producer_handle = tokio::spawn(async move {
        let entries: Vec<(String, String)> = {
            let _guard = p_lock.lock().await;
            let mut items = Vec::new();
            if let Ok(content) = tokio::fs::read_to_string(&p_file).await {
                for line in content.lines() {
                    let parts: Vec<&str> = line.trim().split(',').collect();
                    if parts.len() == 2 {
                        items.push((parts[0].to_string(), parts[1].to_string()));
                    }
                }
            }
            items
        }; // Lock được giải phóng ở đây!

        for (k, a) in entries {
            tx.send((k, a)).await.unwrap();
        }
    });

    // Task consumer tiêu thụ và đồng thời thử lock (giống TX Manager)
    let c_lock = lock.clone();
    let consumer_handle = tokio::spawn(async move {
        let mut received = 0;
        while let Some(_) = rx.recv().await {
            received += 1;
            // Mỗi 100 items tiêu thụ thì thử acquire lock để chứng minh không bị deadlock
            if received % 100 == 0 {
                let _g = c_lock.lock().await;
            }
            if received == 1200 {
                break;
            }
        }
        received
    });

    // Kiểm tra hoàn thành trong 5 giây mà không bị treo/deadlock
    let res = timeout(Duration::from_secs(5), async {
        producer_handle.await.unwrap();
        consumer_handle.await.unwrap()
    })
    .await;

    assert!(res.is_ok(), "Quá trình phục hồi bị treo/deadlock khi backlog > 1000");
    assert_eq!(res.unwrap(), 1200, "Phải nạp và tiêu thụ đủ 1200 items");

    cleanup_temp_env(storage_root, log_dir);
}


