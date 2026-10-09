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
    super::remove_pending_uploads(
        &pending_upload_file,
        &["file1".to_string()],
        contract_a,
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

    super::remove_pending_download(&pending_dl_file, "key_alpha").await;
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
