/// Utility functions shared across production code and unit tests.

/// Tính toán số chunk kỳ vọng mà một server node (chẵn hoặc lẻ) cần nhận:
/// - File chỉ có 1 chunk: chỉ node chẵn (chunk_index == 0) giữ 1 chunk, node lẻ giữ 0 chunk.
/// - File nhiều chunks:
///   + Chunk chẵn (chunk_index % 2 == 0): giữ (total_chunks + 1) / 2
///   + Chunk lẻ (chunk_index % 2 != 0): giữ total_chunks / 2
#[inline]
pub fn expected_chunks(total_chunks: u64, chunk_index: u64) -> u64 {
    if total_chunks == 1 {
        if chunk_index == 0 { 1 } else { 0 }
    } else if chunk_index % 2 == 0 {
        (total_chunks + 1) / 2
    } else {
        total_chunks / 2
    }
}

/// Tính thời gian chờ (giây) thử lại giao dịch theo Exponential Backoff:
/// delay = (15 * 2^(attempt - 1)), tối đa 120s.
#[inline]
pub fn retry_delay_secs(attempt: u32) -> u64 {
    let attempt = attempt.max(1);
    (15u64 * (1u64 << (attempt - 1).min(3))).min(120)
}

/// Thời gian chờ nhanh cho luồng chính (giây) để thử lại tối đa 3 lần trước khi nhường queue:
/// Lần 1: 2s, Lần 2: 4s, Lần 3: 8s
#[inline]
pub fn main_retry_delay_secs(attempt: u32) -> u64 {
    let attempt = attempt.max(1);
    (2u64 * (1u64 << (attempt - 1).min(2))).min(8)
}

/// Kiểm tra một chunk index có nằm trong tập hợp các chunk thực tế mà node đang lưu trữ hay không.
/// Ngăn chặn việc đọc sparse file trả về toàn bộ 1MB byte 0 cho các chunk của node khác.
#[inline]
pub fn is_chunk_in_set(available_chunks: &std::collections::HashSet<u64>, chunk_index: u64) -> bool {
    available_chunks.contains(&chunk_index)
}

/// Tính độ sâu của Merkle Tree từ tổng số chunk bằng phép toán bitwise chính xác,
/// không sử dụng số thực (f64) để tránh sai số làm tròn khi chunk lớn.
#[inline]
pub fn merkle_tree_depth(total_chunks: u64) -> usize {
    if total_chunks <= 1 {
        0
    } else {
        total_chunks.next_power_of_two().trailing_zeros() as usize
    }
}

/// Ghi file an toàn (Atomic Write) qua file tạm và POSIX rename,
/// tránh nguy cơ file bị rỗng (0-byte) khi sập nguồn hoặc crash đột ngột.
pub async fn write_atomic(path: &std::path::Path, content: &str) -> std::io::Result<()> {
    use tokio::io::AsyncWriteExt;
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let tmp_path = path.with_extension(format!("tmp.{}.{}", std::process::id(), nonce));
    {
        let mut file = tokio::fs::File::create(&tmp_path).await?;
        file.write_all(content.as_bytes()).await?;
        file.flush().await?;
        file.sync_data().await?;
    }
    tokio::fs::rename(&tmp_path, path).await?;
    Ok(())
}

/// Dọn các file tạm `*.tmp.<pid>.<nonce>` do `write_atomic` để lại khi process crash giữa `write` và `rename`.
/// Chỉ quét thư mục gốc (không đệ quy). Trả về số file đã xoá.
pub async fn cleanup_stale_tmp_files(dir: &std::path::Path) -> usize {
    let mut removed = 0;
    if let Ok(mut entries) = tokio::fs::read_dir(dir).await {
        while let Ok(Some(entry)) = entries.next_entry().await {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if name.contains(".tmp.") && entry.file_type().await.map(|t| t.is_file()).unwrap_or(false) {
                if tokio::fs::remove_file(entry.path()).await.is_ok() {
                    removed += 1;
                }
            }
        }
    }
    removed
}

/// Kiểm tra xem một bản ghi lỗi đã đến hạn retry (next_retry_at <= now hoặc None) hay chưa.
#[inline]
pub fn is_record_due(next_retry_at: &Option<String>) -> bool {
    match next_retry_at {
        None => true,
        Some(s) if s.trim().is_empty() => true,
        Some(s) => {
            if let Ok(parsed) = chrono::NaiveDateTime::parse_from_str(s.trim(), "%Y-%m-%d %H:%M:%S") {
                chrono::Local::now().naive_local() >= parsed
            } else {
                true
            }
        }
    }
}

/// Tính thời điểm tiếp theo retry giao dịch theo định dạng "%Y-%m-%d %H:%M:%S"
#[inline]
pub fn compute_next_retry_at(delay_secs: u64) -> String {
    let next = chrono::Local::now() + chrono::Duration::seconds(delay_secs as i64);
    next.format("%Y-%m-%d %H:%M:%S").to_string()
}
