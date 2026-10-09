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
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let tmp_path = path.with_extension(format!("tmp.{}.{}", std::process::id(), nonce));
    tokio::fs::write(&tmp_path, content).await?;
    tokio::fs::rename(&tmp_path, path).await?;
    Ok(())
}
