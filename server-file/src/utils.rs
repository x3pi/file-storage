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

/// Tính độ sâu của Merkle Tree từ tổng số chunk.
#[inline]
pub fn merkle_tree_depth(total_chunks: u64) -> usize {
    if total_chunks <= 1 {
        0
    } else {
        (total_chunks as f64).log2().ceil() as usize
    }
}
