use anyhow::{anyhow, bail, Context, Result};
use bytes::Bytes;
use network::quic::QuicConnection;
use network::quic::QuicTransport;
use network::transport::Transport;
use std::net::{SocketAddr, UdpSocket};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot, Semaphore};
use tokio::time::{sleep, timeout};

const CHUNK_SIZE: usize = 1 * 1024 * 1024; // 1 MiB
const NUM_CONNECTIONS: usize = 10;
const TOTAL_CHUNKS: usize = 2000;
const STREAM_CONCURRENCY_PER_CONN: usize = 10;
const CONCURRENCY: usize = NUM_CONNECTIONS * STREAM_CONCURRENCY_PER_CONN;
const LOG_INTERVAL: usize = 50;
const CHUNK_TIMEOUT: Duration = Duration::from_secs(30); // Timeout cho mỗi chunk

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TestMode {
    Upload,
    Download,
}

#[tokio::main]
async fn main() -> Result<()> {
    let expected_bytes = CHUNK_SIZE * TOTAL_CHUNKS;
    let base_transport = QuicTransport::new();

    // --- 1. UPLOAD TEST ---
    println!("---- 1. Bắt đầu test UPLOAD ----");
    let upload_addr = find_available_addr().context("Không tìm được cổng cho UPLOAD")?;
    let (done_tx_up, done_rx_up) = oneshot::channel();
    let server_transport_up = base_transport.clone();

    tokio::spawn(async move {
        if let Err(e) = run_server(
            server_transport_up,
            upload_addr,
            NUM_CONNECTIONS,
            TOTAL_CHUNKS,
            CHUNK_SIZE,
            TestMode::Upload,
            done_tx_up,
        )
        .await
        {
            eprintln!("[server-upload] lỗi: {e:?}");
        }
    });

    sleep(Duration::from_millis(200)).await;

    let upload_elapsed = run_client(
        base_transport.clone(),
        upload_addr,
        NUM_CONNECTIONS,
        TOTAL_CHUNKS,
        CHUNK_SIZE,
        CONCURRENCY,
        TestMode::Upload,
    )
    .await?;
    let _ = done_rx_up.await;
    print_stats("UPLOAD", upload_elapsed, expected_bytes, TOTAL_CHUNKS);

    // --- 2. DOWNLOAD TEST ---
    println!("\n---- 2. Bắt đầu test DOWNLOAD ----");
    let download_addr = find_available_addr().context("Không tìm được cổng cho DOWNLOAD")?;
    let (done_tx_down, done_rx_down) = oneshot::channel();
    let server_transport_down = base_transport.clone();

    tokio::spawn(async move {
        if let Err(e) = run_server(
            server_transport_down,
            download_addr,
            NUM_CONNECTIONS,
            TOTAL_CHUNKS,
            CHUNK_SIZE,
            TestMode::Download,
            done_tx_down,
        )
        .await
        {
            eprintln!("[server-download] lỗi: {e:?}");
        }
    });
    sleep(Duration::from_millis(200)).await;

    let download_elapsed = run_client(
        base_transport.clone(),
        download_addr,
        NUM_CONNECTIONS,
        TOTAL_CHUNKS,
        CHUNK_SIZE,
        CONCURRENCY,
        TestMode::Download,
    )
    .await?;
    let _ = done_rx_down.await;
    print_stats("DOWNLOAD", download_elapsed, expected_bytes, TOTAL_CHUNKS);

    // --- 3. KẾT LUẬN ---
    println!("\n---- 3. TỔNG KẾT SO SÁNH ----");
    println!("Thời gian Upload:   {:.3} giây", upload_elapsed.as_secs_f64());
    println!("Thời gian Download: {:.3} giây", download_elapsed.as_secs_f64());
    if upload_elapsed < download_elapsed {
        let ratio = download_elapsed.as_secs_f64() / upload_elapsed.as_secs_f64();
        println!("=> UPLOAD nhanh hơn {:.2}x", ratio);
    } else {
         let ratio = upload_elapsed.as_secs_f64() / download_elapsed.as_secs_f64();
        println!("=> DOWNLOAD nhanh hơn {:.2}x", ratio);
    }

    Ok(())
}

// Helper in kết quả
fn print_stats(mode: &str, elapsed: Duration, expected_bytes: usize, total_chunks: usize) {
    let total_mb = expected_bytes as f64 / (1024.0 * 1024.0);
    let throughput = total_mb / elapsed.as_secs_f64();
    let per_chunk_ms = elapsed.as_secs_f64() * 1000.0 / total_chunks as f64;
    let chunks_per_second = total_chunks as f64 / elapsed.as_secs_f64();

    println!("---- Kết quả QUIC chunk benchmark [{mode}] ----");
    println!("Số kết nối: {NUM_CONNECTIONS}");
    println!("Số chunk: {total_chunks}");
    println!("Kích thước mỗi chunk: {CHUNK_SIZE} bytes");
    println!("Độ đồng thời: {CONCURRENCY}");
    println!("Thời gian: {:.3} giây", elapsed.as_secs_f64());
    println!("Thông lượng: {:.2} MiB/s", throughput);
    println!("Tốc độ xử lý: {:.2} chunk/giây", chunks_per_second);
    println!("Thời gian trung bình mỗi chunk: {:.3} ms", per_chunk_ms);
}

async fn run_server(
    transport: QuicTransport,
    addr: SocketAddr,
    num_connections: usize,
    total_chunks: usize,
    chunk_size: usize,
    mode: TestMode, // <-- Mới
    done: oneshot::Sender<()>,
) -> Result<()> {
    let mut listener = match transport.listen(addr).await {
        Ok(listener) => listener,
        Err(e) => {
            let _ = done.send(());
            return Err(anyhow!("Không listen được QUIC: {e}"));
        }
    };

    let assignments = distribute_chunks(total_chunks, num_connections);
    let progress = Arc::new(AtomicUsize::new(0));
    let mut handles = Vec::with_capacity(num_connections);
    
    let server_label = if mode == TestMode::Upload { "[server-upload]" } else { "[server-download]" };

    for (idx, expected_chunks) in assignments.into_iter().enumerate() {
        let (mut connection, peer) = listener
            .accept()
            .await
            .map_err(|e| anyhow!("Không accept được kết nối: {e}"))?;
        println!(
            "{} nhận kết nối {}/{} từ {}",
            server_label,
            idx + 1,
            num_connections,
            peer
        );

        let quic_conn = connection
            .as_any_mut()
            .downcast_mut::<QuicConnection>()
            .context("Kết nối nhận được không phải QuicConnection")?;
        let raw_conn = quic_conn.clone_connection();
        drop(connection);

        let progress = progress.clone();
        handles.push(tokio::spawn(async move {
            run_server_connection(
                raw_conn,
                expected_chunks,
                chunk_size,
                total_chunks,
                progress,
                mode, // <-- Mới
                server_label, // <-- Mới
            )
            .await
        }));
    }

    let mut total_received = 0usize;
    for handle in handles {
        total_received += handle.await.expect("Task server bị panic")?;
    }

    println!("{} hoàn tất xử lý {} chunk", server_label, total_received);
    let _ = done.send(());

    Ok(())
}

async fn run_server_connection(
    connection: quinn::Connection,
    expected_chunks: usize,
    chunk_size: usize,
    total_chunks: usize,
    progress: Arc<AtomicUsize>,
    mode: TestMode, // <-- Mới
    server_label: &'static str, // <-- Mới
) -> Result<usize> {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let connection_clone = connection.clone();

    // Spawn task để liên tục accept stream
    let accept_handle = tokio::spawn(async move {
        let mut count = 0;
        loop {
            match connection_clone.accept_bi().await {
                Ok(streams) => {
                    if tx.send(streams).is_err() {
                        break; // Receiver đã đóng
                    }
                    count += 1;
                    if count >= expected_chunks {
                        break;
                    }
                }
                Err(quinn::ConnectionError::ApplicationClosed(_))
                | Err(quinn::ConnectionError::LocallyClosed) => {
                    break;
                }
                Err(e) => {
                    eprintln!("{} Lỗi accept stream: {:?}", server_label, e);
                    break;
                }
            }
        }
        count
    });

    // Chuẩn bị payload cho chế độ Download
    let download_payload = if mode == TestMode::Download {
        Some(Bytes::from(vec![0xBB; chunk_size])) 
    } else {
        None
    };

    // Xử lý các stream đồng thời
    let mut handles = Vec::new();
    while let Some((mut send, mut recv)) = rx.recv().await {
        let progress = progress.clone();
        let payload = download_payload.clone();

        handles.push(tokio::spawn(async move {
            
            // --- LOGIC CHÍNH ĐÃ SỬA ---
            if mode == TestMode::Upload {
                // Chế độ UPLOAD: Server đọc data từ client
                let mut remaining = chunk_size;
                while remaining > 0 {
                    match recv.read_chunk(remaining, true).await {
                        Ok(Some(chunk)) => {
                            let len = chunk.bytes.len();
                            remaining = remaining.saturating_sub(len);
                        }
                        Ok(None) => {
                            return Err(anyhow!("Stream kết thúc sớm sau {} bytes", chunk_size - remaining));
                        }
                        Err(e) => {
                            return Err(anyhow!("Lỗi đọc chunk: {:?}", e));
                        }
                    }
                }
                // Gửi FIN về client
                if let Err(e) = send.finish().await {
                    return Err(anyhow!("Không gửi được tín hiệu FIN về client: {:?}", e));
                }
            } else {
                // Chế độ DOWNLOAD: Server gửi data cho client
                // 1. Đợi client finish stream (tín hiệu "sẵn sàng nhận")
                match recv.read_chunk(1, true).await {
                    Ok(None) => { /* Client finished, good */ },
                    Ok(Some(chunk)) => if !chunk.bytes.is_empty() { 
                        return Err(anyhow!("Client gửi data trong download test?"));
                    }
                    Err(quinn::ReadError::ConnectionLost(_)) => { /* Client đóng là bình thường */ }
                    Err(e) => return Err(anyhow!("Lỗi đợi tín hiệu từ client: {:?}", e)),
                }
                
                // 2. Gửi payload
                let data = payload.context("Payload download bị thiếu")?;
                let mut offset = 0usize;
                while offset < data.len() {
                    let written = send.write(&data[offset..]).await?;
                    if written == 0 {
                        bail!("Ghi 0 byte vào stream");
                    }
                    offset += written;
                }
                send.finish().await?;
            }
            
            // Cập nhật progress (chung cho cả 2 mode)
            let current = progress.fetch_add(1, Ordering::Relaxed) + 1;
            if LOG_INTERVAL > 0 && current % LOG_INTERVAL == 0 {
                println!(
                    "{} đã xử lý {current}/{total_chunks} chunk ({}%)",
                    server_label,
                    (current * 100) / total_chunks
                );
            }
            if current == total_chunks {
                println!("{} ✅ HOÀN THÀNH: đã xử lý tất cả {total_chunks}/{total_chunks} chunk (100%)", server_label);
            }
            
            Ok(())
        }));
    }

    // Đợi task accept hoàn thành
    let _accepted_count = accept_handle.await.expect("Task accept bị panic");

    // Đợi tất cả stream hoàn thành
    let mut processed_chunks = 0;
    for handle in handles {
        handle.await.expect("Task server stream bị panic")?;
        processed_chunks += 1;
    }

    connection.close(0u32.into(), b"done");
    Ok(processed_chunks)
}

async fn run_client(
    transport: QuicTransport,
    addr: SocketAddr,
    num_connections: usize,
    total_chunks: usize,
    chunk_size: usize,
    concurrency: usize,
    mode: TestMode, // <-- Mới
) -> Result<Duration> {
    let mut raw_connections = Vec::with_capacity(num_connections);
    let client_label = if mode == TestMode::Upload { "[client-upload]" } else { "[client-download]" };

    for idx in 0..num_connections {
        let mut connection = transport
            .connect(addr)
            .await
            .map_err(|e| anyhow!("Không kết nối được tới server: {e}"))?;
        let quic_conn = connection
            .as_any_mut()
            .downcast_mut::<QuicConnection>()
            .context("Connection client không phải QuicConnection")?;
        let raw_conn = quic_conn.clone_connection();
        drop(connection);
        println!(
            "{} thiết lập kết nối {}/{} tới server",
            client_label,
            idx + 1,
            num_connections
        );
        raw_connections.push(raw_conn);
    }

    // Payload này chỉ dùng cho UPLOAD
    let upload_payload = if mode == TestMode::Upload {
        Some(Bytes::from(vec![0xAB; chunk_size]))
    } else {
        None
    };
    
    let semaphore = Arc::new(Semaphore::new(concurrency));
    let progress = Arc::new(AtomicUsize::new(0));

    let start = Instant::now();

    let mut handles = Vec::with_capacity(total_chunks);
    let assignments = distribute_chunks(total_chunks, num_connections);

    for (conn, chunk_count) in raw_connections.into_iter().zip(assignments.into_iter()) {
        for _ in 0..chunk_count {
            let semaphore = semaphore.clone();
            let conn = conn.clone();
            let data = upload_payload.clone(); // Sẽ là None nếu là Download
            let progress = progress.clone();
            handles.push(tokio::spawn(async move {
                let _permit = semaphore.acquire_owned().await.expect("Semaphore bị đóng");
                
                // Thay thế send_chunk bằng hàm mới
                let result = perform_chunk_transfer(conn, data, chunk_size, mode).await; 
                
                if result.is_ok() {
                    let current = progress.fetch_add(1, Ordering::Relaxed) + 1;
                    if LOG_INTERVAL > 0 && current % LOG_INTERVAL == 0 {
                        let action = if mode == TestMode::Upload { "gửi" } else { "tải" };
                        println!(
                            "{} đã {action} {current}/{total_chunks} chunk ({}%)",
                            client_label,
                            (current * 100) / total_chunks
                        );
                    }
                    if current == total_chunks {
                         let action = if mode == TestMode::Upload { "gửi" } else { "tải" };
                        println!("{} ✅ HOÀN THÀNH: đã {action} tất cả {total_chunks}/{total_chunks} chunk (100%)", client_label);
                    }
                }
                result
            }));
        }
    }

    let mut completed = 0;
    let mut errors = 0;
    for (idx, handle) in handles.into_iter().enumerate() {
        match handle.await {
            Ok(Ok(())) => {
                completed += 1;
            }
            Ok(Err(e)) => {
                errors += 1;
                eprintln!("{} Lỗi khi xử lý chunk {}: {:?}", client_label, idx + 1, e);
            }
            Err(e) => {
                errors += 1;
                eprintln!("{} Task {} bị panic: {:?}", client_label, idx + 1, e);
            }
        }
    }
    
    println!("{} Tổng kết: {} thành công, {} lỗi", client_label, completed, errors);
    if errors > 0 {
        bail!("Có {} lỗi khi {mode:?} chunk", errors);
    }

    let elapsed = start.elapsed();
    Ok(elapsed)
}

// Hàm này thay thế send_chunk, xử lý cả 2 chiều
async fn perform_chunk_transfer(
    connection: quinn::Connection,
    upload_data: Option<Bytes>, // Dùng cho Upload
    chunk_size: usize, // Dùng cho Download
    mode: TestMode,
) -> Result<()> {
    let (mut send, mut recv) = connection
        .open_bi()
        .await
        .context("Không mở được stream song công")?;

    if mode == TestMode::Upload {
        // --- LOGIC UPLOAD (code cũ của bạn) ---
        let data = upload_data.context("Payload upload bị thiếu")?;
        let mut offset = 0usize;
        while offset < data.len() {
            let written = send.write(&data[offset..]).await?;
            if written == 0 {
                bail!("Ghi 0 byte vào stream");
            }
            offset += written;
        }
        send.finish().await?;

        // Đợi FIN từ server
        match timeout(CHUNK_TIMEOUT, async {
            loop {
                match recv.read_chunk(1, true).await {
                    Ok(Some(chunk)) => {
                        if chunk.bytes.is_empty() {
                            break Ok(());
                        }
                    }
                    Ok(None) => break Ok(()),
                    Err(quinn::ReadError::ConnectionLost(err)) => match err {
                        quinn::ConnectionError::ApplicationClosed(_) => break Ok(()),
                        other => break Err(other.into()),
                    },
                    Err(err) => break Err(err.into()),
                }
            }
        })
        .await
        {
            Ok(result) => result,
            Err(_) => bail!("Timeout khi đợi FIN từ server (Upload) sau {} giây", CHUNK_TIMEOUT.as_secs()),
        }

    } else {
        // --- LOGIC DOWNLOAD (mới) ---
        // 1. Gửi tín hiệu "sẵn sàng nhận" bằng cách finish stream gửi
        send.finish().await.context("Không gửi được FIN (Download request)")?;

        // 2. Đọc data từ server
        let mut remaining = chunk_size;
        while remaining > 0 {
            match timeout(CHUNK_TIMEOUT, recv.read_chunk(remaining, true)).await {
                Ok(Ok(Some(chunk))) => {
                    let len = chunk.bytes.len();
                    remaining = remaining.saturating_sub(len);
                }
                Ok(Ok(None)) => {
                    if remaining == 0 {
                        break; // Đọc xong, server đóng stream, hoàn hảo
                    } else {
                        return Err(anyhow!("Stream (Download) kết thúc sớm, thiếu {} bytes", remaining));
                    }
                }
                Ok(Err(e)) => {
                    return Err(anyhow!("Lỗi đọc chunk (Download): {:?}", e));
                }
                Err(_) => {
                    // Timeout
                    return Err(anyhow!("Timeout khi đọc data (Download) sau {} giây", CHUNK_TIMEOUT.as_secs()));
                }
            }
        }
        Ok(())
    }
}


fn distribute_chunks(total: usize, workers: usize) -> Vec<usize> {
    let mut assignments = Vec::with_capacity(workers);
    if workers == 0 {
        return assignments;
    }

    let base = total / workers;
    let mut remainder = total % workers;

    for _ in 0..workers {
        let mut count = base;
        if remainder > 0 {
            count += 1;
            remainder -= 1;
        }
        assignments.push(count);
    }

    assignments
}

fn find_available_addr() -> Result<SocketAddr> {
    let socket = UdpSocket::bind("127.0.0.1:0").context("Không bind được UDP tạm thời")?;
    let addr = socket
        .local_addr()
        .context("Không lấy được địa chỉ cục bộ")?;
    Ok(addr)
}