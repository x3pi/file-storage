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
const TOTAL_CHUNKS: usize = 1000;
const STREAM_CONCURRENCY_PER_CONN: usize = 10;
const CONCURRENCY: usize = NUM_CONNECTIONS * STREAM_CONCURRENCY_PER_CONN;
const LOG_INTERVAL: usize = 50;
const CHUNK_TIMEOUT: Duration = Duration::from_secs(30); // Timeout cho mỗi chunk

#[tokio::main]
async fn main() -> Result<()> {
    let addr = find_available_addr().context("Không tìm được cổng trống cho server")?;
    let expected_bytes = CHUNK_SIZE * TOTAL_CHUNKS;

    let base_transport = QuicTransport::new();
    let server_transport = base_transport.clone();
    let client_transport = base_transport;

    let (done_tx, done_rx) = oneshot::channel();

    tokio::spawn(async move {
        if let Err(e) = run_server(
            server_transport,
            addr,
            NUM_CONNECTIONS,
            TOTAL_CHUNKS,
            CHUNK_SIZE,
            done_tx,
        )
        .await
        {
            eprintln!("[server] lỗi: {e:?}");
        }
    });

    // Đợi server bind xong
    sleep(Duration::from_millis(200)).await;

    let elapsed = run_client(
        client_transport,
        addr,
        NUM_CONNECTIONS,
        TOTAL_CHUNKS,
        CHUNK_SIZE,
        CONCURRENCY,
    )
    .await?;

    // Đảm bảo server kết thúc sạch sẽ
    let _ = done_rx.await;

    let total_mb = expected_bytes as f64 / (1024.0 * 1024.0);
    let throughput = total_mb / elapsed.as_secs_f64();
    let per_chunk_ms = elapsed.as_secs_f64() * 1000.0 / TOTAL_CHUNKS as f64;
    let chunks_per_second = TOTAL_CHUNKS as f64 / elapsed.as_secs_f64();

    println!("---- QUIC chunk benchmark ----");
    println!("Số kết nối: {NUM_CONNECTIONS}");
    println!("Số chunk: {TOTAL_CHUNKS}");
    println!("Kích thước mỗi chunk: {CHUNK_SIZE} bytes");
    println!("Độ đồng thời: {CONCURRENCY}");
    println!("Thời gian: {:.3} giây", elapsed.as_secs_f64());
    println!("Thông lượng: {:.2} MiB/s", throughput);
    println!("Tốc độ xử lý: {:.2} chunk/giây", chunks_per_second);
    println!("Thời gian trung bình mỗi chunk: {:.3} ms", per_chunk_ms);

    Ok(())
}

async fn run_server(
    transport: QuicTransport,
    addr: SocketAddr,
    num_connections: usize,
    total_chunks: usize,
    chunk_size: usize,
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

    for (idx, expected_chunks) in assignments.into_iter().enumerate() {
        let (mut connection, peer) = listener
            .accept()
            .await
            .map_err(|e| anyhow!("Không accept được kết nối: {e}"))?;
        println!(
            "[server] nhận kết nối {}/{} từ {}",
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
            )
            .await
        }));
    }

    let mut total_received = 0usize;
    for handle in handles {
        total_received += handle.await.expect("Task server bị panic")?;
    }

    println!("[server] hoàn tất nhận {} chunk", total_received);
    let _ = done.send(());

    Ok(())
}

async fn run_server_connection(
    connection: quinn::Connection,
    expected_chunks: usize,
    chunk_size: usize,
    total_chunks: usize,
    progress: Arc<AtomicUsize>,
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
                    eprintln!("[server] Lỗi accept stream: {:?}", e);
                    break;
                }
            }
        }
        count
    });

    // Xử lý các stream đồng thời
    let mut handles = Vec::new();
    while let Some((mut send, mut recv)) = rx.recv().await {
        let chunk_size = chunk_size;
        let progress = progress.clone();
        let total_chunks = total_chunks;
        
        handles.push(tokio::spawn(async move {
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
            
            // Cập nhật progress
            let current = progress.fetch_add(1, Ordering::Relaxed) + 1;
            if LOG_INTERVAL > 0 && current % LOG_INTERVAL == 0 {
                println!(
                    "[server] đã nhận {current}/{total_chunks} chunk ({}%)",
                    (current * 100) / total_chunks
                );
            }
            // Đảm bảo log khi hoàn thành tất cả
            if current == total_chunks {
                println!("[server] ✅ HOÀN THÀNH: đã nhận tất cả {total_chunks}/{total_chunks} chunk (100%)");
            }
            
            Ok(())
        }));
    }

    // Đợi task accept hoàn thành
    let _accepted_count = accept_handle.await.expect("Task accept bị panic");
    
    // Đợi tất cả stream hoàn thành
    let mut received_chunks = 0;
    for handle in handles {
        handle.await.expect("Task server stream bị panic")?;
        received_chunks += 1;
    }

    connection.close(0u32.into(), b"done");
    Ok(received_chunks)
}

async fn run_client(
    transport: QuicTransport,
    addr: SocketAddr,
    num_connections: usize,
    total_chunks: usize,
    chunk_size: usize,
    concurrency: usize,
) -> Result<Duration> {
    let mut raw_connections = Vec::with_capacity(num_connections);

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
            "[client] thiết lập kết nối {}/{} tới server",
            idx + 1,
            num_connections
        );
        raw_connections.push(raw_conn);
    }

    let payload = Bytes::from(vec![0xAB; chunk_size]);
    let semaphore = Arc::new(Semaphore::new(concurrency));
    let progress = Arc::new(AtomicUsize::new(0));

    let start = Instant::now();

    let mut handles = Vec::with_capacity(total_chunks);
    let assignments = distribute_chunks(total_chunks, num_connections);

    for (conn, chunk_count) in raw_connections.into_iter().zip(assignments.into_iter()) {
        for _ in 0..chunk_count {
            let semaphore = semaphore.clone();
            let conn = conn.clone();
            let data = payload.clone();
            let progress = progress.clone();
            handles.push(tokio::spawn(async move {
                let _permit = semaphore.acquire_owned().await.expect("Semaphore bị đóng");
                let result = send_chunk(conn, data).await;
                if result.is_ok() {
                    let current = progress.fetch_add(1, Ordering::Relaxed) + 1;
                    if LOG_INTERVAL > 0 && current % LOG_INTERVAL == 0 {
                        println!(
                            "[client] đã gửi {current}/{total_chunks} chunk ({}%)",
                            (current * 100) / total_chunks
                        );
                    }
                    // Đảm bảo log khi hoàn thành tất cả
                    if current == total_chunks {
                        println!("[client] ✅ HOÀN THÀNH: đã gửi tất cả {total_chunks}/{total_chunks} chunk (100%)");
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
                eprintln!("[client] Lỗi khi gửi chunk {}: {:?}", idx + 1, e);
            }
            Err(e) => {
                errors += 1;
                eprintln!("[client] Task {} bị panic: {:?}", idx + 1, e);
            }
        }
    }
    
    println!("[client] Tổng kết: {} thành công, {} lỗi", completed, errors);
    if errors > 0 {
        bail!("Có {} lỗi khi gửi chunk", errors);
    }

    let elapsed = start.elapsed();
    Ok(elapsed)
}

async fn send_chunk(connection: quinn::Connection, data: Bytes) -> Result<()> {
    let (mut send, mut recv) = connection
        .open_bi()
        .await
        .context("Không mở được stream song công")?;

    let mut offset = 0usize;
    while offset < data.len() {
        let written = send.write(&data[offset..]).await?;
        if written == 0 {
            bail!("Ghi 0 byte vào stream");
        }
        offset += written;
    }
    send.finish().await?;

    // Đợi peer đọc xong để tránh đóng stream quá sớm (với timeout)
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
        Err(_) => bail!("Timeout khi đợi FIN từ server sau {} giây", CHUNK_TIMEOUT.as_secs()),
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
