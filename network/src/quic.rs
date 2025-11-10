// network/src/quic.rs

use super::transport::{Connection, Listener, Transport, TransportResult};
use async_trait::async_trait;
use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use quinn::{
    ClientConfig, Endpoint, RecvStream, SendStream, ServerConfig, TransportConfig, VarInt,
};
use std::any::Any; // ✅ THÊM
use std::convert::TryInto;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

// --- Triển khai Stream (Giữ nguyên) ---
struct QuicStream {
    sender: SendStream,
    receiver: RecvStream,
}
impl tokio::io::AsyncRead for QuicStream {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.receiver).poll_read(cx, buf)
    }
}
impl tokio::io::AsyncWrite for QuicStream {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<Result<usize, std::io::Error>> {
        std::pin::Pin::new(&mut self.sender).poll_write(cx, buf)
    }
    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        std::pin::Pin::new(&mut self.sender).poll_flush(cx)
    }
    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        std::pin::Pin::new(&mut self.sender).poll_shutdown(cx)
    }
}

// --- Triển khai Connection ---

// 🔥 THAY ĐỔI: Struct này đại diện cho MỘT STREAM (1 request)
pub struct QuicStreamHandler {
    framed: Framed<QuicStream, LengthDelimitedCodec>,
}

impl QuicStreamHandler {
    // Hàm để đọc 1 request
    pub async fn recv(&mut self) -> TransportResult<Option<Bytes>> {
        match self.framed.next().await {
            Some(Ok(bytes)) => Ok(Some(bytes.freeze())),
            Some(Err(e)) => Err(Box::new(e)),
            None => Ok(None),
        }
    }
    // Hàm để gửi 1 response VÀ ĐÓNG stream
    pub async fn send(&mut self, data: Bytes) -> TransportResult<()> {
        self.framed.send(data).await?;
        // ✅ CHỈ flush, KHÔNG chờ close → task kết thúc nhanh
        // Stream sẽ tự động close khi drop QuicStreamHandler
        self.framed.flush().await?;
        Ok(())
    }
}

// 🔥 THAY ĐỔI: Struct này đại diện cho KẾT NỐI (Connection)
pub struct QuicConnection {
    connection: quinn::Connection,
}

impl QuicConnection {
    // 🔥 THAY ĐỔI: Hàm mới để chấp nhận MỘT stream
    pub async fn accept_stream(&mut self) -> TransportResult<QuicStreamHandler> {
        // println!("🔌 [QuicConnection] Đang chờ client mở stream mới...");
        let (sender, receiver) = match self.connection.accept_bi().await {
            Ok(streams) => streams,
            Err(quinn::ConnectionError::ApplicationClosed(_))
            | Err(quinn::ConnectionError::LocallyClosed) => {
                return Err(Box::new(std::io::Error::new(
                    std::io::ErrorKind::ConnectionAborted,
                    "Connection closed",
                )));
            }
            Err(e) => {
                return Err(Box::new(e));
            }
        };
        // println!("✅ [QuicConnection] Đã chấp nhận stream mới từ client!");

        let stream = QuicStream { sender, receiver };
        let framed = Framed::new(stream, LengthDelimitedCodec::new());

        Ok(QuicStreamHandler { framed })
    }

    pub fn clone_connection(&self) -> quinn::Connection {
        self.connection.clone()
    }
}

// 🔥 THAY ĐỔI: Impl 'Connection' trait
#[async_trait]
impl Connection for QuicConnection {
    // Các hàm này không còn được dùng ở cấp Connection nữa
    async fn send(&mut self, _data: Bytes) -> TransportResult<()> {
        unimplemented!("Không dùng: send() ở cấp Connection. Gọi qua QuicStreamHandler.")
    }
    async fn recv(&mut self) -> TransportResult<Option<Bytes>> {
        unimplemented!("Không dùng: recv() ở cấp Connection. Gọi qua QuicStreamHandler.")
    }
    // ✅ THÊM: Impl cho hàm 'as_any_mut'
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

// --- Triển khai Listener (Sửa đổi) ---
pub struct QuicListener {
    listener: Endpoint,
}
#[async_trait]
impl Listener for QuicListener {
    // 🔥 THAY ĐỔI: `accept` giờ chỉ chấp nhận KẾT NỐI (Connection)
    async fn accept(&mut self) -> TransportResult<(Box<dyn Connection>, SocketAddr)> {
        let connecting = self.listener.accept().await.unwrap();

        let connection = connecting.await?;
        let addr = connection.remote_address();

        // Trả về đối tượng QuicConnection
        let conn = Box::new(QuicConnection { connection });
        Ok((conn, addr))
    }
}

// --- Triển khai Transport (Sửa đổi) ---
#[derive(Clone)]
pub struct QuicTransport {
    client_endpoint: Arc<Endpoint>,
    server_config: ServerConfig,
    client_config: ClientConfig,
}
impl QuicTransport {
    pub fn new() -> Self {
        let (server_config, client_config) = configure_certificates();
        let mut endpoint = Endpoint::client("0.0.0.0:0".parse().unwrap()).unwrap();
        endpoint.set_default_client_config(client_config.clone());
        Self {
            client_endpoint: Arc::new(endpoint),
            server_config,
            client_config,
        }
    }
    pub fn get_client_config(&self) -> ClientConfig {
        self.client_config.clone()
    }
}
impl Default for QuicTransport {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Transport for QuicTransport {
    async fn connect(&self, address: SocketAddr) -> TransportResult<Box<dyn Connection>> {
        let connection = self.client_endpoint.connect(address, "localhost")?.await?;
        // Trả về connection, không mở stream
        Ok(Box::new(QuicConnection { connection }))
    }

    async fn listen(&self, address: SocketAddr) -> TransportResult<Box<dyn Listener>> {
        let listener = Endpoint::server(self.server_config.clone(), address)?;
        Ok(Box::new(QuicListener { listener }))
    }
}

// --- Cấu hình chứng chỉ & Hiệu năng (Giữ nguyên từ code của bạn) ---
fn configure_certificates() -> (ServerConfig, ClientConfig) {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let cert_der = cert.serialize_der().unwrap();
    let priv_key = cert.serialize_private_key_der();
    let priv_key = rustls::PrivateKey(priv_key);
    let cert_chain = vec![rustls::Certificate(cert_der.clone())];

    let mut transport_config = TransportConfig::default();
    transport_config.max_concurrent_uni_streams(VarInt::from_u32(100_000));
    transport_config.max_concurrent_bidi_streams(VarInt::from_u32(100_000));
    const MAX_STREAM_WINDOW: u32 = 20 * 1024 * 1024;
    const MAX_CONN_WINDOW: u32 = 40 * 1024 * 1024;
    transport_config.stream_receive_window(VarInt::from_u32(MAX_STREAM_WINDOW));
    transport_config.receive_window(VarInt::from_u32(MAX_CONN_WINDOW));
    transport_config.send_window((MAX_CONN_WINDOW as u64).into());
    transport_config.max_idle_timeout(Some(Duration::from_secs(60).try_into().unwrap()));
    transport_config.keep_alive_interval(Some(Duration::from_secs(5)));
    let transport = Arc::new(transport_config);

    let mut server_config = ServerConfig::with_single_cert(cert_chain, priv_key).unwrap();
    server_config.transport = transport.clone();

    let client_crypto = rustls::ClientConfig::builder()
        .with_safe_defaults()
        .with_custom_certificate_verifier(Arc::new(SkipServerVerification))
        .with_no_client_auth();
    let mut client_config = ClientConfig::new(Arc::new(client_crypto));
    client_config.transport_config(transport);

    (server_config, client_config)
}

struct SkipServerVerification;
impl rustls::client::ServerCertVerifier for SkipServerVerification {
    fn verify_server_cert(
        &self,
        _: &rustls::Certificate,
        _: &[rustls::Certificate],
        _: &rustls::ServerName,
        _: &mut dyn Iterator<Item = &[u8]>,
        _: &[u8],
        _: std::time::SystemTime,
    ) -> Result<rustls::client::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::ServerCertVerified::assertion())
    }
}
