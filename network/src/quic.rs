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
use std::fs;
use std::io::BufReader;
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
            Some(Ok(bytes)) => {
                log::trace!("📥 QuicStreamHandler received {} bytes", bytes.len());
                Ok(Some(bytes.freeze()))
            }
            Some(Err(e)) => {
                log::error!("❌ QuicStreamHandler recv error: {}", e);
                Err(Box::new(e))
            }
            None => {
                Ok(None)
            }
        }
    }
    // Hàm để gửi 1 response VÀ GIỮ MỞ stream để tái sử dụng
    pub async fn send(&mut self, data: Bytes) -> TransportResult<()> {
        log::trace!("📤 QuicStreamHandler sending {} bytes", data.len());
        self.framed.send(data).await?;
        // Đã xóa self.framed.close().await? để cho phép tái sử dụng stream
        log::trace!("✅ QuicStreamHandler send completed");
        Ok(())
    }

    pub async fn close(&mut self) -> TransportResult<()> {
        self.framed.close().await?;
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
    // SỬA LẠI: Dùng match để xử lý `None`
    let connecting = match self.listener.accept().await {
        Some(conn) => conn, // OK, có kết nối
        None => {
            // Endpoint đã bị đóng (do lỗi hoặc server tắt)
            // Trả về một lỗi rõ ràng thay vì panic
            log::error!("__-QUIC listener endpoint closed. Stopping accept loop." );
            return Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::ConnectionAborted,
                "Listener endpoint closed",
            )));
        }
    };

    // Chỉ chạy tiếp nếu 'connecting' là Some
    let connection = connecting.await?; 
    let addr = connection.remote_address();
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
        Self::new_with_certs(None, None)
    }

    /// Tạo QuicTransport với certificate và private key từ file
    /// 
    /// # Arguments
    /// * `cert_path` - Đường dẫn đến file certificate (PEM format). Nếu None, sẽ tự động tạo certificate.
    /// * `key_path` - Đường dẫn đến file private key (PEM format). Nếu None, sẽ tự động tạo key.
    pub fn new_with_certs(cert_path: Option<&str>, key_path: Option<&str>) -> Self {
        let (server_config, client_config) = configure_certificates(cert_path, key_path);
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

// --- Cấu hình chứng chỉ & Hiệu năng ---
fn configure_certificates(
    cert_path: Option<&str>,
    key_path: Option<&str>,
) -> (ServerConfig, ClientConfig) {
    let (cert_der, priv_key_der): (Vec<u8>, Vec<u8>) = if let (Some(cert_file), Some(key_file)) =
        (cert_path, key_path)
    {
        // Load certificate từ file PEM
        log::info!("Loading certificate from file: {}", cert_file);
        log::info!("Loading private key from file: {}", key_file);

        let cert_pem = fs::read_to_string(cert_file)
            .unwrap_or_else(|e| panic!("Failed to read certificate file {}: {}", cert_file, e));
        let key_pem = fs::read_to_string(key_file)
            .unwrap_or_else(|e| panic!("Failed to read private key file {}: {}", key_file, e));

        // Parse certificate PEM
        let cert_chain_bytes = cert_pem.as_bytes();
        let certs = rustls_pemfile::certs(&mut BufReader::new(cert_chain_bytes))
            .expect("Failed to parse certificate PEM");
        
        if certs.is_empty() {
            panic!("No certificates found in certificate file: {}", cert_file);
        }
        let cert_der = certs[0].clone();

        // Parse private key PEM - thử PKCS8 trước, sau đó thử PKCS1 (RSA)
        let key_bytes = key_pem.as_bytes();
        let mut keys = rustls_pemfile::pkcs8_private_keys(&mut BufReader::new(key_bytes))
            .unwrap_or_else(|e| {
                log::warn!("Failed to parse private key as PKCS8, trying PKCS1: {}", e);
                vec![]
            });

        if keys.is_empty() {
            // Thử PKCS1 format (RSA)
            keys = rustls_pemfile::rsa_private_keys(&mut BufReader::new(key_bytes))
                .unwrap_or_else(|e| {
                    panic!(
                        "Failed to parse private key PEM (tried PKCS8 and PKCS1): {}",
                        e
                    )
                });
        }

        if keys.is_empty() {
            panic!("No private keys found in key file: {}", key_file);
        }

        log::info!("Successfully loaded certificate and private key from files");
        (cert_der, keys[0].clone())
    } else {
        // Fallback: Tự động tạo certificate (giữ nguyên logic cũ)
        log::info!("No certificate files provided, generating self-signed certificate");
        let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        let cert_der = cert.serialize_der().unwrap();
        let priv_key = cert.serialize_private_key_der();
        (cert_der, priv_key)
    };

    let priv_key_rustls = rustls::PrivateKey(priv_key_der);
    let cert_chain = vec![rustls::Certificate(cert_der)];
    // ✅ ALPN Protocol - Quan trọng cho Android/iOS compatibility
    let alpn_protocols = vec![b"file-storage-v1".to_vec()];

    let mut transport_config = TransportConfig::default();
    transport_config.max_concurrent_uni_streams(VarInt::from_u32(10_000));
    transport_config.max_concurrent_bidi_streams(VarInt::from_u32(10_000));
    const MAX_STREAM_WINDOW: u32 = 20 * 1024 * 1024;
    const MAX_CONN_WINDOW: u32 = 128 * 1024 * 1024;
    transport_config.stream_receive_window(VarInt::from_u32(MAX_STREAM_WINDOW));
    transport_config.receive_window(VarInt::from_u32(MAX_CONN_WINDOW));
    transport_config.send_window((MAX_CONN_WINDOW as u64).into());
    transport_config.max_idle_timeout(Some(Duration::from_secs(90).try_into().unwrap()));
    transport_config.keep_alive_interval(Some(Duration::from_secs(10)));
    let transport = Arc::new(transport_config);

    // ✅ Server Config với ALPN
    let mut server_crypto = rustls::ServerConfig::builder()
        .with_safe_defaults()
        .with_no_client_auth()
        .with_single_cert(cert_chain.clone(), priv_key_rustls.clone())
        .unwrap();
    server_crypto.alpn_protocols = alpn_protocols.clone();
    
    let mut server_config = ServerConfig::with_crypto(Arc::new(server_crypto));
    server_config.transport = transport.clone();

    // ✅ Client Config với ALPN
    let mut client_crypto = rustls::ClientConfig::builder()
        .with_safe_defaults()
        .with_custom_certificate_verifier(Arc::new(SkipServerVerification))
        .with_no_client_auth();
    client_crypto.alpn_protocols = alpn_protocols;
    
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
