// In network/src/transport.rs

use async_trait::async_trait;
use bytes::Bytes;
use std::any::Any;
use std::error::Error;
use std::net::SocketAddr;

/// Lỗi chung cho tầng giao vận.
pub type TransportResult<T> = Result<T, Box<dyn Error + Send + Sync + 'static>>;

/// Đại diện cho một kết nối hai chiều.
#[async_trait]
pub trait Connection: Send {
    async fn send(&mut self, data: Bytes) -> TransportResult<()>;
    async fn recv(&mut self) -> TransportResult<Option<Bytes>>;
    // ✅ THÊM HÀM NÀY
    /// Cho phép downcasting về kiểu cụ thể (như QuicConnection)
    fn as_any_mut(&mut self) -> &mut dyn Any;
}

/// Lắng nghe các kết nối đến.
#[async_trait]
pub trait Listener: Send {
    async fn accept(&mut self) -> TransportResult<(Box<dyn Connection>, SocketAddr)>;
}

/// Giao diện chính để tạo kết nối và listener.
#[async_trait]
pub trait Transport: Send + Sync {
    async fn connect(&self, address: SocketAddr) -> TransportResult<Box<dyn Connection>>;
    async fn listen(&self, address: SocketAddr) -> TransportResult<Box<dyn Listener>>;
}
