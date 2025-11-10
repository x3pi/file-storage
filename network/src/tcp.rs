// // In network/src/tcp.rs

// use super::transport::{Connection, Listener, Transport, TransportResult};
// use async_trait::async_trait;
// use bytes::Bytes;
// use futures::stream::StreamExt;
// use std::net::SocketAddr;
// use tokio::net::{TcpListener, TcpStream};
// use tokio_util::codec::{Framed, LengthDelimitedCodec};

// // --- Triển khai cho một kết nối TCP ---
// pub struct TcpConnection {
//     framed: Framed<TcpStream, LengthDelimitedCodec>,
// }

// #[async_trait]
// impl Connection for TcpConnection {
//     async fn send(&mut self, data: Bytes) -> TransportResult<()> {
//         futures::SinkExt::send(&mut self.framed, data).await?;
//         Ok(())
//     }

//     async fn recv(&mut self) -> TransportResult<Option<Bytes>> {
//         match self.framed.next().await {
//             Some(Ok(bytes)) => Ok(Some(bytes.freeze())),
//             Some(Err(e)) => Err(Box::new(e)),
//             None => Ok(None),
//         }
//     }
// }

// // --- Triển khai cho bộ lắng nghe TCP ---
// pub struct TcpListenerAdapter {
//     listener: TcpListener,
// }

// #[async_trait]
// impl Listener for TcpListenerAdapter {
//     async fn accept(&mut self) -> TransportResult<(Box<dyn Connection>, SocketAddr)> {
//         let (socket, addr) = self.listener.accept().await?;
//         let framed = Framed::new(socket, LengthDelimitedCodec::new());
//         let conn = Box::new(TcpConnection { framed });
//         Ok((conn, addr))
//     }
// }

// // --- Triển khai chính cho giao vận TCP ---
// // SỬA LỖI: Thêm `Clone` vào derive macro.
// #[derive(Default, Clone)]
// pub struct TcpTransport;

// impl TcpTransport {
//     pub fn new() -> Self {
//         Self
//     }
// }

// #[async_trait]
// impl Transport for TcpTransport {
//     async fn connect(&self, address: SocketAddr) -> TransportResult<Box<dyn Connection>> {
//         let stream = TcpStream::connect(address).await?;
//         let framed = Framed::new(stream, LengthDelimitedCodec::new());
//         Ok(Box::new(TcpConnection { framed }))
//     }

//     async fn listen(&self, address: SocketAddr) -> TransportResult<Box<dyn Listener>> {
//         let listener = TcpListener::bind(address).await?;
//         Ok(Box::new(TcpListenerAdapter { listener }))
//     }
// }
