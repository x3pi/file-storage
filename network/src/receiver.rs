// Copyright(C) Facebook, Inc. and its affiliates.
use crate::transport::{Connection, Listener};
use async_trait::async_trait;
use bytes::Bytes;
use log::{debug, info, warn};
use std::error::Error;
use std::net::SocketAddr;

#[cfg(test)]
#[path = "tests/receiver_tests.rs"]
pub mod receiver_tests;

/// Writer bây giờ là một đối tượng Connection trừu tượng, có thể gửi dữ liệu.
pub type Writer = Box<dyn Connection + Send>;

#[async_trait]
pub trait MessageHandler: Clone + Send + Sync + 'static {
    async fn dispatch(&self, writer: &mut Writer, message: Bytes) -> Result<(), Box<dyn Error>>;
}

pub struct Receiver<Handler: MessageHandler> {
    listener: Box<dyn Listener + Send>,
    handler: Handler,
}

impl<Handler: MessageHandler> Receiver<Handler> {
    // Chữ ký này bây giờ khớp với trait Listener đã được cập nhật (Listener: Send)
    pub fn spawn(listener: Box<dyn Listener + Send>, handler: Handler) {
        tokio::spawn(async move {
            Self { listener, handler }.run().await;
        });
    }

    async fn run(&mut self) {
        debug!("Listening for incoming connections...");
        loop {
            let (connection, peer) = match self.listener.accept().await {
                Ok(value) => value,
                Err(e) => {
                    warn!("Failed to accept connection: {}", e);
                    continue;
                }
            };
            info!("Incoming connection established with {}", peer);
            Self::spawn_runner(connection, peer, self.handler.clone()).await;
        }
    }

    async fn spawn_runner(mut connection: Box<dyn Connection + Send>, peer: SocketAddr, handler: Handler) {
        tokio::spawn(async move {
            loop {
                match connection.recv().await {
                    Ok(Some(message)) => {
                        let mut writer: Writer = connection;
                        if let Err(e) = handler.dispatch(&mut writer, message).await {
                            warn!("{}", e);
                            return;
                        }
                        connection = writer;
                    }
                    Ok(None) => {
                        // Kết nối đã đóng
                        break;
                    }
                    Err(e) => {
                        warn!("Failed to receive message from {}: {}", peer, e);
                        return;
                    }
                }
            }
            warn!("Connection closed by peer {}", peer);
        });
    }
}