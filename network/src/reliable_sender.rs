// Copyright(C) Facebook, Inc. and its affiliates.
use crate::error::NetworkError;
use crate::quic::QuicTransport;
use crate::transport::{Connection, Transport};
use bytes::Bytes;
use log::{info, warn};
use rand::prelude::SliceRandom as _;
use rand::rngs::SmallRng;
use rand::SeedableRng as _;
use std::cmp::min;
use std::collections::{HashMap, VecDeque};
use std::fmt::Debug;
use std::net::SocketAddr;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::oneshot;
use tokio::time::{sleep, Duration};

#[cfg(test)]
#[path = "tests/reliable_sender_tests.rs"]
pub mod reliable_sender_tests;

pub type CancelHandler = oneshot::Receiver<Bytes>;

pub struct ReliableSender {
    connections: HashMap<SocketAddr, Sender<InnerMessage>>,
    transport: QuicTransport,
    rng: SmallRng,
}

impl ReliableSender {
    pub fn new() -> Self {
        Self {
            connections: HashMap::new(),
            transport: QuicTransport::new(),
            rng: SmallRng::from_entropy(),
        }
    }

    fn spawn_connection(address: SocketAddr, transport: QuicTransport) -> Sender<InnerMessage> {
        let (tx, rx) = channel(1_000);
        ConnectionManager::spawn(address, transport, rx);
        tx
    }

    pub async fn send(&mut self, address: SocketAddr, data: Bytes) -> CancelHandler {
        let (sender, receiver) = oneshot::channel();
        let transport = self.transport.clone();
        self.connections
            .entry(address)
            .or_insert_with(|| Self::spawn_connection(address, transport))
            .send(InnerMessage {
                data,
                cancel_handler: sender,
            })
            .await
            .expect("Failed to send internal message");
        receiver
    }

    pub async fn broadcast(
        &mut self,
        addresses: Vec<SocketAddr>,
        data: Bytes,
    ) -> Vec<CancelHandler> {
        let mut handlers = Vec::new();
        for address in addresses {
            let handler = self.send(address, data.clone()).await;
            handlers.push(handler);
        }
        handlers
    }

    pub async fn lucky_broadcast(
        &mut self,
        mut addresses: Vec<SocketAddr>,
        data: Bytes,
        nodes: usize,
    ) -> Vec<CancelHandler> {
        addresses.shuffle(&mut self.rng);
        addresses.truncate(nodes);
        self.broadcast(addresses, data).await
    }
}

impl Default for ReliableSender {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug)]
struct InnerMessage {
    data: Bytes,
    cancel_handler: oneshot::Sender<Bytes>,
}

struct ConnectionManager {
    address: SocketAddr,
    transport: QuicTransport,
    receiver: Receiver<InnerMessage>,
    buffer: VecDeque<(Bytes, oneshot::Sender<Bytes>)>,
}

impl ConnectionManager {
    fn spawn(address: SocketAddr, transport: QuicTransport, receiver: Receiver<InnerMessage>) {
        tokio::spawn(async move {
            Self {
                address,
                transport,
                receiver,
                buffer: VecDeque::new(),
            }
            .run()
            .await;
        });
    }

    async fn run(&mut self) {
        let mut retry_delay = Duration::from_millis(200);
        loop {
            while let Ok(InnerMessage {
                data,
                cancel_handler,
            }) = self.receiver.try_recv()
            {
                if !cancel_handler.is_closed() {
                    self.buffer.push_back((data, cancel_handler));
                }
            }
            match self.transport.connect(self.address).await {
                Ok(connection) => {
                    info!("Outgoing connection established with {}", self.address);
                    retry_delay = Duration::from_millis(200);
                    let e = self.keep_alive(connection).await;
                    warn!("{}", e);
                }
                Err(e) => {
                    warn!(
                        "Failed to connect to {}: {}. Retrying in {:?}...",
                        self.address, e, retry_delay
                    );
                    sleep(retry_delay).await;
                    retry_delay = min(retry_delay * 2, Duration::from_secs(60));
                }
            }
        }
    }

    async fn keep_alive(&mut self, mut connection: Box<dyn Connection>) -> NetworkError {
        let mut pending_replies = VecDeque::new();
        while let Some((data, handler)) = self.buffer.pop_front() {
            if handler.is_closed() {
                continue;
            }
            match connection.send(data.clone()).await {
                Ok(()) => pending_replies.push_back((data, handler)),
                Err(e) => {
                    self.buffer.push_front((data, handler));
                    return NetworkError::FailedToSendMessage(self.address, e.to_string());
                }
            }
        }
        loop {
            tokio::select! {
                Some(InnerMessage{data, cancel_handler}) = self.receiver.recv() => {
                    if cancel_handler.is_closed() { continue; }
                    match connection.send(data.clone()).await {
                        Ok(()) => pending_replies.push_back((data, cancel_handler)),
                        Err(e) => {
                            self.buffer.push_front((data, cancel_handler));
                            return NetworkError::FailedToSendMessage(self.address, e.to_string());
                        }
                    }
                },
                response = connection.recv() => {
                    // SỬA LỖI: Lấy cả data và handler ra khỏi hàng đợi.
                    let (data, handler) = match pending_replies.pop_front() {
                        Some(message) => message,
                        None => return NetworkError::UnexpectedAck(self.address)
                    };
                    match response {
                        Ok(Some(bytes)) => { let _ = handler.send(bytes); },
                        _ => {
                            // SỬA LỖI: Đưa lại tin nhắn gốc (data) vào buffer, không phải tin nhắn rỗng.
                            self.buffer.push_front((data, handler));
                            while let Some(pending) = pending_replies.pop_back() {
                                self.buffer.push_front(pending);
                            }
                            return NetworkError::FailedToReceiveAck(self.address);
                        }
                    }
                },
            }
        }
    }
}
