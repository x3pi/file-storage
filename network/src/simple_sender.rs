// In network/src/simple_sender.rs

// Copyright(C) Facebook, Inc. and its affiliates.
use crate::error::NetworkError;
use crate::quic::QuicTransport;
use crate::transport::Transport;
use bytes::Bytes;
use log::{debug, info, warn};
use rand::prelude::SliceRandom as _;
use rand::rngs::SmallRng;
use rand::SeedableRng as _;
use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::time::{sleep, Duration};

#[cfg(test)]
#[path = "tests/simple_sender_tests.rs"]
pub mod simple_sender_tests;

pub struct SimpleSender {
    connections: HashMap<SocketAddr, Sender<Bytes>>,
    transport: QuicTransport,
    rng: SmallRng,
}

impl SimpleSender {
    pub fn new() -> Self {
        Self {
            connections: HashMap::new(),
            transport: QuicTransport::new(),
            rng: SmallRng::from_entropy(),
        }
    }

    fn spawn_connection_manager(address: SocketAddr, transport: QuicTransport) -> Sender<Bytes> {
        let (tx, rx) = channel(1_000);
        ConnectionManager::spawn(address, transport, rx);
        tx
    }

    pub async fn send(&mut self, address: SocketAddr, data: Bytes) {
        let transport_clone = self.transport.clone();
        let tx = self
            .connections
            .entry(address)
            .or_insert_with(|| Self::spawn_connection_manager(address, transport_clone));

        if tx.is_closed() {
            debug!("Connection for {} is closed, recreating.", address);
            let new_tx = Self::spawn_connection_manager(address, self.transport.clone());
            if let Err(e) = new_tx.send(data).await {
                warn!(
                    "Failed to send to newly created connection for {}: {}",
                    address, e
                );
            } else {
                self.connections.insert(address, new_tx);
            }
        } else if let Err(e) = tx.send(data).await {
            warn!(
                "Failed to send to {}: {}. Connection manager will retry.",
                address, e
            );
        }
    }

    pub async fn broadcast(&mut self, addresses: Vec<SocketAddr>, data: Bytes) {
        for address in addresses {
            self.send(address, data.clone()).await;
        }
    }

    pub async fn lucky_broadcast(
        &mut self,
        mut addresses: Vec<SocketAddr>,
        data: Bytes,
        nodes: usize,
    ) {
        addresses.shuffle(&mut self.rng);
        addresses.truncate(nodes);
        self.broadcast(addresses, data).await
    }
}

impl Default for SimpleSender {
    fn default() -> Self {
        Self::new()
    }
}

struct ConnectionManager {
    address: SocketAddr,
    transport: QuicTransport,
    receiver: Receiver<Bytes>,
}

impl ConnectionManager {
    fn spawn(address: SocketAddr, transport: QuicTransport, receiver: Receiver<Bytes>) {
        tokio::spawn(async move {
            Self {
                address,
                transport,
                receiver,
            }
            .run()
            .await;
        });
    }

    async fn run(&mut self) {
        let mut retry_delay = Duration::from_millis(200);
        let mut buffer = VecDeque::new();
        'main: loop {
            while let Ok(data) = self.receiver.try_recv() {
                buffer.push_back(data);
            }
            match self.transport.connect(self.address).await {
                Ok(mut connection) => {
                    info!("Outgoing connection established with {}", self.address);
                    retry_delay = Duration::from_millis(200);
                    while let Some(data) = buffer.pop_front() {
                        if let Err(e) = connection.send(data.clone()).await {
                            warn!(
                                "{}",
                                NetworkError::FailedToSendMessage(self.address, e.to_string())
                            );
                            buffer.push_front(data);
                            continue 'main;
                        }
                    }
                    while let Some(data) = self.receiver.recv().await {
                        if let Err(e) = connection.send(data.clone()).await {
                            warn!(
                                "{}",
                                NetworkError::FailedToSendMessage(self.address, e.to_string())
                            );
                            buffer.push_back(data);
                            continue 'main;
                        }
                    }
                    if self.receiver.is_closed() {
                        break;
                    }
                }
                Err(e) => {
                    warn!(
                        "Failed to connect to {}: {}. Retrying in {:?}...",
                        self.address, e, retry_delay
                    );
                    sleep(retry_delay).await;
                    retry_delay = Duration::from_millis(
                        (retry_delay.as_millis() * 2).clamp(200, 10_000) as u64,
                    );
                }
            }
        }
        warn!("Connection manager for {} is shutting down.", self.address);
    }
}
