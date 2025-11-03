// Copyright(C) Facebook, Inc. and its affiliates.
use crate::quic::QuicTransport;
use crate::transport::Transport;
use bytes::Bytes;
use std::net::SocketAddr;
use tokio::task::JoinHandle;

pub fn listener(address: SocketAddr, expected: String) -> JoinHandle<()> {
    tokio::spawn(async move {
        let transport = QuicTransport::new();
        let mut listener = transport.listen(address).await.unwrap();
        let (mut connection, _) = listener.accept().await.unwrap();

        match connection.recv().await {
            Ok(Some(received)) => {
                let message: String = bincode::deserialize(&received).unwrap();
                assert_eq!(message, expected);
                connection.send(Bytes::from("Ack")).await.unwrap();
            }
            _ => panic!("Failed to receive network message"),
        }
    })
}