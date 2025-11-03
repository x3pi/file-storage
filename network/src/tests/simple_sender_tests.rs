// Copyright(C) Facebook, Inc. and its affiliates.
use super::*;
use crate::common::listener;
use futures::future::try_join_all;

#[tokio::test]
async fn simple_send() {
    // Run a server.
    let address = "127.0.0.1:6100".parse::<SocketAddr>().unwrap();
    let message = "Hello, world!";
    let handle = listener(address, message.to_string());

    // Make the network sender and send the message.
    let mut sender = SimpleSender::new();
    
    // SỬA ĐỔI: Serialize tin nhắn bằng bincode trước khi gửi.
    let bytes = Bytes::from(bincode::serialize(message).unwrap());
    sender.send(address, bytes).await;

    // Ensure the server received the message (ie. it did not panic).
    assert!(handle.await.is_ok());
}

#[tokio::test]
async fn broadcast() {
    // Run 3 servers.
    let message = "Hello, world!";
    let (handles, addresses): (Vec<_>, Vec<_>) = (0..3)
        .map(|x| {
            let address = format!("127.0.0.1:{}", 6_200 + x)
                .parse::<SocketAddr>()
                .unwrap();
            (listener(address, message.to_string()), address)
        })
        .collect::<Vec<_>>()
        .into_iter()
        .unzip();

    // Make the network sender and send the message.
    let mut sender = SimpleSender::new();

    // SỬA ĐỔI: Serialize tin nhắn bằng bincode trước khi gửi.
    let bytes = Bytes::from(bincode::serialize(message).unwrap());
    sender.broadcast(addresses, bytes).await;

    // Ensure all servers received the broadcast.
    assert!(try_join_all(handles).await.is_ok());
}