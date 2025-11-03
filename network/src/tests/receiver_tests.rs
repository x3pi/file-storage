// Copyright(C) Facebook, Inc. and its affiliates.
use super::*;
use tokio::sync::mpsc::channel;
use tokio::sync::mpsc::Sender;
use tokio::time::{sleep, Duration};

// SỬA ĐỔI: Import các thành phần cần thiết từ lớp giao vận mới.
use crate::quic::QuicTransport;
use crate::transport::Transport;

#[derive(Clone)]
struct TestHandler {
    deliver: Sender<String>,
}

#[async_trait]
impl MessageHandler for TestHandler {
    async fn dispatch(&self, writer: &mut Writer, message: Bytes) -> Result<(), Box<dyn Error>> {
        // Reply with an ACK.
        let _ = writer.send(Bytes::from("Ack")).await;

        // Deserialize the message.
        let message = bincode::deserialize(&message).unwrap();

        // Deliver the message to the application.
        self.deliver.send(message).await.unwrap();
        Ok(())
    }
}

#[tokio::test]
async fn receive() {
    // Địa chỉ để listener lắng nghe.
    let address = "127.0.0.1:4000".parse::<SocketAddr>().unwrap();
    let (tx, mut rx) = channel(1);

    // SỬA ĐỔI: Khởi tạo QuicTransport và tạo một listener.
    let transport = QuicTransport::new();
    let listener = transport.listen(address).await.unwrap();

    // Spawn một Receiver để xử lý các kết nối đến.
    Receiver::spawn(listener, TestHandler { deliver: tx });
    sleep(Duration::from_millis(50)).await;

    // SỬA ĐỔI: Sử dụng QuicTransport để kết nối và gửi tin nhắn.
    let sent = "Hello, world!";
    let bytes = Bytes::from(bincode::serialize(sent).unwrap());
    
    // Tạo một client kết nối đến receiver.
    let mut client_connection = transport.connect(address).await.unwrap();
    
    // Gửi tin nhắn.
    client_connection.send(bytes.clone()).await.unwrap();

    // Đảm bảo tin nhắn được chuyển đến channel.
    let message = rx.recv().await;
    assert!(message.is_some());
    let received = message.unwrap();
    assert_eq!(received, sent);
}