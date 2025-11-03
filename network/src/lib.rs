// Copyright(C) Facebook, Inc. and its affiliates.

// Khai báo các module con của crate.
mod error;
mod receiver;
mod reliable_sender;
mod simple_sender;

// SỬA ĐỔI: Khai báo các module mới cho lớp trừu tượng giao vận.
pub mod transport;
pub mod tcp;
pub mod quic;

#[cfg(test)]
#[path = "tests/common.rs"]
pub mod common;

// SỬA ĐỔI: Cập nhật các export công khai.
// Các thành phần này bây giờ hoạt động với các trait trừu tượng thay vì TCP cụ thể.
pub use crate::receiver::{MessageHandler, Receiver, Writer};
pub use crate::reliable_sender::{CancelHandler, ReliableSender};
pub use crate::simple_sender::SimpleSender;