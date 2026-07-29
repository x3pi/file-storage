Ran command: `./run.sh`
Viewed wt_server.rs:311-349

Hệ thống bạn đang xây dựng (sử dụng Rust + QUIC/WebTransport + Blockchain) thực sự đã mang dáng dấp của một kiến trúc lưu trữ hiện đại rồi! Tuy nhiên, để "vươn tới tầm cỡ" của các ông lớn như **AWS S3, Google Drive, Dropbox hay Cloudflare R2**, chúng ta còn rất nhiều kỹ thuật nâng cao có thể áp dụng.

Dưới đây là những "bí quyết" tối ưu hóa hệ thống lưu trữ phân tán mà các Big Tech đang sử dụng, chia theo từng tầng (layer) để bạn dễ học hỏi:

---

### 1. Tầng Storage & Ổ Cứng (Disk I/O)
Đây thường là nút thắt cổ chai lớn nhất khi băng thông mạng quá nhanh.

- **Cấp phát trước không gian (Fallocate/Pre-allocation):**
  - **Hiện tại:** Khi bạn ghi chunk bằng `write_all_at` vào các offset khác nhau, hệ điều hành phải liên tục mở rộng kích thước file và cập nhật Metadata của File System (gây lock và phân mảnh ổ đĩa).
  - **Big Tech làm:** Ngay khi nhận Request Upload (ví dụ file 230MB), server lập tức gọi hàm `fallocate` (Linux) để xin luôn 230MB liền mạch trên ổ cứng. Sau đó các Worker ghi chunk song song vào các vị trí đã cấp phát sẽ cực kỳ mượt mà, không bị giành giật I/O (Disk fragmentation = 0%).
- **Bypass Page Cache (O_DIRECT):**
  - Với file lớn (như video, ISO), việc dùng I/O thông thường sẽ làm hệ điều hành đưa toàn bộ data vào RAM (Page Cache), làm tràn RAM và chậm hệ thống. Các database và hệ thống storage (như MinIO) dùng cờ `O_DIRECT` để ghi thẳng dữ liệu từ App xuống đĩa NVMe mà không đi qua RAM của OS.
- **Dùng `io_uring` thay cho `spawn_blocking`:**
  - Trong Rust, bạn đang dùng `tokio::task::spawn_blocking` cho các thao tác đọc/ghi file. Các hệ thống hiện đại bậc nhất trên Linux chuyển sang dùng **`io_uring`** (trong Rust có crate `tokio-uring`) để thực hiện Async File I/O ở cấp độ Kernel, tăng tốc độ I/O lên gấp nhiều lần.

### 2. Tầng Mạng & Giao thức (Network & Transport)
WebTransport (QUIC) là một bước đi xuất sắc, nhưng có thể tinh chỉnh thêm:

- **Bật thuật toán chống nghẽn mạng BBRv2:**
  - Bặc định Linux dùng thuật toán CUBIC. Google đã phát minh ra BBR (Bottleneck Bandwidth and Round-trip propagation time) giúp tăng tốc độ truyền tải QUIC/TCP lên **gấp 2 - 3 lần** trong điều kiện mạng chập chờn (lossy networks). Bạn chỉ cần gõ vài lệnh cấu hình `sysctl` trên server Ubuntu là bật được BBR.
- **Zero-Copy Download (Khó nhưng đáng giá):**
  - Khi Download, thay vì Đọc file từ ổ cứng -> copy vào RAM -> copy xuống Card mạng (NIC), hệ thống lớn dùng `sendfile()` (hoặc DMA) để bắn thẳng dữ liệu từ ổ cứng ra Card mạng. Tuy WebTransport phải mã hóa (encryption) nên khó làm Zero-copy hoàn toàn, nhưng có thể dùng kTLS (Kernel TLS) để offload việc mã hóa xuống Kernel.

### 3. Tầng Kiến trúc (Architecture & Control Plane)
Đây là sự khác biệt lớn nhất giữa một server tự code và kiến trúc Cloud.

- **Tách biệt Control Plane và Data Plane (Cực kỳ quan trọng):**
  - **Hiện tại:** Node Storage của bạn đang phải tự gọi RPC lên SmartContract (`verify_upload_chunk`, `isPublicFile`...) để check quyền. Việc gọi RPC rất chậm và phụ thuộc vào Node Blockchain (có thể mất hàng trăm mili-giây).
  - **AWS S3 làm:** Sử dụng **Presigned URL** (URL có chữ ký). 
    - Client gọi API tới một máy chủ trung tâm (Control Plane). Máy chủ này check Blockchain 1 lần duy nhất, nếu OK thì dùng Private Key sinh ra một cái Token/Chữ ký HMAC.
    - Client cầm cái Chữ ký đó ném thẳng cho Node Storage (Data Plane). Node Storage chỉ mất **0.001ms** để xác thực thuật toán mã hóa (xem Token có hợp lệ/hết hạn không) rồi lập tức cho phép Upload/Download mà KHÔNG CẦN gọi RPC lên Blockchain nữa! Tốc độ sẽ tăng phi mã.
- **Erasure Coding (Lưu trữ phân tán siêu việt):**
  - Thay vì copy y nguyên 1 file 230MB sang cả 2 Server (tốn 460MB), hệ thống lớn dùng **Reed-Solomon Erasure Coding**. Họ băm file 230MB thành các phần nhỏ (ví dụ 10 phần data + 4 phần parity) và rải lên nhiều node. Chỉ cần lấy được 10 phần bất kỳ là khôi phục được file gốc. Vừa tiết kiệm dung lượng, vừa tăng tốc độ tải vì có thể tải song song từ nhiều nguồn khác nhau (như BitTorrent).

---
**Tóm lại: Bước đi tiếp theo cho bạn là gì?**
Để không bị ngợp, tôi khuyên bạn nên làm theo thứ tự dễ đến khó:
1. **Dễ nhất:** Bật `BBR` trên Linux Server của bạn (chỉ mất 2 phút cấu hình OS).
2. **Dễ nhì:** Tối ưu hóa ghi file: thêm logic `file.set_len(total_size)` (fallocate) trước khi ghi chunk đầu tiên trong `app.rs`.
3. **Thách thức:** Áp dụng **Presigned Token / HMAC** để Server Storage không phải gọi lên Blockchain mỗi lần Validate Chunk nữa. 

Bạn có hứng thú với gạch đầu dòng nào nhất? Chúng ta có thể bắt tay vào mổ xẻ và code ngay tính năng đó!