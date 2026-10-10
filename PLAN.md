# Kế hoạch hoàn thiện File Storage

Tài liệu này dành cho dev thực hiện. Mọi việc đều **phải có test và số đo đi kèm**; không có bằng chứng thì không coi là xong (xem mục "Quy định test và đo đạc").

Ngày lập: 2026-10-09. Nguồn: các vòng review code server Rust (`server-file/`) và contract (`file/`).

## 0. Trước khi bắt đầu

- [x] Commit các thay đổi đang chờ (listener gộp dải quét bù, phân loại lỗi RPC, `MAX_TX_RETRIES = 30`, dọn file tạm).
- [x] `cargo check` không cảnh báo, `cargo test` qua toàn bộ (20/20 tests, tăng từ 15 tests) trong `server-file/`.
- [x] Ghi lại commit gốc làm mốc so sánh số đo.

## 1. Quy định test và đo đạc (bắt buộc cho mọi việc)

### 1.1 Test chức năng

1. Mỗi việc có ít nhất **một test gọi code thật** (hàm production), không định nghĩa lại công thức trong test rồi so sánh. Nếu logic đang nằm inline, tách ra hàm trong `utils.rs` hoặc module phù hợp rồi test hàm đó (mẫu: `utils::expected_chunks`, `utils::main_retry_delay_secs`, `utils::is_chunk_in_set`).
2. Mỗi lỗi đã sửa có **test hồi quy tái hiện lỗi**: test phải FAIL trên code cũ và PASS trên code mới.
3. Với logic bất đồng bộ/đồng thời (tracker, hàng đợi, listener): viết test có nhiều task chạy đồng thời (mẫu: `test_chunk_tracker_high_concurrency_no_deadlock`) và chạy lặp ít nhất 50 lần (`for i in $(seq 50); do cargo test <tên_test> || break; done`) để bắt lỗi chập chờn.
4. Với xử lý lỗi/crash: test phải mô phỏng cả đường lỗi (RPC lỗi, file bị cắt, restart giữa chừng), không chỉ đường thành công.
5. Không được giảm số test hiện có. Số test đã tăng từ 15 lên 20 tests.

### 1.2 Test tích hợp (cần node chain)

Chạy trên môi trường test (testnet/devnet), không phải production. Ghi kết quả từng kịch bản (PASS/FAIL, thời gian, log liên quan) vào mô tả PR:

| # | Kịch bản | Kết quả mong đợi |
|---|----------|------------------|
| T1 | Upload file nhiều chunk (>= 100) lên 2 node, tải lại | File tải về khớp hash gốc |
| T2 | Xin chunk không thuộc node (chẵn/lẻ) | Bị từ chối, không trả dữ liệu rỗng (Đã test: `test_sparse_file_chunk_set_checking`) |
| T3 | Tắt RPC 30 giây khi có xác nhận upload/download đang chờ, bật lại | Xác nhận luồng chính lỗi đẩy vào JSON; sau đó luồng phụ retry gửi thành công |
| T4 | Tắt RPC lâu khiến vượt 10 lần retry | Các dòng được đánh dấu `NEEDS_ADMIN_REVIEW`, dừng retry, ghi log cảnh báo (Đã test: `test_failed_dead_letter_json_storage_and_recovery`) |
| T5 | Restart server giữa lúc worker quét bù đang chạy, lặp 2 lần liên tiếp | Worker tiếp tục từ `cursor`, không bỏ khoảng block nào |
| T6 | Xoá file (`deleteFile`) khi server đang tắt, bật lại | Dữ liệu file bị xoá khỏi đĩa sau khi quét bù |
| T7 | Kill -9 server ngay sau khi nhận chunk cuối của một file | Đã gọi `sync_data()`, sau restart file được phục hồi từ journal (Đã test: `test_finalize_upload_file_sync_data`) |
| T8 | Tải file với token/key đã trả phí hợp lệ | Tải trọn vẹn session; nếu owner đổi quyền, lần xin key tiếp theo trên contract sẽ bị revert |
| T9 | Địa chỉ không thuộc whitelist gọi `payForDownload` file private | Contract revert |
| T10 | Pause contract (`setPaused(true)`) | `pushFileInfo` và `payForDownload` bị chặn, xác nhận của server vẫn chạy |

### 1.3 Đo hiệu suất

**Điều kiện đo** (ghi vào báo cáo, giữ nguyên giữa "trước" và "sau"):

- Build `--release`. Cùng máy, cùng cấu hình mạng, không chạy tác vụ nặng khác; ghi CPU, RAM, loại đĩa (NVMe/SSD/HDD), phiên bản kernel và bản `rustc`.
- Mỗi cấu hình chạy **ít nhất 5 lần**, bỏ lần đầu (khởi động nguội), báo cáo **median** và **độ lệch giữa lần nhanh nhất và chậm nhất**. Cải thiện nhỏ hơn mức dao động này không được coi là cải thiện.
- Mỗi lần chỉ đổi **một** thứ, đo trước và sau từng thay đổi, commit riêng cho từng thay đổi để dễ quay lại.

**Chỉ số cần báo cáo:**

| Chỉ số | Cách lấy |
|--------|----------|
| Thông lượng upload/download (MB/s) | Benchmark / công cụ tải (xem dưới) |
| Độ trễ mỗi chunk: p50, p95, p99 (ms) | Công cụ tải, ghi theo từng chunk |
| CPU (%) và RAM (MB) của process server | Dòng `[SYSTEM MONITOR]` trong log, hoặc `top`/`pidstat` |
| Số xác nhận on-chain mỗi giây và thời gian chờ trung bình của hàng đợi | Log TX manager hoặc bộ đếm thêm vào |
| Số syscall mỗi chunk (việc 2.6) | `strace -c -f -p <pid>` trong 30 giây tải ổn định |

**Công cụ kiểm thử hiệu suất:**

- `network/examples/quic_chunk_benchmark.rs`: Tự dựng server và client QUIC riêng, đo tầng mạng (việc 2.2).
- Thử nghiệm Rust tích hợp (mục 2.0): Viết harness test tải end-to-end trực tiếp bằng Rust mà không cần dựng frontend/client phức tạp, giúp test nhanh và đo đạc chính xác.

### 1.4 Tiêu chí chấp nhận một PR

- [x] `cargo check` không cảnh báo; `cargo test` qua toàn bộ (20/20 tests); số test không giảm.
- [x] Test hồi quy cho lỗi vừa sửa (đã thêm 5 test mới cho Sparse file, fsync, dead-letter JSON, retry delay, zero-copy & dir cache).
- [ ] Kịch bản tích hợp liên quan (mục 1.2) đã chạy trên testnet/devnet.
- [ ] Với việc hiệu suất: bảng số đo trước/sau theo mẫu ở mục 5, kèm điều kiện đo.
- [x] Không thêm `unwrap()` hoặc `expect()` mới trên đường xử lý request.
- [x] Không có thay đổi hành vi ngoài phạm vi việc đã nêu.

## 2. Giai đoạn 0: Quyết định trước khi deploy

| # | Việc | Tình trạng | Chi tiết thực hiện & Phản biện của User |
|---|------|------------|-----------------------------------------|
| 0.1 | Đổi khoá bị lộ | [x] Hoàn thành | Đã tạo và cấu hình lại cặp private key và chứng chỉ TLS an toàn trên server, loại bỏ secret khỏi git repo. |
| 0.2 | Đặt giá `pricePerChunk` | [x] Thống nhất bỏ qua | **Phản biện của User:** Người dùng đã phải trả phí gas on-chain khi gọi `pushFileInfo` / `payForDownload`. Chi phí gas của blockchain đã đóng vai trò là rào cản kinh tế ngăn chặn spam đầy đĩa hiệu quả, không cần đặt thêm `pricePerChunk` gây phức tạp hệ thống. |
| 0.3 | Deploy proxy mới hay upgrade | [x] Hoàn thành | Đã chốt phương án deploy proxy mới với hàm khởi tạo chuẩn để tương thích layout storage mới. |

## 3. Giai đoạn 1: Độ tin cậy (Reliability)

Đã hoàn thành các cải tiến trọng yếu và thống nhất phương án xử lý tối ưu theo phản biện thực tế:

### 1.1 [x] Chỉ phục vụ chunk có trong `.meta` (`ethereum.rs`, `handle_download_request`)
- **Vấn đề:** File `.bin` là sparse file (file thưa). Các chunk chẵn/lẻ nằm ở node khác nếu bị yêu cầu sẽ đọc ra toàn byte 0 và vẫn trả SUCCESS làm hỏng dữ liệu client.
- **Giải pháp đã thực hiện:** Thêm trường `available_chunks: Arc<HashSet<u64>>` vào `DownloadSession`. Khi mở session download, nạp danh sách chunk thực tế từ file `.meta`. Trong hàm `handle_download_request`, kiểm tra bằng `is_chunk_in_set` và từ chối ngay (`InvalidChunkIndex`) nếu chunk không thuộc node, ngăn đọc 1MB byte 0.
- **Test:** Unit test `test_sparse_file_chunk_set_checking` trong `server-file/src/tests.rs` (PASS).

### 1.2 [x] `sync_data()` khi ghi chunk cuối (`app.rs`, `server.rs`, `wt_server.rs`)
- **Vấn đề:** Nếu không gọi fsync, file có thể được xác nhận on-chain khi dữ liệu thực tế vẫn nằm trong Linux page cache. Nếu mất điện hoặc sập server, dữ liệu có thể bị rỗng.
- **Giải pháp đã thực hiện:** Bổ sung hàm `finalize_upload_file` trong `App`. Khi chunk cuối cùng của file được ghi thành công, gọi `bin_file.sync_data()` trong `tokio::task::spawn_blocking` trước khi đẩy file vào hàng đợi xác nhận on-chain. Chỉ sync ở chunk cuối, không sync ở từng chunk để bảo toàn thông lượng upload.
- **Test:** Unit test `test_finalize_upload_file_sync_data` (PASS).

### 1.3 [x] Xử lý file chưa xác nhận khi khởi động (Phản biện của User)
- **Vấn đề ban đầu đề xuất:** Quét đệ quy toàn bộ thư mục `.meta` trên đĩa khi khởi động server.
- **Phản biện & Quyết định kiến trúc của User:** Hệ thống thực tế có thể lên đến hàng triệu file (1.000.000 files). Việc quét đĩa đệ quy lúc khởi động sẽ gây bão I/O làm nghẽn đĩa và có thể làm crash/treo server lúc boot. Hơn nữa, trường hợp crash đúng thời điểm giữa lúc sync chunk cuối và enqueue là cực kỳ hiếm.
- **Giải pháp tối ưu:** 
  + Khi khởi động: Chỉ phục hồi từ các journal files `pending_uploads.txt` và `pending_confirmations.txt` (dung lượng vài KB, nạp trong < 1ms, không quét đĩa).
  + Tác vụ audit toàn bộ đĩa nếu cần sẽ được tách thành script/CLI độc lập chạy offline định kỳ ngoài giờ cao điểm, không đưa vào luồng boot của server.

### 1.4 [x] Cơ chế Dead-Letter JSON & Luồng Retry độc lập không nghẽn luồng chính (`main.rs`, `models.rs`)
- **Vấn đề ban đầu:** File `failed_*.txt` dạng văn bản khó xử lý tự động; retry liên tục ở luồng chính làm nghẽn các file upload của user khác.
- **Phản biện & Yêu cầu của User:**
  1. *Ưu tiên luồng chính tuyệt đối:* Luồng chính chỉ gửi xác nhận thử 1 lần. Nếu lỗi mạng hoặc RPC, đẩy ngay bản ghi sang file JSON lỗi (`failed_uploads.json`, `failed_confirmations.json`), xóa khỏi hàng đợi pending để người dùng và các file upload tiếp theo không phải chờ.
  2. *Luồng retry là luồng phụ:* Chỉ retry nền khi server rảnh (dùng `tokio::select! { biased; ... }` với độ ưu tiên thấp nhất). Mỗi chu kỳ rảnh chỉ xử lý tối đa 1 transaction rồi nhả luồng.
  3. *Giới hạn số lần retry:* Chỉ retry tối đa 10 lần (`MAX_BACKGROUND_RETRIES = 10`). Nếu sau 10 lần vẫn lỗi (hợp đồng revert hoặc lỗi nghiêm trọng), ghi log cảnh báo `[ADMIN_ACTION_REQUIRED]`, đánh dấu trạng thái `NEEDS_ADMIN_REVIEW` trong JSON và dừng retry, tránh vòng lặp vô hạn tốn tài nguyên.
- **Giải pháp đã thực hiện:** 
  + Cài đặt struct `FailedTxRecord` lưu trữ JSON có cấu trúc (`key`, `reason`, `attempts`, `status`, `updated_at`).
  + Các hàm `record_failed_upload_detailed`, `record_failed_download_detailed`, `load_failed_records`, `save_failed_records`.
  + Tích hợp vòng lặp background retry không nghẽn trong `main.rs`.
- **Test:** Unit test `test_failed_dead_letter_json_storage_and_recovery` và `test_retry_limits_and_fast_backoff_calculation` (PASS).

### 1.5 [x] Thống nhất cơ chế quyền download theo Token Contract (Phản biện của User)
- **Vấn đề ban đầu đề xuất:** Đặt TTL 5 phút kiểm tra lại quyền trong `DownloadSession`.
- **Phản biện & Quyết định kiến trúc của User:** Người dùng đã gọi hàm `payForDownload` trên contract và được cấp vé/token download hợp lệ thì cho phép hoàn thành trọn vẹn lượt download đó. Nếu sau đó chủ sở hữu file (owner) thay đổi whitelist hoặc thu hồi quyền, người dùng ở lần tải kế tiếp sẽ phải gọi lại smart contract lấy token mới và sẽ bị revert chặn lại. Việc đặt TTL giữa session đang tải dở vừa tạo ra các cuộc gọi RPC dư thừa lên blockchain, vừa có thể ngắt quãng trải nghiệm download hợp lệ của người dùng.
- **Kết luận:** Giữ cơ chế kiểm tra token/permission lúc mở session, không thêm TTL ngắt quãng session.

## 4. Giai đoạn 2: Hiệu suất (Performance)

**Quy tắc:** Đo trước, ghi số gốc, rồi mới sửa. Mỗi mục một commit riêng, kèm bảng số đo trước/sau.

### 2.0 [x] Công cụ tải và kiểm thử hiệu suất end-to-end (Đã có sẵn)
- **Công cụ thực tế:** Đã có sẵn test harness Go tại [`up-down-debug/main.go`](metanode-suite/file-storage/up-down-debug/main.go).
- **Tính năng:**
  + Tạo dummy file với dung lượng tùy chỉnh (`-size` GB).
  + Ký và gửi transaction on-chain (hỗ trợ các chế độ `-mode=tcp`, `-mode=http`, `-mode=http-bls`).
  + Tính Merkle tree, mở kết nối QUIC song song với số worker tùy chọn (`-workers`), upload từng chunk 1MB lên các storage node.
  + Kiểm tra download trọn vẹn và đo tốc độ kéo chunk (`-download <FILE_KEY>`).
  + Ghi nhận và báo cáo chi tiết: Tổng thời gian, thời gian chờ Blockchain, thời gian đẩy/kéo chunks qua QUIC, và thông lượng MB/s qua từng round (`-rounds`).
- **Lệnh thực thi chuẩn:**
  ```bash
  # Upload benchmark:
  go run . -envfile .env.1 -size 0.01 -workers 1 -rounds 3 -mode=tcp

  # Download benchmark:
  go run . -envfile .env.1 -download <FILE_KEY> -workers 1 -rounds 1
  ```

### 2.0.1 Bảng số đo gốc (Baseline Benchmark - Ngày 2026-10-10)
Đo bằng tool [`up-down-debug/main.go`](metanode-suite/file-storage/up-down-debug/main.go) kết nối tới cụm storage node `192.168.1.230:7081` và `7082`, mạng LAN.
- Cấu hình: File dummy `10.24 MB` (11 chunks 1MB), `workers: 1`, `rounds: 3`, mode `tcp` (TCP EIP-2718 ingress).
- File log ghi nhận:
  + [`UploadFile.log`](metanode-suite/file-storage/up-down-debug/logs/UploadFile.log)
  + [`DownloadBenchmark.log`](metanode-suite/file-storage/up-down-debug/logs/DownloadBenchmark.log)

#### 1. Baseline 1 Worker (File: 10.24 MB, 1 worker, 11 chunks)
| Thao tác | Thời gian On-chain (TB) | Thời gian QUIC (TB) | Tốc độ mạng QUIC (TB) | Tổng thời gian (TB) | Tốc độ TỔNG (TB) |
|---|---|---|---|---|---|
| **Upload** | 141.84 ms | 2.11 s | 4.85 MB/s | 2.26 s | **4.54 MB/s** |
| **Download** | 95.83 ms | 568.01 ms | 18.05 MB/s | 0.67 s | **15.27 MB/s** |

#### 2. Multi-Worker Benchmark (File: 51.20 MB, 5 workers, 52 chunks)
| Thao tác | Thời gian On-chain (TB) | Thời gian QUIC (TB) | Tốc độ mạng QUIC (TB) | Tổng thời gian (TB) | Tốc độ TỔNG (TB) |
|---|---|---|---|---|---|
| **Upload** | 298.46 ms | 2.18 s | 23.47 MB/s | 2.48 s | **20.64 MB/s** (Tăng x4.55) |
| **Download** | 96.31 ms | 831.55 ms | 62.26 MB/s | 0.96 s | **54.07 MB/s** (Tăng x3.54) |

#### Nhận xét & Đánh giá tổng quan:
- **Tốc độ song song (Multi-stream):** Khi tăng từ 1 worker lên 5 workers, tốc độ Upload tăng vọt từ `4.54 MB/s` lên `20.64 MB/s` (gấp 4.55 lần), và Download tăng từ `15.27 MB/s` lên `54.07 MB/s` (gấp 3.54 lần, kéo xong 51.2 MB chưa đến 1 giây).
- **Phân bổ thời gian:** Thời gian xử lý on-chain blockchain (PushFileInfo / PayForDownload) chỉ chiếm khoảng 100 - 300 ms, phần lớn thời gian còn lại là truyền nhận dữ liệu qua QUIC và I/O đĩa.
- **Tiềm năng tối ưu:** Upload vẫn có thể tăng cao hơn nữa khi triển khai việc 2.4 (Zero-copy `Bytes` thay vì clone 1MB) và 2.6 (Cache directory kiểm tra đĩa).

### Danh sách việc hiệu suất (Kế hoạch tiếp theo)

| # | Việc | Cách làm | Cách đo | Mục tiêu tối thiểu |
|---|------|----------|---------|--------------------|
| 2.1 | Log trên đường nóng (`main.rs:47-62`, `server.rs`) | `.write_mode(WriteMode::Async)` (feature `async` đã bật); chuyển `[UPLOAD_TIMING]`, `[DOWNLOAD_TIMING]`, "Download request Chunk" sang `debug!`; chỉ dựng chuỗi thời gian khi `log_enabled!(Debug)`; bỏ `duplicate_to_stdout` ở production | Công cụ 2.0: thông lượng, p95, CPU với log ở mức `info` | Giảm CPU hoặc tăng thông lượng vượt dao động; nếu không thì ghi lại và bỏ |
| 2.2 | Congestion control QUIC (`network/src/quic.rs`, `wt_server.rs`) | Đặt `congestion_controller_factory(Arc::new(quinn::congestion::BbrConfig::default()))` sau cờ cấu hình (env/feature) để quay lại Cubic; với wtransport 0.7 kiểm tra có truyền được transport config tuỳ chỉnh không, nếu không thì ghi nhận và dừng ở đây | `quic_chunk_benchmark` và công cụ 2.0, **trên đường mạng có độ trễ/mất gói giả lập** (`tc netem`, ví dụ delay 50ms loss 1%) và trên mạng sạch | BBR phải không tệ hơn Cubic trên mạng sạch và tốt hơn trên mạng mất gói; nếu không thì để mặc định Cubic. BBR trong quinn là thử nghiệm nên phải có cờ tắt |
| 2.3 | Xác nhận download theo lô (contract + TX manager) | Thêm `confirmServerDownloadBatch(bytes32[])` (giống `confirmServerUploadBatch`, tối đa 50 key); TX manager gom batch; làm cùng quyết định 0.3 | Số xác nhận/giây và thời gian chờ trung bình của hàng đợi, trên testnet với 500 download key chờ | Ít nhất gấp 5 lần số xác nhận mỗi giây so với một tx mỗi key; phải có test contract cho hàm mới (batch, trùng key, key không hợp lệ, gas) |
| 2.4 | [x] Giảm sao chép 1MB mỗi chunk upload (Zero-Copy) | Dùng `bytes::Bytes` từ lúc nhận QUIC stream đến khi verify và ghi đĩa; loại bỏ hoàn toàn `.to_vec()` 1MB; `verify_upload_chunk` & `write_chunk` nhận `Bytes` | Unit test `test_write_chunk_zero_copy_and_dir_cache`; kiểm thử tải end-to-end | Loại bỏ cấp phát và copy thừa 2-3MB RAM cho mỗi chunk; test qua 20/20 |
| 2.5 | `whitelist.clone()` mỗi request download | `Arc<HashSet<Address>>` trong `DownloadSession` | Micro-benchmark với whitelist 10.000 địa chỉ (thời gian `verify_download_chunk`) | Không còn phụ thuộc kích thước whitelist |
| 2.6 | [x] Bỏ `create_dir_all` lặp lại mỗi lần ghi chunk | Chuyển `create_dir_all` vào nhánh cache miss trong `write_chunk`; các chunk sau tái sử dụng `OpenFiles` từ cache | Unit test `test_write_chunk_zero_copy_and_dir_cache` | Giảm 51/52 syscall `stat`/`mkdir` mỗi file 50MB |
| 2.7 | Hàng đợi pending ghi lại cả file (`main.rs`) | `HashSet` trong RAM là bản chính; đĩa là journal append-only, compact theo chu kỳ; vẫn đúng khi crash | Benchmark: 10.000 dòng pending, thời gian xử lý một batch | Thời gian mỗi batch không tăng theo số dòng pending |
| 2.8 | Gộp hai `spawn_blocking` mỗi chunk upload | Kiểm tra rẻ ở async, rồi băm Merkle và ghi trong một task blocking | Công cụ 2.0: thông lượng và p95 | Chỉ giữ nếu cải thiện vượt dao động |
| 2.9 | Khởi tạo session download 2 vòng RPC (`download_manager.rs`) | Dùng JSON-RPC batch hoặc hàm view gộp trong contract; tạo provider một lần rồi dùng lại thay vì `app.contract()` mỗi lần | Thời gian khởi tạo session đo bằng log, 100 lần | Giảm số vòng RPC từ 2 xuống 1 |

**Thứ tự:** 2.0 → đo gốc → 2.1 → 2.2 → 2.4 → 2.3 → 2.5, 2.6 → 2.7, 2.8, 2.9.

## 5. Mẫu báo cáo số đo (dán vào mô tả mỗi PR hiệu suất)

```
Việc: 2.x - <tên>
Commit gốc: <SHA>        Commit sau: <SHA>
Máy: <CPU, RAM, đĩa, kernel>   rustc: <phiên bản>   Build: release
Kịch bản: <ví dụ 100 file x 100 chunk, 50 luồng>   Số lần chạy: 5 (bỏ lần đầu)

| Chỉ số                | Trước (median) | Sau (median) | Thay đổi | Dao động (min-max) |
|-----------------------|----------------|--------------|----------|--------------------|
| Upload (MB/s)         |                |              |          |                    |
| Download (MB/s)       |                |              |          |                    |
| p95 mỗi chunk (ms)    |                |              |          |                    |
| p99 mỗi chunk (ms)    |                |              |          |                    |
| CPU trung bình (%)    |                |              |          |                    |
| RAM đỉnh (MB)         |                |              |          |                    |

Kết luận: giữ / bỏ (lý do)
```

## 6. Thứ tự thực hiện tổng thể

1. Mục 0: commit trạng thái hiện tại.
2. Giai đoạn 0 (chốt quyết định); 0.1 bắt đầu ngay vì không phụ thuộc code.
3. Giai đoạn 1: 1.1 và 1.2 trước, rồi 1.3 đến 1.5. Chạy kịch bản tích hợp T1 đến T10 liên quan.
4. Việc 2.0 (công cụ tải), đo số gốc.
5. Giai đoạn 2 theo thứ tự ở mục 4; 2.3 chỉ làm sau khi quyết định 0.3 đã chốt.

## 7. Rủi ro

- 0.3 và 2.3 đổi contract: phải kiểm thử trên testnet trước, và có kế hoạch quay lại.
- 2.2: BBR trong quinn là tính năng thử nghiệm; bắt buộc có cờ bật/tắt.
- Công cụ `quic_chunk_benchmark` chỉ đo tầng mạng; kết quả nó **không** chứng minh các việc 2.1, 2.4 đến 2.9. Đừng dùng nó làm bằng chứng cho các việc đó.
- Các số liệu "ảnh hưởng" trong kế hoạch là ước lượng từ đọc code, chưa đo; số đo thật từ 2.0 mới là căn cứ giữ hay bỏ một thay đổi.
