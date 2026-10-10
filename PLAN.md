# Kế hoạch hoàn thiện File Storage

Tài liệu này dành cho dev thực hiện. Mọi việc đều **phải có test và số đo đi kèm**; không có bằng chứng thì không coi là xong (xem mục "Quy định test và đo đạc").

Ngày lập: 2026-10-09. Nguồn: các vòng review code server Rust (`server-file/`) và contract (`file/`).

## 0. Trước khi bắt đầu

- [ ] Commit các thay đổi đang chờ (listener gộp dải quét bù, phân loại lỗi RPC, `MAX_TX_RETRIES = 30`, dọn file tạm). Chạy `git status` và đảm bảo cây làm việc sạch.
- [ ] `cargo check` không cảnh báo, `cargo test` qua toàn bộ (hiện 15 test) trong `server-file/`.
- [ ] Ghi lại commit gốc (SHA) làm mốc so sánh số đo.

## 1. Quy định test và đo đạc (bắt buộc cho mọi việc)

### 1.1 Test chức năng

1. Mỗi việc có ít nhất **một test gọi code thật** (hàm production), không định nghĩa lại công thức trong test rồi so sánh. Nếu logic đang nằm inline, tách ra hàm trong `utils.rs` hoặc module phù hợp rồi test hàm đó (mẫu: `utils::expected_chunks`, `utils::retry_delay_secs`).
2. Mỗi lỗi đã sửa có **test hồi quy tái hiện lỗi**: test phải FAIL trên code cũ và PASS trên code mới. Ghi rõ trong PR cách xác nhận điều này (chạy test trên commit gốc).
3. Với logic bất đồng bộ/đồng thời (tracker, hàng đợi, listener): viết test có nhiều task chạy đồng thời (mẫu: `test_chunk_tracker_high_concurrency_no_deadlock`) và chạy lặp ít nhất 50 lần (`for i in $(seq 50); do cargo test <tên_test> || break; done`) để bắt lỗi chập chờn.
4. Với xử lý lỗi/crash: test phải mô phỏng cả đường lỗi (RPC lỗi, file bị cắt, restart giữa chừng), không chỉ đường thành công.
5. Không được giảm số test hiện có. PR phải ghi số test trước và sau.

### 1.2 Test tích hợp (cần node chain)

Chạy trên môi trường test (testnet/devnet), không phải production. Ghi kết quả từng kịch bản (PASS/FAIL, thời gian, log liên quan) vào mô tả PR:

| # | Kịch bản | Kết quả mong đợi |
|---|----------|------------------|
| T1 | Upload file nhiều chunk (>= 100) lên 2 node, tải lại | File tải về khớp hash gốc |
| T2 | Xin chunk không thuộc node (chẵn/lẻ) | Bị từ chối, không trả dữ liệu rỗng |
| T3 | Tắt RPC 30 giây khi có xác nhận upload/download đang chờ, bật lại | Xác nhận vẫn được gửi, không mất, không nằm trong `failed_*.txt` |
| T4 | Tắt RPC hơn 1 giờ (hoặc hạ `MAX_TX_RETRIES` xuống thấp để mô phỏng) | Các dòng chuyển vào `failed_*.txt`; sau khi nạp lại thì được xử lý |
| T5 | Restart server giữa lúc worker quét bù đang chạy, lặp 2 lần liên tiếp | Worker tiếp tục từ `cursor`, không bỏ khoảng block nào |
| T6 | Xoá file (`deleteFile`) khi server đang tắt, bật lại | Dữ liệu file bị xoá khỏi đĩa sau khi quét bù |
| T7 | Kill -9 server ngay sau khi nhận chunk cuối của một file | Sau restart file vẫn được xác nhận (việc 1.3) |
| T8 | Owner đổi whitelist/public khi session download đang mở | Sau TTL, quyền mới được áp dụng (việc 1.5) |
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

**Công cụ có sẵn và giới hạn của nó:**

- `network/examples/quic_chunk_benchmark.rs` (chạy: `cd network && cargo run --release --example quic_chunk_benchmark`). Công cụ này **tự dựng server và client QUIC riêng, chỉ đo tầng mạng** (10 kết nối, 2000 chunk 1MiB). Nó dùng cho **việc 2.2 (congestion control)**; nó **không** chạy qua code của `server-file` nên **không đo được** log, Merkle, ghi đĩa, hay xác nhận on-chain.
- Vì vậy cần thêm **công cụ tải end-to-end** (việc 2.0 bên dưới) cho các việc còn lại.

### 1.4 Tiêu chí chấp nhận một PR

- [ ] `cargo check` không cảnh báo; `cargo test` qua toàn bộ; số test không giảm.
- [ ] Test hồi quy cho lỗi vừa sửa (đã xác nhận FAIL trên commit gốc).
- [ ] Kịch bản tích hợp liên quan (mục 1.2) đã chạy, có kết quả trong PR.
- [ ] Với việc hiệu suất: bảng số đo trước/sau theo mẫu ở mục 5, kèm điều kiện đo.
- [ ] Không thêm `unwrap()` hoặc `expect()` mới trên đường xử lý request.
- [ ] Không có thay đổi hành vi ngoài phạm vi việc đã nêu.

## 2. Giai đoạn 0: Quyết định trước khi deploy

Cần người phụ trách sản phẩm chốt; không phải việc code thuần.

| # | Việc | Vì sao | Cách làm | Nghiệm thu |
|---|------|--------|----------|------------|
| 0.1 | Đổi khoá bị lộ | `PRIVATE_KEY` của hai server và TLS `private.key.local` nằm trong lịch sử git và remote GitHub | Tạo ví storage mới; `addStorageServer` cho ví mới, `removeStorageServer` cho ví cũ; cấp lại chứng chỉ TLS; đặt `.env` chỉ trên server; nếu repo công khai thì xoá lịch sử bằng `git filter-repo` | Ví cũ không còn gọi được `confirmServer*` (thử gọi, phải revert "Caller is not a storage server") |
| 0.2 | Đặt giá `pricePerChunk` | Mặc định 0 nên upload/download miễn phí, ai cũng làm đầy đĩa các node | `setPricePerChunk` ngay sau deploy, hoặc thêm giới hạn dung lượng/số file theo địa chỉ trong `pushFileInfo` | Test contract: upload thiếu tiền bị revert; giá đúng được thu |
| 0.3 | Deploy proxy mới hay upgrade | Layout storage đã đổi (`mKeyToFileInfo` đổi kiểu, `mNameToFileKey` bị xoá) nên upgrade proxy cũ làm hỏng dữ liệu | Khuyến nghị deploy proxy mới; truyền calldata `initialize()` vào constructor `ERC1967Proxy` để không bị front-run; nếu buộc phải upgrade thì chạy OpenZeppelin upgrades plugin (`validateUpgrade`) | Báo cáo validate không lỗi; `initialize()` không gọi lại được từ địa chỉ khác |

## 3. Giai đoạn 1: Độ tin cậy

Làm theo thứ tự. Mỗi việc cần test hồi quy (mục 1.1) và kịch bản tích hợp ghi ở cột cuối.

### 1.1 Chỉ phục vụ chunk có trong `.meta` (`ethereum.rs`, `handle_download_request`)

- **Vấn đề:** file `.bin` là file thưa; chunk chẵn/lẻ nằm ở node khác nên đọc ra toàn byte 0 và vẫn trả SUCCESS.
- **Cách làm:** lưu tập chunk đang có (`Arc<HashSet<u64>>`) vào `DownloadSession` khi khởi tạo; từ chối chunk không thuộc tập.
- **Test:** unit test hàm kiểm tra chunk thuộc tập (tách thành hàm); tích hợp T2.

### 1.2 `sync_data()` khi ghi chunk cuối (`server.rs`, `wt_server.rs`)

- **Vấn đề:** không bao giờ fsync, file có thể được xác nhận trên chain khi dữ liệu còn ở page cache.
- **Cách làm:** gọi `bin_file.sync_data()` trong `spawn_blocking` ngay trước khi enqueue xác nhận. Chỉ ở chunk cuối, không ở mọi chunk.
- **Test:** unit test hàm hoàn tất file có gọi sync (có thể kiểm tra bằng cách tách hàm `finalize_file`); **đo**: thời gian thêm mỗi file (ghi trong PR), mục tiêu không làm giảm thông lượng quá 2%.

### 1.3 Quét `.meta` đủ chunk mà chưa xác nhận khi khởi động

- **Vấn đề:** crash giữa lúc ghi chunk cuối và lúc enqueue thì không chunk nào kích hoạt hoàn tất.
- **Cách làm:** khi khởi động, duyệt `storage_root`; file nào đủ `expected_chunks` và không nằm trong `pending_uploads.txt` thì hỏi `getFileInfo`, còn `Processing` thì enqueue. Chạy ở task nền, không chặn khởi động; giới hạn tốc độ RPC.
- **Test:** unit test hàm "file đủ chunk" với thư mục giả; tích hợp T7. **Đo:** thời gian quét với 10.000 thư mục file.

### 1.4 Dead-letter xử lý lại được (`main.rs`, `record_failed_*`)

- **Vấn đề:** `failed_*.txt` là văn bản cho người đọc, không nạp lại tự động.
- **Cách làm:** đổi sang JSON mỗi dòng (`key`, `contract`, `reason`, `time`, `attempts`); thêm tác vụ định kỳ (hoặc lệnh admin) nạp lại các dòng do hết retry vào hàng đợi, tối đa N lần mỗi key; revert chắc chắn (terminal) không nạp lại.
- **Test:** ghi dead-letter rồi nạp lại, key vào queue; key quá N lần không nạp; dòng cũ định dạng văn bản không làm panic. Tích hợp T4.

### 1.5 TTL cho quyền trong session download

- **Vấn đề:** `is_public` và whitelist được cache đến khi session hết hạn.
- **Cách làm:** thêm `perms_fetched_at` vào session; khi quá TTL (đề xuất 5 phút) thì lấy lại `isPublicFile`/`getWhitelist` từ chain trước khi phục vụ chunk kế tiếp.
- **Test:** unit test hàm "quyền đã quá hạn"; tích hợp T8. **Đo:** số RPC thêm mỗi phút mỗi session (phải nhỏ).

## 4. Giai đoạn 2: Hiệu suất

**Quy tắc:** đo trước, ghi số gốc, rồi mới sửa. Không có số gốc thì không được bắt đầu. Mỗi mục một commit riêng, mỗi mục một bảng trước/sau (mục 5). Thay đổi nào không cải thiện vượt mức dao động đã đo (mục 1.3) thì **bỏ**, không giữ.

### 2.0 Công cụ tải end-to-end (làm đầu tiên)

- Viết công cụ (ví dụ `server-file/examples/load_test.rs` hoặc crate riêng) chạy **qua server thật**: ký bằng khoá owner, tạo Merkle proof, upload N file M chunk song song lên 2 node, rồi tải lại; hỗ trợ cả đường QUIC thô và WebTransport.
- Báo cáo: thông lượng, p50/p95/p99 mỗi chunk, tỷ lệ lỗi, và tách riêng upload và download.
- Kịch bản chuẩn đề xuất (ghi cố định trong repo để so sánh): 100 file x 100 chunk, 50 luồng song song, chạy 5 lần.
- Đây là việc có kết quả kiểm tra được: chạy hai lần liên tiếp trên cùng code, chênh lệch median phải nhỏ (đề xuất < 5%), nếu lớn thì công cụ chưa đủ ổn định để dùng làm thước đo.

### Danh sách việc hiệu suất

| # | Việc | Cách làm | Cách đo | Mục tiêu tối thiểu |
|---|------|----------|---------|--------------------|
| 2.1 | Log trên đường nóng (`main.rs:47-62`, `server.rs`) | `.write_mode(WriteMode::Async)` (feature `async` đã bật); chuyển `[UPLOAD_TIMING]`, `[DOWNLOAD_TIMING]`, "Download request Chunk" sang `debug!`; chỉ dựng chuỗi thời gian khi `log_enabled!(Debug)`; bỏ `duplicate_to_stdout` ở production | Công cụ 2.0: thông lượng, p95, CPU với log ở mức `info` | Giảm CPU hoặc tăng thông lượng vượt dao động; nếu không thì ghi lại và bỏ |
| 2.2 | Congestion control QUIC (`network/src/quic.rs`, `wt_server.rs`) | Đặt `congestion_controller_factory(Arc::new(quinn::congestion::BbrConfig::default()))` sau cờ cấu hình (env/feature) để quay lại Cubic; với wtransport 0.7 kiểm tra có truyền được transport config tuỳ chỉnh không, nếu không thì ghi nhận và dừng ở đây | `quic_chunk_benchmark` và công cụ 2.0, **trên đường mạng có độ trễ/mất gói giả lập** (`tc netem`, ví dụ delay 50ms loss 1%) và trên mạng sạch | BBR phải không tệ hơn Cubic trên mạng sạch và tốt hơn trên mạng mất gói; nếu không thì để mặc định Cubic. BBR trong quinn là thử nghiệm nên phải có cờ tắt |
| 2.3 | Xác nhận download theo lô (contract + TX manager) | Thêm `confirmServerDownloadBatch(bytes32[])` (giống `confirmServerUploadBatch`, tối đa 50 key); TX manager gom batch; làm cùng quyết định 0.3 | Số xác nhận/giây và thời gian chờ trung bình của hàng đợi, trên testnet với 500 download key chờ | Ít nhất gấp 5 lần số xác nhận mỗi giây so với một tx mỗi key; phải có test contract cho hàm mới (batch, trùng key, key không hợp lệ, gas) |
| 2.4 | Giảm sao chép 1MB mỗi chunk upload | Dùng `bytes::Bytes` hoặc `Arc<[u8]>` từ lúc nhận đến lúc ghi; `verify_upload_chunk` và `write_chunk` nhận `Bytes` thay vì `&[u8]` rồi `.to_vec()`; `read_frame` không zero-init `vec![0; data_len]` | Công cụ 2.0: RAM đỉnh, CPU, thông lượng; có thể dùng `heaptrack` hoặc số liệu allocator jemalloc (`GLOBAL` đã dùng jemalloc) | RAM đỉnh/CPU giảm vượt dao động; test Merkle/ghi chunk vẫn qua |
| 2.5 | `whitelist.clone()` mỗi request download | `Arc<HashSet<Address>>` trong `DownloadSession` | Micro-benchmark với whitelist 10.000 địa chỉ (thời gian `verify_download_chunk`) | Không còn phụ thuộc kích thước whitelist |
| 2.6 | `create_dir_all` mỗi lần ghi (`app.rs`, `write_chunk`) | Chuyển vào nhánh cache miss | `strace -c -f` đếm syscall mỗi chunk trước/sau | Giảm số syscall `mkdir`/`stat` mỗi chunk đúng như dự kiến (1 đến 2) |
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
