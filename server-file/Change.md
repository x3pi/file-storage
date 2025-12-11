# Hướng dẫn cấu hình Server

## 1. Tăng giới hạn ulimit cho server-file

Chạy lệnh sau để tăng giới hạn file descriptor:

```bash
sudo ./setup_ulimit.sh
```
---

## 2. Thay đổi cấu hình .env

Cập nhật file `.env` trong thư mục `server1` và `server2`:

```env
CONTRACT_ADDRESS=0x087cdab97d38a3bfFcDee170739E8C11Af651569
```
