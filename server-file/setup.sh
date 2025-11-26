#!/bin/bash
# =================================================================
# QUIC TUNING FOR FILE STORAGE (High Concurrency)
# Spec: Chunk 250KB | Max File 2GB
# =================================================================
echo ">>> Bắt đầu cấu hình tối ưu QUIC..."
# -----------------------------------------------------------------
# 1. TĂNG HÀNG ĐỢI GÓI TIN ĐẦU VÀO (QUAN TRỌNG NHẤT)
# -----------------------------------------------------------------
# Lý do: 1 chunk 250KB = ~190 gói tin UDP. 
# Nếu 150 user cùng upload/download => 28,500 gói tin ập vào.
# Mức cũ 5000 sẽ bị tràn. Set 30,000 là an toàn.
sudo sysctl -w net.core.netdev_max_backlog=30000
# -----------------------------------------------------------------
# 2. BỘ ĐỆM SOCKET (SOCKET BUFFERS)
# -----------------------------------------------------------------
# Max: 32MB. Cần lớn để duy trì throughput cao cho file 2GB.
sudo sysctl -w net.core.rmem_max=33554432
sudo sysctl -w net.core.wmem_max=33554432
# Default: 8MB. 
# 8MB đủ chứa khoảng 32 chunks (250KB/chunk) trong RAM chờ xử lý.
# Không set quá cao (như 25MB) để tránh hết RAM khi có hàng nghìn kết nối.
sudo sysctl -w net.core.rmem_default=8388608
sudo sysctl -w net.core.wmem_default=8388608
# -----------------------------------------------------------------
# 3. BỘ NHỚ UDP TOÀN HỆ THỐNG
# -----------------------------------------------------------------
# Đơn vị: Pages (4KB).
# Min: ~256MB | Pressure: ~512MB | Max: ~1GB
# Cho phép Kernel dùng tới 1GB RAM cho UDP nếu tải quá nặng.
sudo sysctl -w net.ipv4.udp_mem="65536 131072 262144"
# -----------------------------------------------------------------
# 4. TỐI ƯU KẾT NỐI & HANDSHAKE
# -----------------------------------------------------------------
# Tăng hàng đợi bắt tay (Handshake) để xử lý nhiều user login cùng lúc.
sudo sysctl -w net.core.somaxconn=4096
# Tăng tính ổn định khi mạng chập chờn
sudo sysctl -w net.ipv4.udp_rmem_min=16384
sudo sysctl -w net.ipv4.udp_wmem_min=16384

# -----------------------------------------------------------------
# 5. CẤU HÌNH CARD MẠNG (NIC QUEUE)
# -----------------------------------------------------------------
# Tìm tên card mạng chính (thường là eth0, ens3, enp...)
NIC_NAME=$(ip -o -4 route show to default | awk '{print $5}' | head -n1)

if [ -n "$NIC_NAME" ]; then
    echo ">>> Phát hiện card mạng chính: $NIC_NAME"
    # Tăng hàng đợi phần cứng từ 1000 lên 10000 để khớp với Kernel
    sudo ip link set dev "$NIC_NAME" txqueuelen 10000
    echo ">>> Đã tăng txqueuelen lên 10000 cho $NIC_NAME"
else
    echo "⚠️  Không tìm thấy card mạng tự động. Hãy chạy thủ công lệnh: sudo ip link set dev [tên_card] txqueuelen 10000"
fi

echo "================================================================="
echo ">>> HOÀN TẤT! KIỂM TRA LẠI THÔNG SỐ:"
sysctl net.core.netdev_max_backlog
sysctl net.core.rmem_default
echo "================================================================="