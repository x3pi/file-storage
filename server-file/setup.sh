#!/bin/bash
# =================================================================
# QUIC TUNING FOR FILE STORAGE (High Concurrency - PERSISTENT)
# Spec: Chunk 250KB | Max File 2GB
# =================================================================
echo ">>> Bắt đầu cấu hình tối ưu QUIC (Lưu vĩnh viễn)..."

# 1. TẠO FILE CẤU HÌNH SYSCTL ĐỂ LƯU VĨNH VIỄN SAU KHI REBOOT
SYSCTL_CONF="/etc/sysctl.d/99-quic-tuning.conf"
echo ">>> Đang ghi cấu hình mạng vào $SYSCTL_CONF..."

sudo bash -c "cat > $SYSCTL_CONF" <<EOF
# QUIC TUNING SETTINGS
# Tăng hàng đợi gói tin đầu vào
net.core.netdev_max_backlog=30000

# Bộ đệm Socket (Max 32MB, Default 8MB)
net.core.rmem_max=33554432
net.core.wmem_max=33554432
net.core.rmem_default=8388608
net.core.wmem_default=8388608

# Bộ nhớ UDP toàn hệ thống (Max 1GB)
net.ipv4.udp_mem=65536 131072 262144

# Tối ưu kết nối & Handshake
net.core.somaxconn=4096
net.ipv4.udp_rmem_min=16384
net.ipv4.udp_wmem_min=16384
EOF

# Áp dụng ngay lập tức cấu hình vừa lưu
echo ">>> Đang áp dụng cấu hình sysctl..."
sudo sysctl --system > /dev/null

# 2. CẤU HÌNH CARD MẠNG (NIC QUEUE) LƯU VĨNH VIỄN QUA UDEV
NIC_NAME=$(ip -o -4 route show to default | awk '{print $5}' | head -n1)

if [ -n "$NIC_NAME" ]; then
    echo ">>> Phát hiện card mạng chính: $NIC_NAME"
    
    # Áp dụng ngay lập tức
    sudo ip link set dev "$NIC_NAME" txqueuelen 10000
    
    # Tạo udev rule để tự động áp dụng lại mỗi khi khởi động máy
    UDEV_RULE="/etc/udev/rules.d/99-txqueuelen.rules"
    echo ">>> Đang tạo rule UDEV tại $UDEV_RULE để giữ txqueuelen=10000 khi khởi động..."
    sudo bash -c "echo 'ACTION==\"add\", SUBSYSTEM==\"net\", KERNEL==\"$NIC_NAME\", ATTR{tx_queue_len}=\"10000\"' > $UDEV_RULE"
    
    # Tải lại udev rules
    sudo udevadm control --reload-rules
    sudo udevadm trigger
else
    echo "⚠️  Không tìm thấy card mạng tự động. Hãy chạy thủ công lệnh: sudo ip link set dev [tên_card] txqueuelen 10000"
fi

echo "================================================================="
echo ">>> HOÀN TẤT! CẤU HÌNH ĐÃ ĐƯỢC LƯU VĨNH VIỄN (KHÔNG MẤT KHI REBOOT)!"
echo "Kiểm tra lại một vài thông số:"
sysctl net.core.netdev_max_backlog
sysctl net.core.rmem_default
if [ -n "$NIC_NAME" ]; then
    echo "txqueuelen của $NIC_NAME: $(cat /sys/class/net/$NIC_NAME/tx_queue_len)"
fi
echo "================================================================="