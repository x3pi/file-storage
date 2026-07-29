#!/bin/bash

# Kiểm tra xem script có được chạy dưới quyền root không
if [ "$EUID" -ne 0 ]; then 
  echo "❌ Vui lòng chạy script này với quyền sudo (ví dụ: sudo ./setup_bbr.sh)"
  exit 1
fi

echo "🚀 Đang tiến hành cài đặt và bật TCP BBR..."

# 1. Nạp module BBR vào Kernel hiện tại
echo "▶️ Đang nạp module tcp_bbr..."
modprobe tcp_bbr

# 2. Đảm bảo module sẽ tự nạp lại khi khởi động lại máy (Reboot)
echo "▶️ Đang cấu hình tự động nạp module khi khởi động..."
if ! grep -q "tcp_bbr" /etc/modules-load.d/modules.conf; then
    echo "tcp_bbr" >> /etc/modules-load.d/modules.conf
fi

# 3. Ghi cấu hình BBR vào sysctl.conf
echo "▶️ Đang ghi cấu hình mạng vào /etc/sysctl.conf..."
if ! grep -q "net.core.default_qdisc=fq" /etc/sysctl.conf; then
    echo "net.core.default_qdisc=fq" >> /etc/sysctl.conf
fi

if ! grep -q "net.ipv4.tcp_congestion_control=bbr" /etc/sysctl.conf; then
    echo "net.ipv4.tcp_congestion_control=bbr" >> /etc/sysctl.conf
fi

# 4. Cập nhật và áp dụng cấu hình sysctl ngay lập tức
echo "▶️ Đang áp dụng cấu hình..."
sysctl -p

# 5. Kiểm tra kết quả
echo "✅ Kiểm tra trạng thái BBR..."
CURRENT_CC=$(sysctl net.ipv4.tcp_congestion_control | awk '{print $3}')

if [ "$CURRENT_CC" = "bbr" ]; then
    echo "🎉 THÀNH CÔNG! TCP BBR đã được bật và hoạt động hoàn hảo."
else
    echo "⚠️ CẢNH BÁO: Không thể bật BBR. Vui lòng kiểm tra lại hệ điều hành của bạn."
fi
