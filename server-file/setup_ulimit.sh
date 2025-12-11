#!/bin/bash

# Script must run as root
if [ "$EUID" -ne 0 ]; then
  echo "Please run as root: sudo ./setup_ulimit.sh"
  exit 1
fi

TARGET_LIMIT=500288

echo ">>> Setting permanent ulimit to $TARGET_LIMIT ..."

# 1. Update limits.conf
echo "* soft nofile $TARGET_LIMIT" >> /etc/security/limits.conf
echo "* hard nofile $TARGET_LIMIT" >> /etc/security/limits.conf

# 2. Update common-session (needed to apply limits to login shells)
if ! grep -q "pam_limits.so" /etc/pam.d/common-session; then
  echo "session required pam_limits.so" >> /etc/pam.d/common-session
fi

# 3. Update systemd limits for ALL services
echo "DefaultLimitNOFILE=$TARGET_LIMIT" >> /etc/systemd/system.conf
echo "DefaultLimitNOFILE=$TARGET_LIMIT" >> /etc/systemd/user.conf

# 4. Reload systemd
systemctl daemon-reload

echo ">>> Done!"
echo ">>> PLEASE REBOOT to apply all changes."
echo "After reboot, run: ulimit -n"
