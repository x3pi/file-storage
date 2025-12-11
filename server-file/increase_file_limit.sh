#!/bin/bash

echo "🔧 Increasing file descriptor limits for server-file..."
echo ""

# Check current limits
echo "📊 Current limits:"
ulimit -n
echo ""

# Set temporary limit (for current session)
echo "⚡ Setting temporary limit (current session only)..."
ulimit -n 65536
echo "✅ Temporary limit set to 65536"
echo ""

# Configure permanent limits
echo "💾 Configuring permanent limits..."

# Backup limits.conf
if [ ! -f /etc/security/limits.conf.backup ]; then
    sudo cp /etc/security/limits.conf /etc/security/limits.conf.backup
    echo "📁 Backed up /etc/security/limits.conf"
fi

# Add limits if not already present
if ! grep -q "^* soft nofile 65536" /etc/security/limits.conf; then
    echo "* soft nofile 65536" | sudo tee -a /etc/security/limits.conf
    echo "* hard nofile 65536" | sudo tee -a /etc/security/limits.conf
    echo "✅ Added limits to /etc/security/limits.conf"
else
    echo "ℹ️  Limits already configured in /etc/security/limits.conf"
fi

# Configure systemd limits if running as service
echo ""
echo "📝 For systemd service, add this to your .service file:"
echo "   [Service]"
echo "   LimitNOFILE=65536"
echo ""

# Configure sysctl
echo "🔧 Configuring system-wide limits..."
if ! grep -q "^fs.file-max" /etc/sysctl.conf; then
    echo "fs.file-max = 2097152" | sudo tee -a /etc/sysctl.conf
    sudo sysctl -p
    echo "✅ System-wide file-max set to 2097152"
else
    echo "ℹ️  System-wide file-max already configured"
fi

echo ""
echo "✅ Configuration complete!"
echo ""
echo "⚠️  IMPORTANT: You need to:"
echo "   1. Logout and login again (for /etc/security/limits.conf to take effect)"
echo "   OR"
echo "   2. Run: exec bash (to reload shell with new limits)"
echo ""
echo "📊 Verify with: ulimit -n"
echo ""
