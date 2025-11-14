#!/bin/bash

# Script để theo dõi log của server

LOG_FILE="log/app_rCURRENT.log"

if [ ! -f "$LOG_FILE" ]; then
    echo "❌ Log file not found: $LOG_FILE"
    echo "💡 Server chưa được chạy hoặc chưa tạo log file"
    exit 1
fi

echo "📊 Monitoring server logs..."
echo "📍 File: $LOG_FILE"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

# Lọc và highlight các log quan trọng
tail -f "$LOG_FILE" | while read line; do
    # Highlight SYSTEM MONITOR
    if echo "$line" | grep -q "SYSTEM MONITOR"; then
        echo -e "\033[1;36m$line\033[0m"  # Cyan bold
    # Highlight CRITICAL errors
    elif echo "$line" | grep -q "💀💀💀\|CRITICAL"; then
        echo -e "\033[1;31m$line\033[0m"  # Red bold
    # Highlight warnings
    elif echo "$line" | grep -q "⚠️\|WARN\|HIGH"; then
        echo -e "\033[1;33m$line\033[0m"  # Yellow bold
    # Highlight errors
    elif echo "$line" | grep -q "❌\|ERROR\|Failed"; then
        echo -e "\033[0;31m$line\033[0m"  # Red
    # Highlight success
    elif echo "$line" | grep -q "✅\|SUCCESS"; then
        echo -e "\033[0;32m$line\033[0m"  # Green
    # Highlight connections
    elif echo "$line" | grep -q "📥\|📤\|Accepted\|Closed"; then
        echo -e "\033[0;34m$line\033[0m"  # Blue
    # Normal lines
    else
        echo "$line"
    fi
done
