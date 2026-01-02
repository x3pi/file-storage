#!/bin/bash

# Script để nén project, loại trừ thư mục target và các file không cần thiết

# Lấy tên thư mục hiện tại làm tên project
PROJECT_NAME="file-storage"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
ZIP_NAME="${PROJECT_NAME}_${TIMESTAMP}.zip"

# Thư mục gốc của project
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "📦 Đang nén project: $PROJECT_NAME"
echo "📁 Thư mục: $SCRIPT_DIR"
echo "📄 File output: $ZIP_NAME"
echo ""

# Tạo file zip, loại trừ:
# - target/ (build artifacts)
# - log/ (log files)
# - storage_*/ (storage directories)
# - *.log (log files)
# - .git/ (git repository)
zip -r "$ZIP_NAME" . \
    -x "*/target/*" \
    -x "*/log/*" \
    -x "*/storage_*/*" \
    -x "*.log" \
    -x ".git/*" \
    -x ".git/" \
    -x ".gitignore" \
    -x "*.zip" \
    -x "*.swp" \
    -x "*.swo" \
    -x "*~" \
    -x ".DS_Store" \
    -x "zip.sh"

if [ $? -eq 0 ]; then
    FILE_SIZE=$(du -h "$ZIP_NAME" | cut -f1)
    echo ""
    echo "✅ Nén thành công!"
    echo "📦 File: $ZIP_NAME"
    echo "📊 Kích thước: $FILE_SIZE"
    echo "📍 Vị trí: $SCRIPT_DIR/$ZIP_NAME"
else
    echo ""
    echo "❌ Lỗi khi nén project!"
    exit 1
fi

