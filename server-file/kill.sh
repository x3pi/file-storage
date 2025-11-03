#!/bin/bash

SESSION="rust_servers"

echo "🛑 Dừng tất cả tiến trình Rust đang chạy trên các cổng 7081–7082..."
for PORT in 7081 7082; do
  PID=$(sudo lsof -t -i:$PORT 2>/dev/null)
  if [ -n "$PID" ]; then
    echo " - Kill port $PORT (PID: $PID)"
    sudo kill -9 $PID
  else
    echo " - Port $PORT trống."
  fi
done

echo "🧹 Dừng tmux session nếu đang chạy..."
tmux has-session -t $SESSION 2>/dev/null
if [ $? == 0 ]; then
  tmux kill-session -t $SESSION
  echo " - Đã kill session '$SESSION'."
else
  echo " - Không có session '$SESSION' nào đang chạy."
fi

echo "✅ Dọn dẹp hoàn tất!"
