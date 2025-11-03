#!/bin/bash

SESSION="rust_servers"

# Nếu session đã tồn tại, thì attach lại luôn
tmux has-session -t $SESSION 2>/dev/null
if [ $? == 0 ]; then
  echo "Session '$SESSION' already running. Attaching..."
  tmux attach -t $SESSION
  exit 0
fi

# Tạo session mới, pane 1 chạy server 1 với .env.server1
tmux new-session -d -s $SESSION -n server1 "echo 'Starting server 1 (0.0.0.0:7081) with .env.server1...'; ENV_FILE=.env.server1 cargo run -- 0.0.0.0:7081"

# Tạo thêm pane bên phải cho server 2 với .env.server2
tmux split-window -h -t $SESSION "echo 'Starting server 2 (0.0.0.0:7082) with .env.server2...'; ENV_FILE=.env.server2 cargo run -- 0.0.0.0:7082"

# Điều chỉnh layout cho dễ nhìn
tmux select-layout -t $SESSION tiled

# Hiện GUI
tmux attach -t $SESSION
