# File Descriptor Limit Configuration

## Problem
The server requires high file descriptor limits to handle many concurrent connections. Default Linux limit (1024) is too low and will cause "Too many open files" errors.

## Automated Setup

Run the setup script:
```bash
cd /home/abc/nhat/file-storage/server-file
./increase_file_limit.sh
```

Then **logout and login again** or run:
```bash
exec bash
```

## Manual Setup

### 1. Temporary Fix (Current Session Only)
```bash
ulimit -n 65536
```

### 2. Permanent Fix (User Level)

Edit `/etc/security/limits.conf`:
```bash
sudo nano /etc/security/limits.conf
```

Add these lines:
```
* soft nofile 65536
* hard nofile 65536
```

**Important:** Logout and login for changes to take effect!

### 3. System-Wide Limit

Edit `/etc/sysctl.conf`:
```bash
sudo nano /etc/sysctl.conf
```

Add:
```
fs.file-max = 2097152
```

Apply changes:
```bash
sudo sysctl -p
```

### 4. For Systemd Services

Create/edit service file (e.g., `/etc/systemd/system/file-storage.service`):
```ini
[Unit]
Description=File Storage Server
After=network.target

[Service]
Type=simple
User=abc
WorkingDirectory=/home/abc/nhat/file-storage/server-file
ExecStart=/path/to/server-file 0.0.0.0:7081
LimitNOFILE=65536
Restart=always

[Install]
WantedBy=multi-user.target
```

Reload and restart:
```bash
sudo systemctl daemon-reload
sudo systemctl restart file-storage
```

## Verification

Check current limit:
```bash
ulimit -n
```

Check for a running process:
```bash
cat /proc/$(pgrep server-file)/limits | grep "open files"
```

## How It Works

The server now checks file limits on startup:
- **Minimum required:** 10,000 file descriptors
- **Recommended:** 65,536 file descriptors
- If limit is too low, server will exit with helpful error message

## Why 65536?

Each connection uses file descriptors for:
- TCP socket
- Chunk files being written
- Database connections
- Log files
- System resources

With 65536 limit, server can handle:
- ~30,000+ concurrent connections
- Thousands of simultaneous chunk uploads
- Safe margin for system operations
