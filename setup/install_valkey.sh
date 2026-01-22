#!/bin/bash
set -e

echo "Installing Valkey 8..."

# Install dependencies
sudo apt-get update
sudo apt-get install -y build-essential tcl pkg-config libssl-dev

# Download and compile Valkey
cd /tmp
wget https://github.com/valkey-io/valkey/archive/refs/tags/8.0.1.tar.gz
tar -xzf 8.0.1.tar.gz
cd valkey-8.0.1

# Compile
make BUILD_TLS=yes
sudo make install

# Create Valkey user and directories
sudo useradd -r -s /bin/false valkey || true
sudo mkdir -p /var/lib/valkey
sudo mkdir -p /var/log/valkey
sudo mkdir -p /etc/valkey
sudo chown -R valkey:valkey /var/lib/valkey
sudo chown -R valkey:valkey /var/log/valkey

# Create Valkey configuration
sudo tee /etc/valkey/valkey.conf > /dev/null <<EOF
# Valkey configuration for benchmark
bind 127.0.0.1
port 6379
daemonize no
supervised systemd
pidfile /var/run/valkey/valkey.pid
loglevel notice
logfile /var/log/valkey/valkey.log

# Persistence - disabled for pure in-memory performance
save ""
appendonly no

# Memory
maxmemory 4gb
maxmemory-policy noeviction

# Performance
tcp-backlog 511
timeout 0
tcp-keepalive 300

# Limits
maxclients 10000

# Working directory
dir /var/lib/valkey

# Streams configuration
stream-node-max-bytes 4096
stream-node-max-entries 100
EOF

# Create systemd service
sudo tee /etc/systemd/system/valkey.service > /dev/null <<EOF
[Unit]
Description=Valkey In-Memory Data Store
After=network.target

[Service]
Type=notify
User=valkey
Group=valkey
ExecStart=/usr/local/bin/valkey-server /etc/valkey/valkey.conf
ExecStop=/usr/local/bin/valkey-cli shutdown
Restart=always
RuntimeDirectory=valkey
RuntimeDirectoryMode=0755

[Install]
WantedBy=multi-user.target
EOF

# Start Valkey
sudo systemctl daemon-reload
sudo systemctl enable valkey
sudo systemctl start valkey

# Wait for Valkey to start
sleep 2

# Test connection
valkey-cli ping

echo "Valkey 8 installed successfully!"
echo "Connection: localhost:6379"
echo "Status: sudo systemctl status valkey"
