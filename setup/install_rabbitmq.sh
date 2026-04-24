#!/bin/bash
# Install RabbitMQ 3.13 on Ubuntu 22.04/24.04.
# Enables the management plugin and configures benchmark credentials.
# Single-node install: quorum queues work but their durability guarantee
# is degenerate on one node — documented in environment.txt per run.

set -e

echo "Installing RabbitMQ..."

# Erlang + RabbitMQ from Cloudsmith (official packaging channels)
sudo apt-get install -y -qq curl gnupg apt-transport-https

curl -1sLf 'https://github.com/rabbitmq/signing-keys/releases/download/3.0/cloudsmith.rabbitmq-erlang.E495BB49CC4BBE5B.key' \
    | sudo gpg --dearmor -o /usr/share/keyrings/rabbitmq-erlang-archive-keyring.gpg
curl -1sLf 'https://github.com/rabbitmq/signing-keys/releases/download/3.0/cloudsmith.rabbitmq-server.9F4587F226208342.key' \
    | sudo gpg --dearmor -o /usr/share/keyrings/rabbitmq-server-archive-keyring.gpg

CODENAME=$(lsb_release -cs)
sudo tee /etc/apt/sources.list.d/rabbitmq.list > /dev/null <<EOF
deb [signed-by=/usr/share/keyrings/rabbitmq-erlang-archive-keyring.gpg] https://ppa1.rabbitmq.com/rabbitmq/rabbitmq-erlang/deb/ubuntu ${CODENAME} main
deb [signed-by=/usr/share/keyrings/rabbitmq-server-archive-keyring.gpg] https://ppa1.rabbitmq.com/rabbitmq/rabbitmq-server/deb/ubuntu ${CODENAME} main
EOF

sudo apt-get update -qq
sudo apt-get install -y -qq erlang-base \
    erlang-asn1 erlang-crypto erlang-eldap erlang-ftp erlang-inets \
    erlang-mnesia erlang-os-mon erlang-parsetools erlang-public-key \
    erlang-runtime-tools erlang-snmp erlang-ssl erlang-syntax-tools \
    erlang-tftp erlang-tools erlang-xmerl
sudo apt-get install -y -qq rabbitmq-server

# Enable management plugin (used by rabbitmqadmin + optional UI at :15672)
sudo rabbitmq-plugins enable rabbitmq_management

# Raise memory high-watermark so the load scenario doesn't trigger flow
# control — we want to measure sustained throughput, not throttled.
# 0.6 = 60% of RAM, up from default 0.4.
sudo tee /etc/rabbitmq/rabbitmq.conf > /dev/null <<'EOF'
# Benchmark-specific RabbitMQ config

# Memory high-watermark: 60% of system RAM (default 40% can throttle the
# producer during the 'load' scenario and mask the real throughput ceiling).
vm_memory_high_watermark.relative = 0.6

# Disk free limit — 2 GB absolute minimum
disk_free_limit.absolute = 2GB

# Expose the management UI on localhost only
management.tcp.ip = 127.0.0.1
management.tcp.port = 15672

# Higher channel max to accommodate many workers
channel_max = 2047
EOF

sudo systemctl enable rabbitmq-server
sudo systemctl restart rabbitmq-server

# Wait for startup
echo "Waiting for RabbitMQ to become ready..."
for i in $(seq 1 30); do
    if sudo rabbitmqctl status > /dev/null 2>&1; then
        echo "RabbitMQ is ready."
        break
    fi
    sleep 2
done

# Benchmark user + permissions. Remove the default guest user's remote
# access (best practice) and create bench_user with administrator tag so
# rabbitmqadmin can declare queues.
sudo rabbitmqctl add_user bench_user bench_pass 2>/dev/null || \
    sudo rabbitmqctl change_password bench_user bench_pass
sudo rabbitmqctl set_user_tags bench_user administrator
sudo rabbitmqctl set_permissions -p / bench_user '.*' '.*' '.*'

# Make rabbitmqadmin available
if [ ! -x /usr/local/bin/rabbitmqadmin ]; then
    sudo curl -s http://localhost:15672/cli/rabbitmqadmin -o /usr/local/bin/rabbitmqadmin
    sudo chmod +x /usr/local/bin/rabbitmqadmin
fi

echo ""
echo "RabbitMQ installed:"
sudo rabbitmqctl status | head -5 || true
echo ""
echo "Bench credentials: bench_user / bench_pass"
echo "Management UI: http://localhost:15672 (localhost only)"
