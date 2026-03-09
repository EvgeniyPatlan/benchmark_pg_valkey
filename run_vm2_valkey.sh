#!/bin/bash
# ============================================================
# VM2: Valkey Streams Benchmark — Full Setup and Run
# ============================================================
# Target: Fresh Ubuntu 24.04 VM with 8+ vCPU, 16GB+ RAM, 100GB SSD
# Duration: ~3-4 hours (5 runs) or ~45 min (1 run)
#
# This VM needs both Valkey AND PostgreSQL (for pgbench load generation).
#
# Usage:
#   chmod +x run_vm2_valkey.sh
#   ./run_vm2_valkey.sh          # 5 runs per scenario (default)
#   ./run_vm2_valkey.sh 1        # 1 run per scenario (quick smoke test)
# ============================================================
set -e

NUM_RUNS=${1:-5}
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

echo "============================================================"
echo "VM2: Valkey Streams Benchmark"
echo "Runs per scenario: $NUM_RUNS"
echo "============================================================"
echo ""

# ------------------------------------------------------------
# 1. System dependencies
# ------------------------------------------------------------
echo ">>> Installing system dependencies..."
sudo apt-get update -qq
sudo apt-get install -y -qq git python3 python3-pip sysstat htop iotop wget gnupg2 build-essential tcl pkg-config libssl-dev

# ------------------------------------------------------------
# 2. Install PostgreSQL 16 (needed for pgbench load generation)
# ------------------------------------------------------------
echo ""
echo ">>> Installing PostgreSQL 16 (for pgbench)..."
if ! command -v psql &> /dev/null; then
    sudo sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list'
    wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add -
    sudo apt-get update -qq
    sudo apt-get install -y -qq postgresql-16 postgresql-contrib-16 postgresql-client-16
else
    echo "PostgreSQL already installed: $(psql --version)"
fi

# Performance tuning
sudo tee /etc/postgresql/16/main/conf.d/performance.conf > /dev/null <<'PGCONF'
shared_buffers = 2GB
effective_cache_size = 6GB
maintenance_work_mem = 512MB
checkpoint_completion_target = 0.9
wal_buffers = 16MB
random_page_cost = 1.1
effective_io_concurrency = 200
work_mem = 32MB
min_wal_size = 1GB
max_wal_size = 4GB
max_connections = 200
PGCONF

sudo systemctl restart postgresql

# Create benchmark DB and user
sudo -u postgres psql -c "CREATE USER bench_user WITH PASSWORD 'bench_pass';" 2>/dev/null || true
sudo -u postgres psql -c "CREATE DATABASE bench_db OWNER bench_user;" 2>/dev/null || true
sudo -u postgres psql -c "GRANT ALL PRIVILEGES ON DATABASE bench_db TO bench_user;" 2>/dev/null || true

if ! sudo grep -q "bench_user" /etc/postgresql/16/main/pg_hba.conf; then
    echo "host    bench_db        bench_user      127.0.0.1/32            md5" | sudo tee -a /etc/postgresql/16/main/pg_hba.conf
    sudo systemctl reload postgresql
fi

cat > ~/.pgpass <<'EOF'
localhost:5432:bench_db:bench_user:bench_pass
EOF
chmod 600 ~/.pgpass

export PGPASSWORD=bench_pass

echo ""
echo ">>> Verifying PostgreSQL connection..."
PGPASSWORD=bench_pass psql -h localhost -U bench_user -d bench_db -c "SELECT 'PostgreSQL OK' AS status;"

# ------------------------------------------------------------
# 3. Install Valkey 8
# ------------------------------------------------------------
echo ""
echo ">>> Installing Valkey 8..."
if ! command -v valkey-server &> /dev/null; then
    cd /tmp
    wget -q https://github.com/valkey-io/valkey/archive/refs/tags/8.0.1.tar.gz
    tar -xzf 8.0.1.tar.gz
    cd valkey-8.0.1
    make BUILD_TLS=yes -j$(nproc) > /dev/null 2>&1
    sudo make install > /dev/null 2>&1

    sudo useradd -r -s /bin/false valkey 2>/dev/null || true
    sudo mkdir -p /var/lib/valkey /var/log/valkey /etc/valkey
    sudo chown -R valkey:valkey /var/lib/valkey /var/log/valkey

    sudo tee /etc/valkey/valkey.conf > /dev/null <<'VKCONF'
bind 127.0.0.1
port 6379
daemonize no
supervised systemd
loglevel notice
logfile /var/log/valkey/valkey.log
save ""
appendonly no
maxmemory 4gb
maxmemory-policy noeviction
tcp-backlog 511
timeout 0
tcp-keepalive 300
maxclients 10000
dir /var/lib/valkey
stream-node-max-bytes 4096
stream-node-max-entries 100
VKCONF

    sudo tee /etc/systemd/system/valkey.service > /dev/null <<'VKSVC'
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
VKSVC

    sudo systemctl daemon-reload
    sudo systemctl enable valkey
    sudo systemctl start valkey
    sleep 2
    cd "$SCRIPT_DIR"
else
    echo "Valkey already installed: $(valkey-server --version)"
fi

echo ""
echo ">>> Verifying Valkey connection..."
valkey-cli ping

# ------------------------------------------------------------
# 4. Python dependencies
# ------------------------------------------------------------
echo ""
echo ">>> Installing Python dependencies..."
pip3 install -r setup/requirements.txt --break-system-packages -q 2>/dev/null || \
pip3 install -r setup/requirements.txt -q

# ------------------------------------------------------------
# 5. Initialize schemas
# ------------------------------------------------------------
echo ""
echo ">>> Initializing schemas..."
cd schema/

# PG schema (needed for pgbench + baseline)
PGPASSWORD=bench_pass psql -h localhost -U bench_user -d bench_db -f pg_queue_basic.sql
PGPASSWORD=bench_pass pgbench -h localhost -U bench_user -d bench_db -i -s 10 2>/dev/null

# Valkey partitioned streams
bash init_valkey.sh 8

cd "$SCRIPT_DIR"

# ------------------------------------------------------------
# 6. Validate Valkey baseline latency
# ------------------------------------------------------------
echo ""
echo ">>> Validating Valkey baseline latency..."
cd benchmark/
python3 validate_valkey_latency.py
cd "$SCRIPT_DIR"

# ------------------------------------------------------------
# 7. Run benchmarks
# ------------------------------------------------------------
echo ""
echo "============================================================"
echo ">>> Starting Valkey benchmarks ($NUM_RUNS runs per scenario)"
echo ">>> Scenarios: cold, warm, load"
echo ">>> Total test runs: $((3 * NUM_RUNS))"
echo "============================================================"
echo ""

./orchestration/run_valkey_tests.sh "$NUM_RUNS"

# ------------------------------------------------------------
# 8. Optional: Durability test
# ------------------------------------------------------------
echo ""
echo ">>> Running durability test..."
cd benchmark/
sudo python3 test_durability.py --num-jobs 10000 --output ../results/vm2_valkey/durability_results.json || \
    echo "Durability test skipped (needs sudo)"
cd "$SCRIPT_DIR"

# ------------------------------------------------------------
# 9. Done
# ------------------------------------------------------------
echo ""
echo "============================================================"
echo "VM2 COMPLETE"
echo "============================================================"
echo ""
echo "Results are in: results/vm2_valkey/"
echo ""
echo "Next steps:"
echo "  1. Copy results to the analysis machine (or to VM1):"
echo "     scp -r $(whoami)@$(hostname -I | awk '{print $1}'):$SCRIPT_DIR/results/vm2_valkey/ ."
echo ""
echo "  2. Run analysis (on whichever machine has both result sets):"
echo "     cd analysis/"
echo "     python3 analyze.py --pg-results ../results/vm1_pg --valkey-results ../results/vm2_valkey --output ../results/analysis"
echo "     python3 generate_graphs.py --input ../results/analysis/summary.csv --all-runs ../results/analysis/all_runs.csv --output ../results/graphs"
