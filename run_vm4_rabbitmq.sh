#!/bin/bash
# ============================================================
# VM4: RabbitMQ Benchmark — Full Setup and Run
# ============================================================
# Target: Fresh Ubuntu 24.04 VM with 8+ vCPU, 16GB+ RAM, 100GB SSD
# Duration: ~6-8 hours (5 runs, 2 variants) or ~1.5h (1 run)
#
# This VM needs RabbitMQ + Erlang + PostgreSQL (for pgbench load).
#
# Usage:
#   chmod +x run_vm4_rabbitmq.sh
#   ./run_vm4_rabbitmq.sh          # 5 runs per scenario (default)
#   ./run_vm4_rabbitmq.sh 1        # smoke test
# ============================================================
set -e

NUM_RUNS=${1:-5}
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

echo "============================================================"
echo "VM4: RabbitMQ Benchmark"
echo "Runs per scenario: $NUM_RUNS"
echo "============================================================"

echo ""
echo ">>> Installing system dependencies..."
sudo apt-get update -qq
sudo apt-get install -y -qq git python3 python3-pip sysstat htop iotop wget gnupg2

echo ""
echo ">>> Installing PostgreSQL 16 (for pgbench)..."
if ! command -v psql &> /dev/null; then
    sudo sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list'
    wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add -
    sudo apt-get update -qq
    sudo apt-get install -y -qq postgresql-16 postgresql-contrib-16 postgresql-client-16
fi

sudo -u postgres psql -c "CREATE USER bench_user WITH PASSWORD 'bench_pass';" 2>/dev/null || \
    sudo -u postgres psql -c "ALTER USER bench_user WITH PASSWORD 'bench_pass';"
sudo -u postgres psql -c "CREATE DATABASE bench_db OWNER bench_user;" 2>/dev/null || true
sudo -u postgres psql -c "GRANT ALL PRIVILEGES ON DATABASE bench_db TO bench_user;"

echo "host    bench_db        bench_user      127.0.0.1/32            md5" | sudo tee -a /etc/postgresql/16/main/pg_hba.conf > /dev/null
sudo systemctl reload postgresql

echo ""
echo ">>> Installing RabbitMQ..."
sudo bash setup/install_rabbitmq.sh

echo ""
echo ">>> Installing Python dependencies..."
pip3 install -r setup/requirements.txt --break-system-packages -q 2>/dev/null || \
pip3 install -r setup/requirements.txt -q

echo ""
echo ">>> Initializing PostgreSQL..."
cat > ~/.pgpass <<'EOF'
localhost:5432:bench_db:bench_user:bench_pass
EOF
chmod 600 ~/.pgpass

PGPASSWORD=bench_pass pgbench -h localhost -U bench_user -d bench_db -i -s 10 2>/dev/null

echo ""
echo ">>> Initializing RabbitMQ queues..."
bash schema/init_rabbitmq.sh

echo ""
echo ">>> Verifying setup..."
PGPASSWORD=bench_pass psql -h localhost -U bench_user -d bench_db -c "SELECT 1;" > /dev/null && echo "  PostgreSQL OK"
sudo rabbitmqctl list_queues | grep -q bench_queue && echo "  RabbitMQ queues OK"

echo ""
echo ">>> Starting RabbitMQ benchmarks ($NUM_RUNS runs per scenario)..."
export PGPASSWORD=bench_pass
bash orchestration/run_rabbitmq_tests.sh "$NUM_RUNS"

echo ""
echo "============================================================"
echo "VM4 RabbitMQ benchmark complete. Results in: results/vm4_rabbitmq/"
echo "============================================================"
