#!/bin/bash
# ============================================================
# VM1: PostgreSQL Queue Benchmark — Full Setup and Run
# ============================================================
# Target: Fresh Ubuntu 24.04 VM with 8+ vCPU, 16GB+ RAM, 100GB SSD
# Duration: ~10-12 hours (5 runs) or ~2-3 hours (1 run)
#
# Usage:
#   chmod +x run_vm1_pg.sh
#   ./run_vm1_pg.sh          # 5 runs per scenario (default)
#   ./run_vm1_pg.sh 1        # 1 run per scenario (quick smoke test)
# ============================================================
set -e

NUM_RUNS=${1:-5}
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

echo "============================================================"
echo "VM1: PostgreSQL Queue Benchmark"
echo "Runs per scenario: $NUM_RUNS"
echo "============================================================"
echo ""

# ------------------------------------------------------------
# 1. System dependencies
# ------------------------------------------------------------
echo ">>> Installing system dependencies..."
sudo apt-get update -qq
sudo apt-get install -y -qq git python3 python3-pip sysstat htop iotop wget gnupg2

# ------------------------------------------------------------
# 2. Install PostgreSQL 16
# ------------------------------------------------------------
echo ""
echo ">>> Installing PostgreSQL 16..."
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

# Create benchmark DB and user (ignore errors if already exists)
sudo -u postgres psql -c "CREATE USER bench_user WITH PASSWORD 'bench_pass';" 2>/dev/null || true
sudo -u postgres psql -c "CREATE DATABASE bench_db OWNER bench_user;" 2>/dev/null || true
sudo -u postgres psql -c "GRANT ALL PRIVILEGES ON DATABASE bench_db TO bench_user;" 2>/dev/null || true

# Ensure pg_hba allows local md5 auth (idempotent)
if ! sudo grep -q "bench_user" /etc/postgresql/16/main/pg_hba.conf; then
    echo "host    bench_db        bench_user      127.0.0.1/32            md5" | sudo tee -a /etc/postgresql/16/main/pg_hba.conf
    sudo systemctl reload postgresql
fi

# .pgpass
cat > ~/.pgpass <<'EOF'
localhost:5432:bench_db:bench_user:bench_pass
EOF
chmod 600 ~/.pgpass

export PGPASSWORD=bench_pass

# Verify connection
echo ""
echo ">>> Verifying PostgreSQL connection..."
PGPASSWORD=bench_pass psql -h localhost -U bench_user -d bench_db -c "SELECT 'PostgreSQL OK' AS status;"

# ------------------------------------------------------------
# 3. Python dependencies
# ------------------------------------------------------------
echo ""
echo ">>> Installing Python dependencies..."
pip3 install -r setup/requirements.txt --break-system-packages -q 2>/dev/null || \
pip3 install -r setup/requirements.txt -q

# ------------------------------------------------------------
# 4. Initialize schemas
# ------------------------------------------------------------
echo ""
echo ">>> Initializing database schemas..."
cd schema/
PGPASSWORD=bench_pass psql -h localhost -U bench_user -d bench_db -f pg_queue_basic.sql
PGPASSWORD=bench_pass psql -h localhost -U bench_user -d bench_db -f pg_queue_delete_returning.sql
PGPASSWORD=bench_pass psql -h localhost -U bench_user -d bench_db -f pg_queue_partitioned.sql
PGPASSWORD=bench_pass pgbench -h localhost -U bench_user -d bench_db -i -s 10 2>/dev/null
cd "$SCRIPT_DIR"

# ------------------------------------------------------------
# 5. Verify setup
# ------------------------------------------------------------
echo ""
echo ">>> Verifying setup..."
bash verify_setup.sh || true

# ------------------------------------------------------------
# 6. Run benchmarks
# ------------------------------------------------------------
echo ""
echo "============================================================"
echo ">>> Starting PostgreSQL benchmarks ($NUM_RUNS runs per scenario)"
echo ">>> Queue types: skip_locked, delete_returning, partitioned"
echo ">>> Scenarios: cold, warm, load"
echo ">>> Total test runs: $((3 * 3 * NUM_RUNS))"
echo "============================================================"
echo ""

./orchestration/run_pg_tests.sh "$NUM_RUNS"

# ------------------------------------------------------------
# 7. Done
# ------------------------------------------------------------
echo ""
echo "============================================================"
echo "VM1 COMPLETE"
echo "============================================================"
echo ""
echo "Results are in: results/vm1_pg/"
echo ""
echo "Next steps:"
echo "  1. Copy results to the analysis machine:"
echo "     scp -r $(whoami)@$(hostname -I | awk '{print $1}'):$SCRIPT_DIR/results/vm1_pg/ ."
echo ""
echo "  2. Or copy Valkey results here and analyze locally:"
echo "     scp -r user@valkey-vm:~/benchmark_pg_valkey/results/vm2_valkey/ results/"
echo "     cd analysis/"
echo "     python3 analyze.py --pg-results ../results/vm1_pg --valkey-results ../results/vm2_valkey --output ../results/analysis"
echo "     python3 generate_graphs.py --input ../results/analysis/summary.csv --all-runs ../results/analysis/all_runs.csv --output ../results/graphs"
