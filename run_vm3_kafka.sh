#!/bin/bash
# ============================================================
# VM3: Kafka Benchmark — Full Setup and Run
# ============================================================
# Target: Fresh Ubuntu 24.04 VM with 8+ vCPU, 16GB+ RAM, 100GB SSD
# Duration: ~4-5 hours (5 runs) or ~1h (1 run)
#
# This VM needs Java 17, Kafka, AND PostgreSQL (for pgbench load).
#
# Usage:
#   chmod +x run_vm3_kafka.sh
#   ./run_vm3_kafka.sh          # 5 runs per scenario (default)
#   ./run_vm3_kafka.sh 1        # 1 run per scenario (smoke test)
# ============================================================
set -e

NUM_RUNS=${1:-5}
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

echo "============================================================"
echo "VM3: Kafka Benchmark"
echo "Runs per scenario: $NUM_RUNS"
echo "============================================================"

# ------------------------------------------------------------
# 1. System dependencies
# ------------------------------------------------------------
echo ""
echo ">>> Installing system dependencies..."
sudo apt-get update -qq
sudo apt-get install -y -qq git python3 python3-pip sysstat htop iotop wget gnupg2

# ------------------------------------------------------------
# 2. Install PostgreSQL 16 (for pgbench load + baseline)
# ------------------------------------------------------------
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

# ------------------------------------------------------------
# 3. Install Kafka
# ------------------------------------------------------------
echo ""
echo ">>> Installing Kafka..."
sudo bash setup/install_kafka.sh

# ------------------------------------------------------------
# 4. Install Python dependencies
# ------------------------------------------------------------
echo ""
echo ">>> Installing Python dependencies..."
pip3 install -r setup/requirements.txt --break-system-packages -q 2>/dev/null || \
pip3 install -r setup/requirements.txt -q

# ------------------------------------------------------------
# 5. PG schema + pgbench init (needed for load scenario)
# ------------------------------------------------------------
echo ""
echo ">>> Initializing PostgreSQL..."
cat > ~/.pgpass <<'EOF'
localhost:5432:bench_db:bench_user:bench_pass
EOF
chmod 600 ~/.pgpass

PGPASSWORD=bench_pass pgbench -h localhost -U bench_user -d bench_db -i -s 10 2>/dev/null

# ------------------------------------------------------------
# 6. Initialize Kafka topic
# ------------------------------------------------------------
echo ""
echo ">>> Initializing Kafka topic..."
bash schema/init_kafka.sh

# ------------------------------------------------------------
# 7. Verify setup
# ------------------------------------------------------------
echo ""
echo ">>> Verifying setup..."
PGPASSWORD=bench_pass psql -h localhost -U bench_user -d bench_db -c "SELECT 1;" > /dev/null && echo "  PostgreSQL OK"

# Kafka verify via librdkafka (no JVM) — kafka-topics.sh hangs on
# single-node KRaft during initial metadata fetch in some VM setups.
python3 - <<'PY'
import sys
from confluent_kafka.admin import AdminClient
try:
    md = AdminClient({'bootstrap.servers': 'localhost:9092',
                      'socket.timeout.ms': 5000}).list_topics(timeout=10)
    if 'bench_queue' in md.topics:
        print("  Kafka topic OK")
    else:
        print("  Kafka topic MISSING (reachable but no bench_queue topic)")
        sys.exit(1)
except Exception as e:
    print(f"  Kafka UNREACHABLE: {e}")
    sys.exit(1)
PY

# ------------------------------------------------------------
# 8. Run benchmarks
# ------------------------------------------------------------
echo ""
echo ">>> Starting Kafka benchmarks ($NUM_RUNS runs per scenario)..."
export PGPASSWORD=bench_pass
bash orchestration/run_kafka_tests.sh "$NUM_RUNS"

echo ""
echo "============================================================"
echo "VM3 Kafka benchmark complete. Results in: results/vm3_kafka/"
echo "============================================================"
