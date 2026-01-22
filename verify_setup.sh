#!/bin/bash
# Setup Verification and Fix Script
# Run this before starting benchmarks to ensure everything is configured correctly

set -e

echo "=========================================="
echo "Queue Benchmark - Setup Verification"
echo "=========================================="
echo ""

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print status
print_status() {
    if [ $1 -eq 0 ]; then
        echo -e "${GREEN}✓${NC} $2"
    else
        echo -e "${RED}✗${NC} $2"
    fi
}

# Check PostgreSQL
echo "1. Checking PostgreSQL..."
sudo systemctl status postgresql > /dev/null 2>&1
print_status $? "PostgreSQL service is running"

# Check database exists
PGPASSWORD=bench_pass psql -h localhost -U bench_user -d bench_db -c "SELECT 1;" > /dev/null 2>&1
DB_STATUS=$?
print_status $DB_STATUS "Can connect to bench_db"

if [ $DB_STATUS -ne 0 ]; then
    echo -e "${YELLOW}Fixing PostgreSQL authentication...${NC}"
    sudo -u postgres psql -c "ALTER USER bench_user WITH PASSWORD 'bench_pass';" 
    echo "host    bench_db        bench_user      127.0.0.1/32            md5" | sudo tee -a /etc/postgresql/16/main/pg_hba.conf > /dev/null
    sudo systemctl reload postgresql
    sleep 2
    
    PGPASSWORD=bench_pass psql -h localhost -U bench_user -d bench_db -c "SELECT 1;" > /dev/null 2>&1
    print_status $? "PostgreSQL authentication fixed"
fi

# Set up .pgpass
echo ""
echo "2. Setting up .pgpass file..."
cat > ~/.pgpass << 'EOF'
localhost:5432:bench_db:bench_user:bench_pass
EOF
chmod 600 ~/.pgpass
print_status $? ".pgpass file created"

# Check Python packages
echo ""
echo "3. Checking Python packages..."
python3 -c "import psycopg2" > /dev/null 2>&1
print_status $? "psycopg2 installed"

python3 -c "import pandas" > /dev/null 2>&1
PANDAS_STATUS=$?
print_status $PANDAS_STATUS "pandas installed"

python3 -c "import matplotlib" > /dev/null 2>&1
MATPLOTLIB_STATUS=$?
print_status $MATPLOTLIB_STATUS "matplotlib installed"

# Try to import valkey (optional for VM1)
python3 -c "import valkey" > /dev/null 2>&1
VALKEY_STATUS=$?
if [ $VALKEY_STATUS -eq 0 ]; then
    print_status 0 "valkey module installed"
else
    echo -e "${YELLOW}  valkey module not installed (OK for VM1, required for VM2)${NC}"
fi

if [ $PANDAS_STATUS -ne 0 ] || [ $MATPLOTLIB_STATUS -ne 0 ]; then
    echo ""
    echo -e "${YELLOW}Installing missing Python packages...${NC}"
    pip3 install pandas matplotlib seaborn numpy
fi

# Check schemas
echo ""
echo "4. Checking database schemas..."
PGPASSWORD=bench_pass psql -h localhost -U bench_user -d bench_db -c "\dt queue_jobs" > /dev/null 2>&1
print_status $? "queue_jobs table exists"

PGPASSWORD=bench_pass psql -h localhost -U bench_user -d bench_db -c "\dt queue_jobs_dr" > /dev/null 2>&1
print_status $? "queue_jobs_dr table exists"

PGPASSWORD=bench_pass psql -h localhost -U bench_user -d bench_db -c "\dt queue_jobs_part" > /dev/null 2>&1
print_status $? "queue_jobs_part table exists"

# Check pgbench
echo ""
echo "5. Checking pgbench..."
PGPASSWORD=bench_pass psql -h localhost -U bench_user -d bench_db -c "\dt pgbench_accounts" > /dev/null 2>&1
PGBENCH_STATUS=$?
print_status $PGBENCH_STATUS "pgbench tables initialized"

if [ $PGBENCH_STATUS -ne 0 ]; then
    echo -e "${YELLOW}Initializing pgbench...${NC}"
    PGPASSWORD=bench_pass pgbench -h localhost -U bench_user -d bench_db -i -s 10
    print_status $? "pgbench initialized"
fi

# Test producer
echo ""
echo "6. Testing producer..."
cd benchmark
python3 producer.py --backend pg --queue-type skip_locked --rate 10 --duration 1 > /tmp/producer_test.log 2>&1
PRODUCER_STATUS=$?
print_status $PRODUCER_STATUS "Producer runs successfully"

if [ $PRODUCER_STATUS -ne 0 ]; then
    echo -e "${RED}Producer error:${NC}"
    cat /tmp/producer_test.log
fi

# Test worker
echo ""
echo "7. Testing worker..."
timeout 2 python3 worker_pg.py --queue-type skip_locked --workers 1 --output /tmp/worker_test.jsonl > /dev/null 2>&1 || true
if [ -f /tmp/worker_test.jsonl ]; then
    print_status 0 "Worker runs successfully"
else
    print_status 1 "Worker test failed"
fi

cd ..

# Summary
echo ""
echo "=========================================="
echo "Setup Status Summary"
echo "=========================================="
echo ""

if [ $DB_STATUS -eq 0 ] && [ $PRODUCER_STATUS -eq 0 ]; then
    echo -e "${GREEN}✓ All checks passed! Ready to run benchmarks.${NC}"
    echo ""
    echo "To start benchmarks:"
    echo "  export PGPASSWORD=bench_pass"
    echo "  cd orchestration"
    echo "  ./run_pg_tests.sh"
else
    echo -e "${RED}✗ Some checks failed. Please review errors above.${NC}"
fi

echo ""
