# Queue Architecture Benchmark: PostgreSQL vs Valkey Streams

## Table of Contents

- [Overview](#overview)
- [Research Motivation](#research-motivation)
- [System Architecture](#system-architecture)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Running Benchmarks](#running-benchmarks)
- [Analyzing Results](#analyzing-results)
- [Troubleshooting](#troubleshooting)
- [Configuration](#configuration)
- [Understanding Results](#understanding-results)

---

## Overview

This benchmark framework provides a comprehensive, reproducible methodology for comparing **database-backed queue tables** with **in-memory Valkey Streams** under realistic load conditions.

### What Gets Tested

- **4 Queue Implementations**: 3 PostgreSQL variants + 1 Valkey Streams
- **3 Test Scenarios**: Cold start, warm system, mixed load
- **Key Metrics**: Throughput, tail latency (p50/p95/p99), CPU usage, memory

### Test Duration

- **VM1 (PostgreSQL)**: ~2-2.5 hours (9 tests)
- **VM2 (Valkey)**: ~45 minutes (3 tests)
- **Analysis**: ~5 minutes

---

## Research Motivation

Modern backend systems face a critical architectural decision: use database tables as job queues (simple but potentially slow) or adopt in-memory message streams (complex but potentially faster).

**Research Questions:**

1. Does Valkey Streams reduce database contention compared to DB queues?
2. How does tail latency change under load?
3. Does Valkey preserve throughput when PostgreSQL is busy?
4. Which DB queue optimization performs best?
5. How large is the performance gap under real contention?

---

## System Architecture

### VM1: PostgreSQL Queue Implementations

#### 1. SKIP LOCKED (Classic)

- `SELECT ... FOR UPDATE SKIP LOCKED`
- Standard row-level locking
- Single table with status column

#### 2. DELETE RETURNING (Aggressive)

- Atomic `DELETE ... RETURNING`
- Removes from queue immediately
- Archive table for completed jobs

#### 3. Partitioned (Distributed)

- Hash-partitioned queue (16 partitions)
- Reduces lock contention
- Workers assigned to specific partitions

### VM2: Valkey Streams

- Consumer groups for work distribution
- Automatic message claiming for failed workers
- Pure in-memory (no persistence)
- Redis-compatible protocol

### Test Scenarios

| Scenario              | Description                       | Purpose                     |
| --------------------- | --------------------------------- | --------------------------- |
| **Cold Start**  | Fresh DB/Valkey, no cache         | Measure initial performance |
| **Warm System** | After 60s warmup at 1000 jobs/sec | Measure optimal performance |
| **Mixed Load**  | Concurrent pgbench (10 clients)   | Measure under contention    |

---

## Prerequisites

### Hardware Requirements

- **Minimum**: 4 CPU cores, 8GB RAM, 50GB disk
- **Recommended**: 8 CPU cores, 16GB RAM, 100GB SSD

### Software Requirements

- Ubuntu 22.04 or 24.04 (or Debian-based system)
- Root/sudo access
- Python 3.9+
- Internet connection for package installation

### Two VMs with Identical Specs

Both VMs should have the same CPU, RAM, and disk configuration for fair comparison.

---

## Installation

### Step 1: Extract Archive

```bash
# Extract the benchmark framework
tar -xzf queue-benchmark.tar.gz
cd queue-benchmark

# Make scripts executable
chmod +x setup/*.sh orchestration/*.sh schema/*.sh benchmark/*.py analysis/*.py
```

### Step 2: Install on VM1 (PostgreSQL)

```bash
# Install system dependencies
sudo apt-get update
sudo apt-get install -y git python3 python3-pip sysstat htop iotop

# Install PostgreSQL 16
sudo ./setup/install_pg.sh

# Install Python dependencies
pip3 install -r setup/requirements.txt

# Set up authentication
cat > ~/.pgpass << 'EOF'
localhost:5432:bench_db:bench_user:bench_pass
EOF
chmod 600 ~/.pgpass

# Verify PostgreSQL connection
PGPASSWORD=bench_pass psql -h localhost -U bench_user -d bench_db -c "SELECT 1;"
```

### Step 3: Install on VM2 (PostgreSQL + Valkey)

```bash
# Install system dependencies
sudo apt-get update
sudo apt-get install -y git python3 python3-pip sysstat htop iotop build-essential

# Install PostgreSQL 16
sudo ./setup/install_pg.sh

# Install Valkey 8
sudo ./setup/install_valkey.sh

# Install Python dependencies
pip3 install -r setup/requirements.txt

# Set up authentication
cat > ~/.pgpass << 'EOF'
localhost:5432:bench_db:bench_user:bench_pass
EOF
chmod 600 ~/.pgpass

# Verify installations
PGPASSWORD=bench_pass psql -h localhost -U bench_user -d bench_db -c "SELECT 1;"
valkey-cli ping  # Should return "PONG"
```

### Step 4: Initialize Database Schemas

**On VM1:**

```bash
cd schema/

# Initialize all PostgreSQL queue schemas
PGPASSWORD=bench_pass psql -h localhost -U bench_user -d bench_db -f pg_queue_basic.sql
PGPASSWORD=bench_pass psql -h localhost -U bench_user -d bench_db -f pg_queue_delete_returning.sql
PGPASSWORD=bench_pass psql -h localhost -U bench_user -d bench_db -f pg_queue_partitioned.sql

# Initialize pgbench (for load testing)
PGPASSWORD=bench_pass pgbench -h localhost -U bench_user -d bench_db -i -s 10

cd ..
```

**On VM2:**

```bash
cd schema/

# Initialize PostgreSQL schema (for baseline)
PGPASSWORD=bench_pass psql -h localhost -U bench_user -d bench_db -f pg_queue_basic.sql

# Initialize Valkey Streams
bash init_valkey.sh

# Initialize pgbench
PGPASSWORD=bench_pass pgbench -h localhost -U bench_user -d bench_db -i -s 10

cd ..
```

---

## Running Benchmarks

### VM1: PostgreSQL Benchmarks

```bash
# Set environment variable for authentication
export PGPASSWORD=bench_pass

# Run all PostgreSQL tests (~2-2.5 hours)
./orchestration/run_pg_tests.sh
```

This will run:

- **skip_locked**: cold, warm, load (3 tests)
- **delete_returning**: cold, warm, load (3 tests)
- **partitioned**: cold, warm, load (3 tests)
- **Total**: 9 tests × 15 minutes each

**Monitor progress in another terminal:**

```bash
# Watch queue status
watch -n 2 'PGPASSWORD=bench_pass psql -h localhost -U bench_user -d bench_db -c "SELECT status, count(*) FROM queue_jobs GROUP BY status;"'

# Monitor system resources
htop

# Check disk I/O
iotop
```

**Results saved to:** `results/vm1_pg/`

### VM2: Valkey Benchmarks

```bash
# Set environment variable for authentication
export PGPASSWORD=bench_pass

# Run all Valkey tests (~45 minutes)
./orchestration/run_valkey_tests.sh
```

This will run:

- **valkey_streams**: cold, warm, load (3 tests)

**Monitor progress in another terminal:**

```bash
# Watch Valkey stream
watch -n 2 'valkey-cli XLEN bench_queue'

# Monitor Valkey info
watch -n 2 'valkey-cli INFO stats | grep instantaneous_ops_per_sec'

# Monitor system resources
htop
```

**Results saved to:** `results/vm2_valkey/`

### What Each Test Does

1. **Cleans database/Valkey** (removes old data)
2. **Starts system metrics collection** (CPU, memory, disk I/O)
3. **Starts database/Valkey stats collection**
4. **For "warm" scenario**: Runs 60-second warmup
5. **For "load" scenario**: Starts pgbench in background
6. **Starts 20 worker threads**
7. **Produces 5000 jobs/sec for 5 minutes** (1.5 million jobs total)
8. **Waits for workers to finish**
9. **Collects and saves metrics**

---

## Analyzing Results

### Step 1: Collect Results from Both VMs

**Option A: Analyze on your local machine**

```bash
# On your local machine
mkdir -p queue-benchmark-analysis/results/{vm1_pg,vm2_valkey}
cd queue-benchmark-analysis

# Copy results from VM1
scp -r user@vm1:~/queue-benchmark/results/vm1_pg/* results/vm1_pg/

# Copy results from VM2
scp -r user@vm2:~/queue-benchmark/results/vm2_valkey/* results/vm2_valkey/

# Copy analysis scripts
scp -r user@vm1:~/queue-benchmark/analysis .
```

**Option B: Analyze directly on VM1**

```bash
# On VM1, copy VM2 results
scp -r user@vm2:~/queue-benchmark/results/vm2_valkey results/

# Or if you have shared storage
cp -r /shared/vm2_valkey results/
```

### Step 2: Install Analysis Dependencies

```bash
# Install Python packages for visualization
pip3 install pandas matplotlib seaborn numpy
```

### Step 3: Run Statistical Analysis

```bash
cd analysis/

python3 analyze.py \
    --pg-results ../results/vm1_pg \
    --valkey-results ../results/vm2_valkey \
    --output ../results/analysis
```

**This will:**

- Parse all metrics files (JSONL and CSV)
- Calculate statistics (throughput, latency percentiles, CPU)
- Print comparison tables to console
- Create `results/analysis/summary.csv` and `summary.json`

**Console output example:**

```
================================================================================
BENCHMARK RESULTS SUMMARY
================================================================================

SCENARIO: COLD
────────────────────────────────────────────────────────────────────────────────
Backend         Queue Type           Throughput      p50 (ms)   p95 (ms)   p99 (ms)   CPU %
────────────────────────────────────────────────────────────────────────────────
postgresql      skip_locked          4523 j/s        12.50      45.20      89.30      45.2
postgresql      delete_returning     4789 j/s        11.20      42.10      85.60      48.1
postgresql      partitioned          4956 j/s        10.80      38.50      78.20      43.5
valkey          streams              6234 j/s        5.30       8.90       15.40      28.3
```

### Step 4: Generate Graphs

```bash
python3 generate_graphs.py \
    --input ../results/analysis/summary.csv \
    --output ../results/graphs
```

**This creates 6 PNG files:**

1. **throughput_comparison.png**

   - Bar charts comparing throughput across all implementations
   - Grouped by scenario (cold, warm, load)
2. **latency_comparison.png**

   - Horizontal bar charts for p50, p95, p99 latencies
   - 3×3 grid (3 scenarios × 3 percentiles)
3. **latency_distribution.png**

   - Box plots showing latency distribution
   - Compares all implementations side-by-side
4. **cpu_usage.png**

   - CPU utilization comparison
   - Shows resource efficiency
5. **scenario_comparison.png**

   - Line graphs showing how each implementation performs across scenarios
   - Useful for understanding warmup effects and load impact
6. **valkey_vs_best_pg.png**

   - Direct comparison: Valkey vs best PostgreSQL implementation
   - Shows the maximum performance gap

**All graphs are publication-quality (300 DPI)**

### Step 5: View the Graphs

**On Linux with GUI:**

```bash
xdg-open ../results/graphs/throughput_comparison.png
```

**Copy to local machine:**

```bash
# From your local machine
scp -r user@vm1:~/queue-benchmark/results/graphs ./
```

**In Jupyter Notebook:**

```python
from IPython.display import Image
Image(filename='results/graphs/throughput_comparison.png')
```

---

## Troubleshooting

### PostgreSQL Connection Issues

**Problem:** `FATAL: password authentication failed for user "bench_user"`

**Solution:**

```bash
# Reset password
sudo -u postgres psql -c "ALTER USER bench_user WITH PASSWORD 'bench_pass';"

# Add to pg_hba.conf
echo "host    bench_db        bench_user      127.0.0.1/32            md5" | sudo tee -a /etc/postgresql/16/main/pg_hba.conf

# Reload PostgreSQL
sudo systemctl reload postgresql

# Test connection
PGPASSWORD=bench_pass psql -h localhost -U bench_user -d bench_db -c "SELECT 1;"
```

### Valkey Connection Issues

**Problem:** `valkey-cli` not found or connection refused

**Solution:**

```bash
# Check Valkey status
sudo systemctl status valkey

# View logs
sudo tail -f /var/log/valkey/valkey.log

# Restart Valkey
sudo systemctl restart valkey

# Test connection
valkey-cli ping  # Should return "PONG"
```

### Worker/Producer Hangs

**Problem:** Workers or producer seem stuck

**Solution:**

```bash
# Check if processes are running
ps aux | grep python3

# Check for errors in producer log
tail -f results/vm1_pg/skip_locked_cold_producer.log

# Check database for stuck jobs
PGPASSWORD=bench_pass psql -h localhost -U bench_user -d bench_db -c \
  "SELECT status, count(*), max(created_at) FROM queue_jobs GROUP BY status;"

# Kill stuck processes
pkill -f worker_pg.py
pkill -f producer.py
```

### Disk Space Issues

**Problem:** Out of disk space during benchmark

**Solution:**

```bash
# Check disk usage
df -h

# Clean old results
rm -rf results/vm1_pg/*

# Truncate PostgreSQL tables
PGPASSWORD=bench_pass psql -h localhost -U bench_user -d bench_db -c "TRUNCATE queue_jobs;"

# Vacuum PostgreSQL
PGPASSWORD=bench_pass psql -h localhost -U bench_user -d bench_db -c "VACUUM FULL;"
```

### Analysis Script Errors

**Problem:** `ImportError` or `ModuleNotFoundError`

**Solution:**

```bash
# Install missing packages
pip3 install pandas matplotlib seaborn numpy psutil

# Or reinstall all
pip3 install -r setup/requirements.txt
```

### Graphs Not Generating

**Problem:** Matplotlib backend issues on headless server

**Solution:**

```bash
# Set matplotlib to use non-interactive backend
export MPLBACKEND=Agg

# Then run graph generation
python3 generate_graphs.py --input ../results/analysis/summary.csv
```

---

## Configuration

### Benchmark Parameters

Edit `benchmark/config.py` to adjust test parameters:

```python
BENCHMARK = {
    'production_rate': 1000,        # Jobs per second (default: 1000)
    'production_duration': 180,     # Test duration in seconds (default: 180)
    'num_workers': 20,              # Concurrent workers (default: 20)
    'job_size_bytes': 512,         # Payload size (default: 1KB)
    'job_processing_time_ms': 10,   # Simulated work time (default: 10ms)
}
```

### Database Configuration

Edit `benchmark/config.py` to change connection settings:

```python
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'bench_db',
    'user': 'bench_user',
    'password': 'bench_pass',
}
```

### Valkey Configuration

Edit `benchmark/config.py`:

```python
VALKEY_CONFIG = {
    'host': 'localhost',
    'port': 6379,
    'stream_name': 'bench_queue',
    'consumer_group': 'bench_workers',
}
```

### PostgreSQL Performance Tuning

The installation script configures PostgreSQL with these settings (in `/etc/postgresql/16/main/conf.d/performance.conf`):

```ini
shared_buffers = 2GB
effective_cache_size = 6GB
maintenance_work_mem = 512MB
work_mem = 32MB
max_connections = 200
```

Adjust based on your VM's RAM.

---

## Understanding Results

### Metrics Explained

| Metric                | Description                | Good Value                    |
| --------------------- | -------------------------- | ----------------------------- |
| **Throughput**  | Jobs processed per second  | Higher is better              |
| **p50 Latency** | Median job completion time | Lower is better, typical case |
| **p95 Latency** | 95th percentile latency    | Lower is better, SLA target   |
| **p99 Latency** | 99th percentile latency    | Lower is better, worst case   |
| **CPU Usage**   | Average CPU utilization    | Lower = more efficient        |
| **Memory**      | RAM consumption            | Lower = more efficient        |

### Expected Patterns

**Throughput:**

- Valkey should be 20-40% higher than PostgreSQL
- Partitioned queue should be fastest PostgreSQL variant
- Under load, all implementations degrade somewhat

**Latency:**

- Valkey should have 50-70% lower p95/p99 latency
- Cold start has higher latency than warm
- Under load, tail latency (p99) increases significantly

**CPU Usage:**

- Valkey typically uses 30-50% less CPU
- Partitioned queue uses slightly more CPU (parallelism overhead)
- DELETE RETURNING may show higher CPU due to archiving

### Interpreting Graphs

1. **throughput_comparison.png**

   - Look for consistent performance across scenarios
   - Identify which implementation degrades least under load
2. **latency_comparison.png**

   - Focus on p95 and p99 (these matter for SLAs)
   - Large p99 spikes indicate queueing/contention issues
3. **valkey_vs_best_pg.png**

   - Shows the maximum performance gap
   - Helps justify architectural decision

### Red Flags

- **Throughput drops >50% under load**: Serious contention
- **p99 latency >10x p50**: Queue depth issues
- **CPU at 100%**: System is bottlenecked
- **Metrics missing**: Test failed, check logs

---

## Project Structure

```
queue-benchmark/
├── README.md                    # This file
├── QUICKSTART.md               # Quick reference guide
├── PROJECT_SUMMARY.md          # Research methodology
├── CHECKLIST.md                # Execution checklist
│
├── setup/                      # Installation scripts
│   ├── install_pg.sh          # PostgreSQL 16 setup
│   ├── install_valkey.sh      # Valkey 8 setup
│   └── requirements.txt       # Python dependencies
│
├── schema/                     # Database schemas
│   ├── pg_queue_basic.sql                # SKIP LOCKED implementation
│   ├── pg_queue_delete_returning.sql     # DELETE RETURNING implementation
│   ├── pg_queue_partitioned.sql          # Partitioned queue (16 partitions)
│   └── init_valkey.sh                    # Valkey Streams setup
│
├── benchmark/                  # Core benchmark code
│   ├── config.py              # Configuration (rates, workers, etc.)
│   ├── producer.py            # Job producer (generates load)
│   ├── worker_pg.py           # PostgreSQL queue workers
│   └── worker_valkey.py       # Valkey Streams workers
│
├── orchestration/             # Test execution scripts
│   ├── run_pg_tests.sh        # Run all PostgreSQL tests
│   └── run_valkey_tests.sh    # Run all Valkey tests
│
├── analysis/                  # Results analysis
│   ├── analyze.py             # Statistical analysis
│   └── generate_graphs.py     # Graph generation
│
└── results/                   # Test results (generated)
    ├── vm1_pg/                # PostgreSQL results
    │   ├── skip_locked_cold_metrics.jsonl
    │   ├── skip_locked_cold_system.csv
    │   ├── skip_locked_cold_db_stats.csv
    │   └── ... (27 files total)
    │
    ├── vm2_valkey/            # Valkey results
    │   ├── valkey_cold_metrics.jsonl
    │   ├── valkey_cold_system.csv
    │   ├── valkey_cold_valkey_stats.csv
    │   └── ... (9 files total)
    │
    ├── analysis/              # Analyzed results
    │   ├── summary.csv
    │   └── summary.json
    │
    └── graphs/                # Generated visualizations
        ├── throughput_comparison.png
        ├── latency_comparison.png
        ├── latency_distribution.png
        ├── cpu_usage.png
        ├── scenario_comparison.png
        └── valkey_vs_best_pg.png
```

---

## Quick Reference Commands

### Start Benchmarks

```bash
# VM1 - PostgreSQL
export PGPASSWORD=bench_pass
./orchestration/run_pg_tests.sh

# VM2 - Valkey
export PGPASSWORD=bench_pass
./orchestration/run_valkey_tests.sh
```

### Monitor Progress

```bash
# PostgreSQL queue status
watch -n 2 'PGPASSWORD=bench_pass psql -h localhost -U bench_user -d bench_db -c "SELECT status, count(*) FROM queue_jobs GROUP BY status;"'

# Valkey stream length
watch -n 2 'valkey-cli XLEN bench_queue'

# System resources
htop
```

### Analyze Results

```bash
cd analysis/

# Statistical analysis
python3 analyze.py \
    --pg-results ../results/vm1_pg \
    --valkey-results ../results/vm2_valkey

# Generate graphs
python3 generate_graphs.py \
    --input ../results/analysis/summary.csv
```

### Clean Up

```bash
# Clean PostgreSQL
PGPASSWORD=bench_pass psql -h localhost -U bench_user -d bench_db -c "TRUNCATE queue_jobs;"

# Clean Valkey
valkey-cli FLUSHDB

# Remove old results
rm -rf results/vm1_pg/* results/vm2_valkey/*
```

---

## License

MIT License - Feel free to use, modify, and distribute.

---

**Happy Benchmarking!** 🚀
