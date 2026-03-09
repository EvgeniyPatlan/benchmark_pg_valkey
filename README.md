# Queue Architecture Benchmark: PostgreSQL vs Valkey Streams

## Table of Contents

- [Overview](#overview)
- [Research Framing](#research-framing)
- [Decision Guide](#decision-guide)
- [Test Environment](#test-environment)
- [System Architecture](#system-architecture)
- [The Batching Discovery](#the-batching-discovery)
- [Quick Start (VM Scripts)](#quick-start-vm-scripts)
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

This is not a fight. It is a benchmark to help you understand when each tool fits. PostgreSQL wins on durability, simplicity, and median latency. Valkey wins on throughput stability, tail latency, and CPU efficiency under load.

### What Gets Tested

- **4 Queue Implementations**: 3 PostgreSQL variants + 1 Valkey Streams
- **3 Test Scenarios**: Cold start, warm system, mixed load
- **5 Runs Per Scenario**: For statistical rigor (mean +/- stddev)
- **Key Metrics**: Throughput, tail latency (p50/p95/p99), CPU usage, memory

### Test Duration

- **VM1 (PostgreSQL)**: ~10-12 hours (9 tests x 5 runs each)
- **VM2 (Valkey)**: ~3-4 hours (3 tests x 5 runs each)
- **Analysis**: ~5 minutes

---

## Research Framing

### Benchmark Fairness Principle

- **Fairness means realistic deployments**, not hardware parity.
- The comparison is: **1 PostgreSQL node** (how most teams deploy a PG-backed queue) vs **1 Valkey standalone node** (properly configured, no clustering).
- A Valkey cluster (3+ nodes) vs 1 PG node would be unfair.
- A misconfigured single-node Valkey (hot-shard) would also be unfair.
- Each system is tested at its **best single-node configuration**, matching what a real team would deploy.

### Research Questions

1. Does Valkey Streams reduce database contention compared to DB queues?
2. How does tail latency change under load?
3. Does Valkey preserve throughput when PostgreSQL is busy?
4. Which DB queue optimization performs best?
5. How large is the performance gap under real contention?

---

## Decision Guide

Use this quick-reference to decide which queue technology fits your use case:

| Criterion | PostgreSQL Queue | Valkey Streams |
|---|---|---|
| **Throughput** | < 500 jobs/sec | > 500 jobs/sec |
| **Traffic pattern** | Stable / predictable | Bursty / variable |
| **p95/p99 SLA** | Not required | Required |
| **Job durability** | Critical (financial) | Best-effort acceptable |
| **Operational overhead** | Prefer fewer systems | Dedicated infra OK |
| **Query flexibility** | Needed (filter/report) | Not needed |

**Use PostgreSQL** for payment processing, user operations, small teams preferring simplicity, and moderate throughput (<500 j/s).

**Use Valkey** for analytics pipelines, event processing, high-volume background jobs, and systems with strict latency SLAs.

**Hybrid approach**: Use PostgreSQL's durability for critical operations, Valkey's performance for high volume.

---

## Test Environment

Every benchmark run automatically saves a full environment specification to `results/*/environment.txt`. You must document:

| Parameter | VM1 (PostgreSQL) | VM2 (Valkey) |
|---|---|---|
| **Scope** | 1 PostgreSQL node | 1 Valkey standalone node |
| **OS** | Ubuntu 22.04/24.04 | Ubuntu 22.04/24.04 |
| **vCPU** | 8 (recommended) | 8 (recommended) |
| **RAM** | 16GB (recommended) | 16GB (recommended) |
| **Disk** | 100GB SSD | 100GB SSD |
| **PostgreSQL** | 16.x | 16.x (for pgbench) |
| **Valkey** | - | 8.0.1 |
| **Python** | 3.9+ | 3.9+ |
| **Driver** | psycopg2 2.9.9 | valkey-py |

### Load Scenario Configuration

The "load" scenario uses **pgbench** to simulate concurrent transactional traffic:

| Parameter | Value |
|---|---|
| **Tool** | pgbench (PostgreSQL built-in TPC-B-like benchmark) |
| **Clients** | 10 concurrent connections |
| **Threads** | 4 |
| **Duration** | 320 seconds (covers test + drain) |
| **Scale factor** | 10 (~1.6M rows in pgbench_accounts) |
| **Query type** | Standard TPC-B-like OLTP mix (SELECT/UPDATE/INSERT) |
| **Purpose** | Simulate real application traffic competing for DB resources |

---

## System Architecture

### VM1: PostgreSQL Queue Implementations

#### 1. SKIP LOCKED (Classic) — Most Common Production Default

- `SELECT ... FOR UPDATE SKIP LOCKED`
- Standard row-level locking
- Single table with status column
- **Why included**: This is what most teams actually use. Represents the realistic baseline.

#### 2. DELETE RETURNING (Aggressive) — Optimized for Throughput

- Atomic `DELETE ... RETURNING`
- Removes from queue immediately, archives completed jobs
- **Why included**: Represents the best-case optimization where jobs are immediately removed, reducing table bloat.

#### 3. Partitioned (Distributed) — Reduces Lock Contention

- Hash-partitioned queue (16 partitions)
- Reduces lock contention by distributing workers across partitions
- **Why included**: Represents the most advanced PG queue optimization, reducing contention at the cost of complexity.

### VM2: Valkey Streams

- **8 partitioned stream keys** (`bench_queue:0` through `bench_queue:7`) to avoid hot-shard antipattern
- Consumer groups for work distribution across all partitions
- Batch reads (50 messages per XREADGROUP) and batch acknowledgments
- Automatic message claiming for failed workers
- Pure in-memory (no persistence by default)
- Redis-compatible protocol

### Test Scenarios

| Scenario | Description | Purpose |
|---|---|---|
| **Cold Start** | Fresh DB/Valkey, no cache | Measure initial performance |
| **Warm System** | After 60s warmup at 1000 jobs/sec | Measure optimal performance |
| **Mixed Load** | Concurrent pgbench (10 clients, 4 threads, TPC-B OLTP) | Measure under contention |

---

## The Batching Discovery

**The single most important configuration decision for Valkey queue performance.**

Batch size dramatically affects Valkey Streams performance:

| Metric | batch_size=1 | batch_size=50 | Improvement |
|---|---|---|---|
| Throughput | ~6% better than PG | ~7-9% better than PG | +6% |
| p95 Latency | Similar to PG | 223x better than PG | 223x |
| CPU Usage | Similar to PG | 70-75% less than PG | -25% |

With `batch_size=1`, Valkey performance was mediocre — each XREADGROUP/XACK round-trip has overhead that dominates at single-message granularity. With `batch_size=50`, the amortized overhead per message drops dramatically.

**Configuration** (in `benchmark/config.py`):
```python
'valkey_worker_batch_size': 50,  # Messages per XREADGROUP call
'valkey_worker_poll_interval_ms': 100,  # Block time for batching
```

---

## Quick Start (VM Scripts)

The fastest way to run the full benchmark is with the self-contained VM scripts. Each script handles all installation, configuration, schema setup, and benchmark execution on a fresh Ubuntu 24.04 VM.

### VM1: PostgreSQL Benchmark

```bash
chmod +x run_vm1_pg.sh
./run_vm1_pg.sh          # 5 runs per scenario (default, ~10-12 hours)
./run_vm1_pg.sh 1        # 1 run per scenario (quick smoke test, ~2-3 hours)
```

Installs PostgreSQL 16, creates the benchmark database, initializes all three queue schemas (skip_locked, delete_returning, partitioned), and runs the full benchmark suite.

### VM2: Valkey Benchmark

```bash
chmod +x run_vm2_valkey.sh
./run_vm2_valkey.sh      # 5 runs per scenario (default, ~3-4 hours)
./run_vm2_valkey.sh 1    # 1 run per scenario (quick smoke test, ~45 min)
```

Installs PostgreSQL 16 (for pgbench load generation), compiles Valkey 8 from source, sets up partitioned streams, validates baseline latency, runs the benchmark suite, and runs the durability test.

### After Both VMs Finish

Collect results from both VMs and run analysis:

```bash
# On either VM (or a third machine):
mkdir -p results/{vm1_pg,vm2_valkey}
scp -r user@pg-vm:~/benchmark_pg_valkey/results/vm1_pg/* results/vm1_pg/
scp -r user@valkey-vm:~/benchmark_pg_valkey/results/vm2_valkey/* results/vm2_valkey/

cd analysis/
python3 analyze.py \
    --pg-results ../results/vm1_pg \
    --valkey-results ../results/vm2_valkey \
    --output ../results/analysis

python3 generate_graphs.py \
    --input ../results/analysis/summary.csv \
    --all-runs ../results/analysis/all_runs.csv \
    --output ../results/graphs
```

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
tar -xzf queue-benchmark.tar.gz
cd queue-benchmark
chmod +x setup/*.sh orchestration/*.sh schema/*.sh benchmark/*.py analysis/*.py
```

### Step 2: Install on VM1 (PostgreSQL)

```bash
sudo apt-get update
sudo apt-get install -y git python3 python3-pip sysstat htop iotop

sudo ./setup/install_pg.sh
pip3 install -r setup/requirements.txt

cat > ~/.pgpass << 'EOF'
localhost:5432:bench_db:bench_user:bench_pass
EOF
chmod 600 ~/.pgpass

PGPASSWORD=bench_pass psql -h localhost -U bench_user -d bench_db -c "SELECT 1;"
```

### Step 3: Install on VM2 (PostgreSQL + Valkey)

```bash
sudo apt-get update
sudo apt-get install -y git python3 python3-pip sysstat htop iotop build-essential

sudo ./setup/install_pg.sh
sudo ./setup/install_valkey.sh
pip3 install -r setup/requirements.txt

cat > ~/.pgpass << 'EOF'
localhost:5432:bench_db:bench_user:bench_pass
EOF
chmod 600 ~/.pgpass

PGPASSWORD=bench_pass psql -h localhost -U bench_user -d bench_db -c "SELECT 1;"
valkey-cli ping  # Should return "PONG"
```

### Step 4: Initialize Database Schemas

**On VM1:**

```bash
cd schema/
PGPASSWORD=bench_pass psql -h localhost -U bench_user -d bench_db -f pg_queue_basic.sql
PGPASSWORD=bench_pass psql -h localhost -U bench_user -d bench_db -f pg_queue_delete_returning.sql
PGPASSWORD=bench_pass psql -h localhost -U bench_user -d bench_db -f pg_queue_partitioned.sql
PGPASSWORD=bench_pass pgbench -h localhost -U bench_user -d bench_db -i -s 10
cd ..
```

**On VM2:**

```bash
cd schema/
PGPASSWORD=bench_pass psql -h localhost -U bench_user -d bench_db -f pg_queue_basic.sql
bash init_valkey.sh 8  # Creates 8 partitioned stream keys
PGPASSWORD=bench_pass pgbench -h localhost -U bench_user -d bench_db -i -s 10
cd ..
```

---

## Running Benchmarks

### Validate Valkey Baseline Latency (Recommended First Step)

Before running the full benchmark, validate that Valkey latency is reasonable:

```bash
cd benchmark/
python3 validate_valkey_latency.py
```

Expected results for a properly configured single-node Valkey:
- **p50**: < 5ms (if > 20ms, investigate hot-shard, network RTT, or persistence)
- **p95**: < 10ms
- **p99**: < 20ms

### VM1: PostgreSQL Benchmarks

```bash
export PGPASSWORD=bench_pass
./orchestration/run_pg_tests.sh [NUM_RUNS]  # Default: 5 runs per scenario
```

This runs 9 test configurations x NUM_RUNS = 45 total runs:
- **skip_locked**: cold, warm, load (x5 each)
- **delete_returning**: cold, warm, load (x5 each)
- **partitioned**: cold, warm, load (x5 each)

**Results saved to:** `results/vm1_pg/`

### VM2: Valkey Benchmarks

```bash
export PGPASSWORD=bench_pass
./orchestration/run_valkey_tests.sh [NUM_RUNS]  # Default: 5 runs per scenario
```

This runs 3 scenarios x NUM_RUNS = 15 total runs.

**Results saved to:** `results/vm2_valkey/`

### What Each Test Does

1. Cleans database/Valkey (removes old data)
2. Starts system metrics collection (CPU, memory, disk I/O)
3. Starts database/Valkey stats collection
4. For "warm" scenario: Runs 60-second warmup
5. For "load" scenario: Starts pgbench (10 clients, 4 threads, TPC-B OLTP) + measures app query latency
6. Starts 20 worker threads
7. Produces 1000 jobs/sec for 180 seconds (180,000 jobs total)
8. Waits for workers to drain the queue
9. Collects and saves metrics

---

## Analyzing Results

### Step 1: Collect Results from Both VMs

```bash
mkdir -p queue-benchmark-analysis/results/{vm1_pg,vm2_valkey}
cd queue-benchmark-analysis

scp -r user@vm1:~/queue-benchmark/results/vm1_pg/* results/vm1_pg/
scp -r user@vm2:~/queue-benchmark/results/vm2_valkey/* results/vm2_valkey/
scp -r user@vm1:~/queue-benchmark/analysis .
```

### Step 2: Run Statistical Analysis

```bash
cd analysis/

python3 analyze.py \
    --pg-results ../results/vm1_pg \
    --valkey-results ../results/vm2_valkey \
    --output ../results/analysis
```

This will:
- Parse all metrics files across all runs
- Calculate mean +/- stddev and 95% confidence intervals
- Print comparison tables with statistical measures
- Create `results/analysis/summary.csv`, `summary.json`, and `all_runs.csv`

### Step 3: Generate Graphs

```bash
python3 generate_graphs.py \
    --input ../results/analysis/summary.csv \
    --all-runs ../results/analysis/all_runs.csv \
    --output ../results/graphs
```

**This creates 7 PNG files (300 DPI):**

1. **throughput_comparison.png** — Bar charts with error bars across scenarios
2. **latency_comparison.png** — 3x3 grid of latency percentiles with error bars
3. **latency_distribution.png** — Annotated box plots (Valkey = tight, PG = wide)
4. **cpu_usage.png** — CPU utilization comparison with error bars
5. **scenario_comparison.png** — Line graphs showing degradation under stress + stability metric
6. **valkey_vs_best_pg.png** — Direct comparison with percentage differences
7. **decision_guide.png** — Visual decision reference table

### Step 4: Run Durability Test (Optional)

```bash
cd benchmark/
sudo python3 test_durability.py --num-jobs 10000
```

Tests crash recovery under different Valkey persistence modes and compares to PostgreSQL.

---

## Troubleshooting

### PostgreSQL Connection Issues

```bash
sudo -u postgres psql -c "ALTER USER bench_user WITH PASSWORD 'bench_pass';"
echo "host    bench_db        bench_user      127.0.0.1/32            md5" | sudo tee -a /etc/postgresql/16/main/pg_hba.conf
sudo systemctl reload postgresql
```

### Valkey Connection Issues

```bash
sudo systemctl status valkey
sudo tail -f /var/log/valkey/valkey.log
sudo systemctl restart valkey
valkey-cli ping
```

### High Valkey Latency (p50 > 20ms)

If Valkey p50 latency is unexpectedly high:

1. **Run the validation script**: `python3 validate_valkey_latency.py`
2. **Check if using partitioned streams**: Single-key (`bench_queue`) serializes all traffic. Use 8 partitioned keys.
3. **Check persistence**: `valkey-cli CONFIG GET appendonly` — if `yes`, this adds latency
4. **Check network**: `valkey-cli --latency` — should be < 1ms for localhost
5. **Check poll interval**: Higher `valkey_worker_poll_interval_ms` increases apparent latency

### Worker/Producer Hangs

```bash
ps aux | grep python3
tail -f results/vm1_pg/skip_locked_cold_run1_producer.log
pkill -f worker_pg.py
pkill -f producer.py
```

### Disk Space Issues

```bash
df -h
rm -rf results/vm1_pg/*
PGPASSWORD=bench_pass psql -h localhost -U bench_user -d bench_db -c "TRUNCATE queue_jobs;"
PGPASSWORD=bench_pass psql -h localhost -U bench_user -d bench_db -c "VACUUM FULL;"
```

---

## Configuration

### Benchmark Parameters

Edit `benchmark/config.py`:

```python
BENCHMARK = {
    'production_rate': 1000,        # Jobs per second
    'production_duration': 180,     # Test duration in seconds
    'num_workers': 10,              # Concurrent workers (orchestration scripts use 20)
    'job_size_bytes': 512,          # Payload size
    'job_processing_time_ms': 5,    # Simulated work time

    # PG workers: single-row fetches, fast polling
    'pg_worker_batch_size': 1,
    'pg_worker_poll_interval_ms': 10,

    # Valkey workers: batch reads, longer block time
    'valkey_worker_batch_size': 50,
    'valkey_worker_poll_interval_ms': 100,

    'num_runs': 5,                  # Runs per scenario for statistical rigor
}
```

### Valkey Stream Partitioning

```python
VALKEY_CONFIG = {
    'stream_name': 'bench_queue',       # prefix
    'consumer_group': 'bench_workers',
    'num_stream_partitions': 8,         # bench_queue:0 .. bench_queue:7
}
```

### Database Configuration

```python
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'bench_db',
    'user': 'bench_user',
    'password': 'bench_pass',
}
```

---

## Understanding Results

### Metrics Explained

| Metric | Description | Good Value |
|---|---|---|
| **Throughput** | Jobs processed per second (mean +/- stddev) | Higher is better |
| **p50 Latency** | Median job completion time | Lower is better |
| **p95 Latency** | 95th percentile latency (SLA target) | Lower is better |
| **p99 Latency** | 99th percentile latency (worst case) | Lower is better |
| **CPU Usage** | Average CPU utilization (mean +/- stddev) | Lower = more efficient |
| **Stddev** | Standard deviation across runs | Lower = more reproducible |

### Expected Patterns

**Throughput:**
- Valkey should be 20-40% higher than PostgreSQL
- Partitioned queue should be fastest PostgreSQL variant
- Under load, all implementations degrade somewhat — Valkey the least

**Latency:**
- Valkey should have 50-70% lower p95/p99 latency
- Cold start has higher latency than warm
- Under load, tail latency (p99) increases significantly for PostgreSQL

**CPU Usage:**
- Valkey typically uses 30-50% less CPU (with proper batching: 70-75% less)

### Red Flags

- **Throughput drops >50% under load**: Serious contention
- **p99 latency >10x p50**: Queue depth issues
- **CPU at 100%**: System is bottlenecked
- **Valkey p50 > 20ms**: Check for hot-shard antipattern
- **High stddev across runs**: Environmental instability, re-run with machine isolated

---

## Project Structure

```
queue-benchmark/
├── README.md                    # This file
├── run_vm1_pg.sh                # Self-contained PG VM setup + benchmark
├── run_vm2_valkey.sh            # Self-contained Valkey VM setup + benchmark
│
├── setup/                       # Installation scripts
│   ├── install_pg.sh            # PostgreSQL 16 setup
│   ├── install_valkey.sh        # Valkey 8 setup
│   └── requirements.txt         # Python dependencies
│
├── schema/                      # Database schemas
│   ├── pg_queue_basic.sql       # SKIP LOCKED (most common default)
│   ├── pg_queue_delete_returning.sql  # DELETE RETURNING (optimized)
│   ├── pg_queue_partitioned.sql # Partitioned queue (16 partitions)
│   └── init_valkey.sh           # Partitioned Valkey Streams setup (8 keys)
│
├── benchmark/                   # Core benchmark code
│   ├── config.py                # All configuration + environment detection
│   ├── producer.py              # Job producer (PG + partitioned Valkey)
│   ├── worker_pg.py             # PostgreSQL queue workers
│   ├── worker_valkey.py         # Valkey Streams workers (partitioned + batched)
│   ├── validate_valkey_latency.py  # Baseline latency validation
│   ├── test_durability.py       # Crash recovery testing
│   └── measure_app_queries.py   # App query impact measurement
│
├── orchestration/               # Test execution scripts
│   ├── run_pg_tests.sh          # All PG tests (multi-run, env logging)
│   └── run_valkey_tests.sh      # All Valkey tests (multi-run, env logging)
│
├── analysis/                    # Results analysis
│   ├── analyze.py               # Statistical analysis (mean +/- stddev, CI)
│   └── generate_graphs.py       # Graph generation (error bars, annotations)
│
└── results/                     # Test results (generated)
    ├── vm1_pg/                  # PostgreSQL results + environment.txt
    ├── vm2_valkey/              # Valkey results + environment.txt
    ├── analysis/                # summary.csv, summary.json, all_runs.csv
    └── graphs/                  # 7 PNG visualizations (300 DPI)
```

---

## License

MIT License - Feel free to use, modify, and distribute.
