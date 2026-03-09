#!/bin/bash
# Run Valkey Streams Benchmarks
# Uses partitioned stream keys to avoid hot-shard antipattern
# Supports multiple runs for statistical rigor (mean +/- stddev)
#
# Load Scenario Details:
#   Tool: pgbench (PostgreSQL built-in benchmark) running on same machine
#   Concurrency: 10 clients, 4 threads
#   Duration: 320 seconds
#   Query type: Standard TPC-B-like OLTP mix
#   Scale factor: 10
#   Purpose: Simulate co-located database load to test Valkey isolation

set -e

# Configuration
RESULTS_DIR="./results/vm2_valkey"
BENCHMARK_DIR="./benchmark"
SCENARIOS=("cold" "warm" "load")
NUM_RUNS=${1:-5}  # Default 5 runs per scenario

# Check PGPASSWORD is set (needed for pgbench in load test)
if [ -z "$PGPASSWORD" ]; then
    echo "ERROR: PGPASSWORD environment variable not set!"
    echo "Run: export PGPASSWORD=bench_pass"
    exit 1
fi

# PostgreSQL connection parameters
PGHOST=localhost
PGPORT=5432
PGDATABASE=bench_db
PGUSER=bench_user

export PGHOST PGPORT PGDATABASE PGUSER

# Create results directory
mkdir -p "$RESULTS_DIR"

# Save environment info
save_environment() {
    local env_file="$RESULTS_DIR/environment.txt"
    echo "Test Environment" > "$env_file"
    echo "================" >> "$env_file"
    echo "Scope: 1 Valkey standalone node (single-node deployment)" >> "$env_file"
    echo "Date: $(date -Iseconds)" >> "$env_file"
    echo "OS: $(lsb_release -ds 2>/dev/null || cat /etc/os-release | head -1)" >> "$env_file"
    echo "Kernel: $(uname -r)" >> "$env_file"
    echo "CPU: $(nproc) vCPU ($(lscpu | grep 'Model name' | sed 's/Model name: *//'))" >> "$env_file"
    echo "RAM: $(free -h | awk '/Mem:/{print $2}')" >> "$env_file"
    echo "Disk: $(df -h / | awk 'NR==2{print $2, $5}')" >> "$env_file"
    echo "Valkey: $(valkey-server --version 2>/dev/null || echo 'unknown')" >> "$env_file"
    echo "PostgreSQL: $(psql --version)" >> "$env_file"
    echo "Python: $(python3 --version)" >> "$env_file"
    echo "" >> "$env_file"
    echo "Valkey Configuration:" >> "$env_file"
    echo "  Stream partitions: 8 (bench_queue:0 .. bench_queue:7)" >> "$env_file"
    echo "  Consumer group: bench_workers" >> "$env_file"
    echo "  Persistence: disabled (save '', appendonly no)" >> "$env_file"
    echo "  maxmemory: 4gb" >> "$env_file"
    echo "  maxmemory-policy: noeviction" >> "$env_file"
    echo "" >> "$env_file"
    echo "Load Scenario Configuration:" >> "$env_file"
    echo "  Tool: pgbench" >> "$env_file"
    echo "  Clients: 10" >> "$env_file"
    echo "  Threads: 4" >> "$env_file"
    echo "  Duration: 320s" >> "$env_file"
    echo "  Scale factor: 10" >> "$env_file"
    echo "" >> "$env_file"
    echo "Benchmark Parameters:" >> "$env_file"
    echo "  Production rate: 1000 jobs/sec" >> "$env_file"
    echo "  Duration: 180 seconds" >> "$env_file"
    echo "  Workers: 20" >> "$env_file"
    echo "  Worker batch size: 50" >> "$env_file"
    echo "  Job size: 512 bytes" >> "$env_file"
    echo "  Processing time: 5ms (simulated)" >> "$env_file"
    echo "  Runs per scenario: $NUM_RUNS" >> "$env_file"
    echo "Saved environment info to $env_file"
}

# Function to clean Valkey (all partitions)
clean_valkey() {
    echo "Cleaning Valkey (all partitions)..."
    local NUM_PARTITIONS=8
    local STREAM_PREFIX="bench_queue"

    for i in $(seq 0 $((NUM_PARTITIONS - 1))); do
        valkey-cli DEL "${STREAM_PREFIX}:${i}" > /dev/null 2>&1 || true
    done
    # Also clean legacy single-key stream
    valkey-cli DEL bench_queue > /dev/null 2>&1 || true
}

# Function to get Valkey stats
get_valkey_stats() {
    local output_file=$1

    valkey-cli INFO stats | grep -E "^(total_connections_received|total_commands_processed|instantaneous_ops_per_sec|keyspace_hits|keyspace_misses)" | \
        awk -F: '{printf "%s,", $2}' | sed 's/,$/\n/' >> "$output_file"

    valkey-cli INFO memory | grep "^used_memory:" | awk -F: '{printf "%s\n", $2}' >> "$output_file"
}

# Function to collect system metrics
collect_system_metrics() {
    local output_file=$1
    local duration=$2

    echo "Collecting system metrics to $output_file for ${duration}s..."

    {
        echo "timestamp,cpu_user,cpu_system,cpu_idle,mem_used_pct,disk_read_kb,disk_write_kb"

        for i in $(seq 1 $duration); do
            timestamp=$(date -Iseconds)
            cpu_stats=$(mpstat 1 1 | awk '/Average:/ {print $3","$5","$12}')
            mem_used=$(free | awk '/Mem:/ {printf "%.2f", $3/$2 * 100}')
            disk_io=$(iostat -x 1 1 | awk '/^[sv]d/ {print $6","$7}' | head -1)
            echo "$timestamp,$cpu_stats,$mem_used,$disk_io"
        done
    } > "$output_file"
}

# Function to get total stream length across all partitions
get_total_stream_length() {
    local total=0
    local NUM_PARTITIONS=8
    local STREAM_PREFIX="bench_queue"

    for i in $(seq 0 $((NUM_PARTITIONS - 1))); do
        local n=$(valkey-cli XLEN "${STREAM_PREFIX}:${i}" 2>/dev/null || echo 0)
        total=$((total + n))
    done
    echo $total
}

# Function to run a single benchmark
run_benchmark() {
    local scenario=$1
    local run_num=$2
    local result_prefix="$RESULTS_DIR/valkey_${scenario}_run${run_num}"

    echo ""
    echo "=========================================="
    echo "Running: Valkey Streams - $scenario (run $run_num/$NUM_RUNS)"
    echo "=========================================="

    # Clean Valkey
    clean_valkey

    # Handle scenario-specific setup
    if [ "$scenario" == "warm" ]; then
        echo "Warming up system..."

        python3 "$BENCHMARK_DIR/worker_valkey.py" \
            --workers 20 \
            --output "${result_prefix}_warmup_metrics.jsonl" &
        WORKER_PID=$!

        python3 "$BENCHMARK_DIR/producer.py" \
            --backend valkey \
            --rate 1000 \
            --duration 60

        echo "Waiting for workers to process warmup jobs..."
        sleep 30

        kill $WORKER_PID 2>/dev/null || true
        wait $WORKER_PID 2>/dev/null || true

        clean_valkey

        echo "Warmup complete"
        sleep 5
    fi

    # Start system metrics collection
    collect_system_metrics "${result_prefix}_system.csv" 320 &
    SYSTEM_PID=$!

    # Start Valkey stats collection
    {
        echo "timestamp,connections,commands,ops_per_sec,hits,misses,memory_used" > "${result_prefix}_valkey_stats.csv"
        for i in $(seq 1 320); do
            timestamp=$(date -Iseconds)
            echo -n "$timestamp," >> "${result_prefix}_valkey_stats.csv"
            get_valkey_stats "${result_prefix}_valkey_stats.csv"
            sleep 1
        done
    } &
    VALKEY_STATS_PID=$!

    # Start background load if needed
    if [ "$scenario" == "load" ]; then
        echo "Starting background load..."
        echo "  Tool: pgbench (TPC-B-like OLTP)"
        echo "  Clients: 10, Threads: 4, Duration: 320s"
        pgbench -h localhost \
            -c 10 -j 4 -T 320 \
            > "${result_prefix}_pgbench.log" 2>&1 &
        PGBENCH_PID=$!

        # Measure app query latency during Valkey queue operation
        python3 "$BENCHMARK_DIR/measure_app_queries.py" \
            --mode valkey-queue \
            --num-queries 500 \
            --output "${result_prefix}_app_queries.json" &
        APP_QUERY_PID=$!

        sleep 5
    fi

    # Start workers
    echo "Starting workers..."
    python3 "$BENCHMARK_DIR/worker_valkey.py" \
        --workers 20 \
        --output "${result_prefix}_metrics.jsonl" &
    WORKER_PID=$!

    sleep 2

    # Start producer
    echo "Starting producer (1000 jobs/sec for 180 seconds)..."
    python3 "$BENCHMARK_DIR/producer.py" \
        --backend valkey \
        --rate 1000 \
        --duration 180 \
        > "${result_prefix}_producer.log" 2>&1

    echo "Producer finished. Waiting for workers to complete remaining jobs..."

    # Wait for streams to be drained
    MAX_WAIT=300
    ELAPSED=0
    INTERVAL=5

    while [ $ELAPSED -lt $MAX_WAIT ]; do
        REMAINING=$(get_total_stream_length)

        if [ "$REMAINING" -eq 0 ] 2>/dev/null; then
            echo "All jobs completed!"
            break
        fi

        echo "Still processing... ($REMAINING messages remaining across partitions)"
        sleep $INTERVAL
        ELAPSED=$((ELAPSED + INTERVAL))
    done

    if [ $ELAPSED -ge $MAX_WAIT ]; then
        echo "WARNING: Timeout reached, some messages may be unprocessed"
    fi

    sleep 5

    # Stop workers
    echo "Stopping workers..."
    kill $WORKER_PID 2>/dev/null || true
    wait $WORKER_PID 2>/dev/null || true

    # Stop background load
    if [ "$scenario" == "load" ]; then
        echo "Stopping background load..."
        kill $PGBENCH_PID 2>/dev/null || true
        wait $PGBENCH_PID 2>/dev/null || true
        kill $APP_QUERY_PID 2>/dev/null || true
        wait $APP_QUERY_PID 2>/dev/null || true
    fi

    # Stop metrics collection
    kill $SYSTEM_PID 2>/dev/null || true
    kill $VALKEY_STATS_PID 2>/dev/null || true
    wait $SYSTEM_PID 2>/dev/null || true
    wait $VALKEY_STATS_PID 2>/dev/null || true

    # Get final stream stats
    {
        echo "Total stream length: $(get_total_stream_length)"
        for i in $(seq 0 7); do
            echo "bench_queue:$i length: $(valkey-cli XLEN bench_queue:$i 2>/dev/null || echo 0)"
        done
        valkey-cli XINFO GROUPS bench_queue:0 2>/dev/null || true
    } > "${result_prefix}_final_stats.txt"

    echo "Benchmark complete: Valkey Streams - $scenario (run $run_num)"
    echo "Results saved to: $result_prefix"
}

# Main execution
echo "Valkey Streams Benchmark Suite (Partitioned)"
echo "Results directory: $RESULTS_DIR"
echo "Runs per scenario: $NUM_RUNS"
echo ""

# Check Valkey is running
valkey-cli ping > /dev/null 2>&1 || {
    echo "Error: Valkey is not running!"
    echo "Start Valkey with: sudo systemctl start valkey"
    exit 1
}

# Save environment
save_environment

# Initialize pgbench
echo "Initializing pgbench..."
pgbench -h localhost -i -s 10 > /dev/null 2>&1 || true

# Initialize partitioned streams
echo "Initializing partitioned Valkey streams..."
bash ./schema/init_valkey.sh 8

# Validate baseline Valkey latency
echo ""
echo "Validating baseline Valkey latency..."
python3 "$BENCHMARK_DIR/validate_valkey_latency.py" 2>/dev/null || echo "  (validation script not available, skipping)"
echo ""

# Measure baseline app query latency
echo "Measuring baseline app query latency..."
python3 "$BENCHMARK_DIR/measure_app_queries.py" \
    --mode baseline \
    --num-queries 500 \
    --output "$RESULTS_DIR/app_queries_baseline.json" 2>/dev/null || true

# Run all scenarios with multiple runs
for scenario in "${SCENARIOS[@]}"; do
    for run_num in $(seq 1 $NUM_RUNS); do
        run_benchmark "$scenario" "$run_num"

        echo "Cooling down..."
        sleep 30
    done
done

echo ""
echo "=========================================="
echo "All Valkey benchmarks complete!"
echo "=========================================="
echo "Results location: $RESULTS_DIR"
echo "Total runs: $((${#SCENARIOS[@]} * NUM_RUNS))"
echo ""
echo "To analyze results:"
echo "  cd analysis"
echo "  python3 analyze.py --valkey-results $RESULTS_DIR"
