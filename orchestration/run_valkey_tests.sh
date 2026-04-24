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
RESULTS_DIR="${RESULTS_DIR:-./results/vm2_valkey}"
BENCHMARK_DIR="./benchmark"
SCENARIOS=("cold" "warm" "load")
NUM_RUNS=${1:-5}  # Default 5 runs per scenario

# Durability mode: none | matched | strict.
#   none    -> appendonly no, save "" (legacy default, zero-durability)
#   matched -> appendfsync everysec (roughly matches PG synchronous_commit=off)
#   strict  -> appendfsync always (matches PG synchronous_commit=on)
# Reviewer concern #2: the headline comparison should include at least one
# matched-durability data point.
DURABILITY_MODE="${DURABILITY_MODE:-none}"
export DURABILITY_MODE

NUM_WORKERS="${NUM_WORKERS:-20}"
export JOB_PROCESSING_TIME_MS="${JOB_PROCESSING_TIME_MS:-5}"

# VM2 pgbench must be identical to VM1 so Valkey-under-load and
# PG-under-load are symmetric (reviewer smaller note).
PGBENCH_CLIENTS="${PGBENCH_CLIENTS:-10}"
PGBENCH_THREADS="${PGBENCH_THREADS:-4}"
PGBENCH_DURATION="${PGBENCH_DURATION:-320}"
PGBENCH_SCALE="${PGBENCH_SCALE:-10}"

PRODUCER_AUTO_CAP="${PRODUCER_AUTO_CAP:-0}"
PRODUCER_RATE="${PRODUCER_RATE:-1000}"
CAPACITY_FILE="${CAPACITY_FILE:-results/capacity.json}"

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
    echo "  Durability mode: $DURABILITY_MODE" >> "$env_file"
    echo "  appendonly: $(valkey-cli CONFIG GET appendonly | tail -1)" >> "$env_file"
    echo "  appendfsync: $(valkey-cli CONFIG GET appendfsync | tail -1)" >> "$env_file"
    echo "  save: $(valkey-cli CONFIG GET save | tail -1)" >> "$env_file"
    echo "  maxmemory: $(valkey-cli CONFIG GET maxmemory | tail -1)" >> "$env_file"
    echo "  maxmemory-policy: $(valkey-cli CONFIG GET maxmemory-policy | tail -1)" >> "$env_file"
    echo "" >> "$env_file"
    echo "Load Scenario Configuration (identical to VM1 for symmetry):" >> "$env_file"
    echo "  Tool: pgbench" >> "$env_file"
    echo "  Clients: $PGBENCH_CLIENTS" >> "$env_file"
    echo "  Threads: $PGBENCH_THREADS" >> "$env_file"
    echo "  Duration: ${PGBENCH_DURATION}s" >> "$env_file"
    echo "  Scale factor: $PGBENCH_SCALE" >> "$env_file"
    echo "" >> "$env_file"
    echo "Benchmark Parameters:" >> "$env_file"
    echo "  Production rate: ${PRODUCER_RATE} jobs/sec (pre-cap)" >> "$env_file"
    echo "  Auto-cap: $PRODUCER_AUTO_CAP (fraction=0.9, capacity=$CAPACITY_FILE)" >> "$env_file"
    echo "  Duration: 180 seconds" >> "$env_file"
    echo "  Workers: $NUM_WORKERS" >> "$env_file"
    echo "  Worker batch size: 50" >> "$env_file"
    echo "  Job size: 512 bytes" >> "$env_file"
    echo "  Processing time: ${JOB_PROCESSING_TIME_MS}ms (simulated)" >> "$env_file"
    echo "  Runs per scenario: $NUM_RUNS" >> "$env_file"
    echo "Saved environment info to $env_file"
}

# Apply Valkey durability mode at runtime via CONFIG SET so we don't need to
# restart the server between runs.
apply_durability_mode() {
    case "$DURABILITY_MODE" in
        none)
            echo "Durability mode = none (appendonly off, in-memory only)"
            valkey-cli CONFIG SET appendonly no > /dev/null
            valkey-cli CONFIG SET save "" > /dev/null
            ;;
        matched)
            echo "Durability mode = matched (appendonly on, appendfsync everysec)"
            valkey-cli CONFIG SET appendonly yes > /dev/null
            valkey-cli CONFIG SET appendfsync everysec > /dev/null
            ;;
        strict)
            echo "Durability mode = strict (appendonly on, appendfsync always)"
            valkey-cli CONFIG SET appendonly yes > /dev/null
            valkey-cli CONFIG SET appendfsync always > /dev/null
            ;;
        *)
            echo "ERROR: unknown DURABILITY_MODE=$DURABILITY_MODE"
            exit 1
            ;;
    esac
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

# Function to get total pending (unacknowledged) messages across all partitions
get_total_pending() {
    local total=0
    local NUM_PARTITIONS=8
    local STREAM_PREFIX="bench_queue"
    local GROUP="bench_workers"

    for i in $(seq 0 $((NUM_PARTITIONS - 1))); do
        local key="${STREAM_PREFIX}:${i}"
        # XPENDING returns: [num_pending, min_id, max_id, [[consumer, count], ...]]
        # First field is the pending count
        local n=$(valkey-cli XPENDING "$key" "$GROUP" 2>/dev/null | head -1)
        # If no group or error, fall back to XLEN
        if [ -z "$n" ] || ! [[ "$n" =~ ^[0-9]+$ ]]; then
            n=$(valkey-cli XLEN "$key" 2>/dev/null || echo 0)
        fi
        total=$((total + n))
    done
    echo $total
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

    local producer_extra=()
    if [ "$PRODUCER_AUTO_CAP" == "1" ]; then
        producer_extra=(--auto-cap --capacity-file "$CAPACITY_FILE")
    fi

    # Handle scenario-specific setup
    if [ "$scenario" == "warm" ]; then
        echo "Warming up system..."

        python3 "$BENCHMARK_DIR/worker_valkey.py" \
            --workers "$NUM_WORKERS" \
            --output "${result_prefix}_warmup_metrics.jsonl" &
        WORKER_PID=$!

        python3 "$BENCHMARK_DIR/producer.py" \
            --backend valkey \
            --rate "$PRODUCER_RATE" \
            --duration 60 \
            "${producer_extra[@]}"

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
        echo "Starting background load (identical to VM1 for symmetry)..."
        echo "  Tool: pgbench (TPC-B-like OLTP)"
        echo "  Clients: $PGBENCH_CLIENTS, Threads: $PGBENCH_THREADS, Duration: ${PGBENCH_DURATION}s, Scale: $PGBENCH_SCALE"
        pgbench -h localhost \
            -c "$PGBENCH_CLIENTS" -j "$PGBENCH_THREADS" -T "$PGBENCH_DURATION" \
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
    echo "Starting workers ($NUM_WORKERS)..."
    python3 "$BENCHMARK_DIR/worker_valkey.py" \
        --workers "$NUM_WORKERS" \
        --output "${result_prefix}_metrics.jsonl" &
    WORKER_PID=$!

    sleep 2

    # Start producer
    echo "Starting producer (${PRODUCER_RATE} jobs/sec for 180 seconds, auto-cap=$PRODUCER_AUTO_CAP)..."
    python3 "$BENCHMARK_DIR/producer.py" \
        --backend valkey \
        --rate "$PRODUCER_RATE" \
        --duration 180 \
        "${producer_extra[@]}" \
        > "${result_prefix}_producer.log" 2>&1

    echo "Producer finished. Waiting for workers to complete remaining jobs..."

    # Wait for all messages to be acknowledged (pending count reaches 0)
    MAX_WAIT=300
    ELAPSED=0
    INTERVAL=5

    while [ $ELAPSED -lt $MAX_WAIT ]; do
        PENDING=$(get_total_pending)
        TOTAL=$(get_total_stream_length)

        if [ "$PENDING" -eq 0 ] 2>/dev/null; then
            echo "All jobs acknowledged! (total stream length: $TOTAL)"
            break
        fi

        echo "Still processing... ($PENDING pending messages, $TOTAL total in streams)"
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

# Apply durability mode first so environment.txt captures the active values.
apply_durability_mode

# Save environment
save_environment

# Initialize pgbench
echo "Initializing pgbench (scale=$PGBENCH_SCALE)..."
pgbench -h localhost -i -s "$PGBENCH_SCALE" > /dev/null 2>&1 || true

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
