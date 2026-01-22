#!/bin/bash
# Run Valkey Streams Benchmarks
# This script runs Valkey streams through different scenarios

set -e

# Configuration
RESULTS_DIR="../results/vm2_valkey"
BENCHMARK_DIR="./benchmark"
SCENARIOS=("cold" "warm" "load")

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

# Function to clean Valkey
clean_valkey() {
    echo "Cleaning Valkey..."
    valkey-cli FLUSHDB
    
    # Recreate stream and consumer group
    valkey-cli DEL bench_queue
}

# Function to get Valkey stats
get_valkey_stats() {
    local output_file=$1
    
    # Get INFO stats
    valkey-cli INFO stats | grep -E "^(total_connections_received|total_commands_processed|instantaneous_ops_per_sec|keyspace_hits|keyspace_misses)" | \
        awk -F: '{printf "%s,", $2}' | sed 's/,$/\n/' >> "$output_file"
    
    # Get memory info
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
            
            # CPU usage
            cpu_stats=$(mpstat 1 1 | awk '/Average:/ {print $3","$5","$12}')
            
            # Memory usage
            mem_used=$(free | awk '/Mem:/ {printf "%.2f", $3/$2 * 100}')
            
            # Disk I/O
            disk_io=$(iostat -x 1 1 | awk '/^[sv]d/ {print $6","$7}' | head -1)
            
            echo "$timestamp,$cpu_stats,$mem_used,$disk_io"
        done
    } > "$output_file"
}

# Function to run a single benchmark
run_benchmark() {
    local scenario=$1
    local result_prefix="$RESULTS_DIR/valkey_${scenario}"
    
    echo ""
    echo "=========================================="
    echo "Running: Valkey Streams - $scenario"
    echo "=========================================="
    
    # Clean Valkey
    clean_valkey
    
    # Handle scenario-specific setup
    if [ "$scenario" == "warm" ]; then
        echo "Warming up system..."
        
        # Start workers
        python3 "$BENCHMARK_DIR/worker_valkey.py" \
            --workers 20 \
            --output "${result_prefix}_warmup_metrics.jsonl" &
        WORKER_PID=$!
        
        # Run warmup producer (1 minute at 1000 jobs/sec)
        python3 "$BENCHMARK_DIR/producer.py" \
            --backend valkey \
            --rate 1000 \
            --duration 60
        
        # Wait a bit for workers to catch up
        echo "Waiting for workers to process warmup jobs..."
        sleep 30
        
        # Stop workers
        kill $WORKER_PID 2>/dev/null || true
        wait $WORKER_PID 2>/dev/null || true
        
        # Clean again
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
        echo "Starting background load (pgbench on PostgreSQL)..."
        pgbench -h localhost \
            -c 10 -j 4 -T 320 \
            > "${result_prefix}_pgbench.log" 2>&1 &
        PGBENCH_PID=$!
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
    echo "Starting producer (5000 jobs/sec for 300 seconds)..."
    python3 "$BENCHMARK_DIR/producer.py" \
        --backend valkey \
        --rate 5000 \
        --duration 300 \
        > "${result_prefix}_producer.log" 2>&1
    
    echo "Producer finished. Waiting for workers to complete remaining jobs..."
    sleep 10
    
    # Stop workers
    echo "Stopping workers..."
    kill $WORKER_PID 2>/dev/null || true
    wait $WORKER_PID 2>/dev/null || true
    
    # Stop background load
    if [ "$scenario" == "load" ]; then
        echo "Stopping background load..."
        kill $PGBENCH_PID 2>/dev/null || true
        wait $PGBENCH_PID 2>/dev/null || true
    fi
    
    # Stop metrics collection
    kill $SYSTEM_PID 2>/dev/null || true
    kill $VALKEY_STATS_PID 2>/dev/null || true
    wait $SYSTEM_PID 2>/dev/null || true
    wait $VALKEY_STATS_PID 2>/dev/null || true
    
    # Get final stream stats
    echo "Stream length: $(valkey-cli XLEN bench_queue)" > "${result_prefix}_final_stats.txt"
    valkey-cli XINFO STREAM bench_queue >> "${result_prefix}_final_stats.txt" 2>/dev/null || true
    valkey-cli XINFO GROUPS bench_queue >> "${result_prefix}_final_stats.txt" 2>/dev/null || true
    
    echo "Benchmark complete: Valkey Streams - $scenario"
    echo "Results saved to: $result_prefix"
}

# Main execution
echo "Valkey Streams Benchmark Suite"
echo "Results directory: $RESULTS_DIR"
echo ""

# Check Valkey is running
valkey-cli ping > /dev/null 2>&1 || {
    echo "Error: Valkey is not running!"
    echo "Start Valkey with: sudo systemctl start valkey"
    exit 1
}

# Initialize pgbench (for load testing if needed)
echo "Initializing pgbench..."
pgbench -h localhost -i -s 10 > /dev/null 2>&1 || true

# Run all scenarios
for scenario in "${SCENARIOS[@]}"; do
    run_benchmark "$scenario"
    
    # Wait between tests
    echo "Cooling down..."
    sleep 30
done

echo ""
echo "=========================================="
echo "All Valkey benchmarks complete!"
echo "=========================================="
echo "Results location: $RESULTS_DIR"
echo ""
echo "To analyze results:"
echo "  cd ../analysis"
echo "  python3 analyze.py --valkey-results $RESULTS_DIR"
