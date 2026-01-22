#!/bin/bash
# Run PostgreSQL Queue Benchmarks
# This script runs all three PG queue implementations through different scenarios

set -e

# Configuration
RESULTS_DIR="./results/vm1_pg"
BENCHMARK_DIR="./benchmark"
QUEUE_TYPES=("skip_locked" "delete_returning" "partitioned")
SCENARIOS=("cold" "warm" "load")

# Check PGPASSWORD is set
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

# Function to clean database
clean_database() {
    local queue_type=$1
    echo "Cleaning database for $queue_type..."
    
    case $queue_type in
        "skip_locked")
            psql -h localhost -c "TRUNCATE queue_jobs;"
            ;;
        "delete_returning")
            psql -h localhost -c "TRUNCATE queue_jobs_dr, queue_completed_dr;"
            ;;
        "partitioned")
            psql -h localhost -c "TRUNCATE queue_jobs_part;"
            ;;
    esac
}

# Function to get database stats
get_db_stats() {
    local output_file=$1
    psql -h localhost -t -A -F',' << EOF > "$output_file"
SELECT 
    NOW() as timestamp,
    numbackends as connections,
    xact_commit as commits,
    xact_rollback as rollbacks,
    blks_read as blocks_read,
    blks_hit as blocks_hit,
    tup_returned as tuples_returned,
    tup_fetched as tuples_fetched,
    tup_inserted as tuples_inserted,
    tup_updated as tuples_updated,
    tup_deleted as tuples_deleted
FROM pg_stat_database 
WHERE datname = 'bench_db';
EOF
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
    local queue_type=$1
    local scenario=$2
    local result_prefix="$RESULTS_DIR/${queue_type}_${scenario}"
    
    echo ""
    echo "=========================================="
    echo "Running: $queue_type - $scenario"
    echo "=========================================="
    
    # Clean database
    clean_database "$queue_type"
    
    # Handle scenario-specific setup
    if [ "$scenario" == "warm" ]; then
        echo "Warming up system..."
        
        # Start workers
        python3 "$BENCHMARK_DIR/worker_pg.py" \
            --queue-type "$queue_type" \
            --workers 20 \
            --output "${result_prefix}_warmup_metrics.jsonl" &
        WORKER_PID=$!
        
        # Run warmup producer (1 minute at 1000 jobs/sec)
        python3 "$BENCHMARK_DIR/producer.py" \
            --backend pg \
            --queue-type "$queue_type" \
            --rate 1000 \
            --duration 60
        
        # Wait a bit for workers to catch up
        echo "Waiting for workers to process warmup jobs..."
        sleep 30
        
        # Stop workers
        kill $WORKER_PID 2>/dev/null || true
        wait $WORKER_PID 2>/dev/null || true
        
        echo "Warmup complete"
        sleep 5
    fi
    
    # Start system metrics collection
    collect_system_metrics "${result_prefix}_system.csv" 320 &
    SYSTEM_PID=$!
    
    # Start database stats collection
    {
        echo "timestamp,connections,commits,rollbacks,blocks_read,blocks_hit,tuples_returned,tuples_fetched,tuples_inserted,tuples_updated,tuples_deleted" > "${result_prefix}_db_stats.csv"
        for i in $(seq 1 320); do
            get_db_stats "${result_prefix}_db_stats_tmp.csv"
            cat "${result_prefix}_db_stats_tmp.csv" >> "${result_prefix}_db_stats.csv"
            sleep 1
        done
        rm -f "${result_prefix}_db_stats_tmp.csv"
    } &
    DB_STATS_PID=$!
    
    # Start background load if needed
    if [ "$scenario" == "load" ]; then
        echo "Starting background load (pgbench)..."
        pgbench -h localhost \
            -c 10 -j 4 -T 320 \
            > "${result_prefix}_pgbench.log" 2>&1 &
        PGBENCH_PID=$!
        sleep 5
    fi
    
    # Start workers
    echo "Starting workers..."
    python3 "$BENCHMARK_DIR/worker_pg.py" \
        --queue-type "$queue_type" \
        --workers 20 \
        --output "${result_prefix}_metrics.jsonl" &
    WORKER_PID=$!
    
    sleep 2
    
    # Start producer
    echo "Starting producer (1000 jobs/sec for 180 seconds)..."
    python3 "$BENCHMARK_DIR/producer.py" \
        --backend pg \
        --queue-type "$queue_type" \
        --rate 1000 \
        --duration 180 \
        > "${result_prefix}_producer.log" 2>&1
    
    echo "Producer finished. Waiting for workers to complete remaining jobs..."
    echo "Producer finished. Waiting for workers to complete remaining jobs..."

# Wait for queue to be empty (check every 10 seconds, max 30 minutes)
MAX_WAIT=1800  # 30 minutes max
ELAPSED=0
INTERVAL=10

while [ $ELAPSED -lt $MAX_WAIT ]; do
    # Check pending jobs based on queue type
    case $queue_type in
        "skip_locked")
            PENDING=$(psql -h localhost -t -c "SELECT count(*) FROM queue_jobs WHERE status='pending';")
            ;;
        "delete_returning")
            PENDING=$(psql -h localhost -t -c "SELECT count(*) FROM queue_jobs_dr;")
            ;;
        "partitioned")
            PENDING=$(psql -h localhost -t -c "SELECT count(*) FROM queue_jobs_part WHERE status='pending';")
            ;;
    esac
    
    PENDING=$(echo $PENDING | tr -d ' ')
    
    if [ "$PENDING" -eq 0 ] 2>/dev/null; then
        echo "All jobs completed!"
        break
    fi
    
    echo "Still processing... ($PENDING pending jobs)"
    sleep $INTERVAL
    ELAPSED=$((ELAPSED + INTERVAL))
done

if [ $ELAPSED -ge $MAX_WAIT ]; then
    echo "WARNING: Timeout reached, some jobs may be unprocessed"
fi

# Give workers a moment to write final metrics
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
    fi
    
    # Stop metrics collection
    kill $SYSTEM_PID 2>/dev/null || true
    kill $DB_STATS_PID 2>/dev/null || true
    wait $SYSTEM_PID 2>/dev/null || true
    wait $DB_STATS_PID 2>/dev/null || true
    
    echo "Benchmark complete: $queue_type - $scenario"
    echo "Results saved to: $result_prefix"
}

# Main execution
echo "PostgreSQL Queue Benchmark Suite"
echo "Results directory: $RESULTS_DIR"
echo ""

# Initialize pgbench (for load testing)
echo "Initializing pgbench..."
pgbench -h localhost -i -s 10 > /dev/null 2>&1 || true

# Run all combinations
for queue_type in "${QUEUE_TYPES[@]}"; do
    for scenario in "${SCENARIOS[@]}"; do
        run_benchmark "$queue_type" "$scenario"
        
        # Wait between tests
        echo "Cooling down..."
        sleep 30
    done
done

echo ""
echo "=========================================="
echo "All PostgreSQL benchmarks complete!"
echo "=========================================="
echo "Results location: $RESULTS_DIR"
echo ""
echo "To analyze results:"
echo "  cd ../analysis"
echo "  python3 analyze.py --pg-results $RESULTS_DIR"
