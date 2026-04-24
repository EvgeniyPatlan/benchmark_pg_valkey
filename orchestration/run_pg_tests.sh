#!/bin/bash
# Run PostgreSQL Queue Benchmarks
# Runs all three PG queue implementations through different scenarios
# Supports multiple runs for statistical rigor (mean +/- stddev)
#
# Load Scenario Details:
#   Tool: pgbench (PostgreSQL built-in benchmark)
#   Concurrency: 10 clients, 4 threads
#   Duration: 320 seconds (covers full test + drain)
#   Query type: Standard TPC-B-like OLTP mix (SELECT/UPDATE/INSERT)
#   Scale factor: 10 (~1.6M rows in pgbench_accounts)

set -e

# Configuration
RESULTS_DIR="${RESULTS_DIR:-./results/vm1_pg}"
BENCHMARK_DIR="./benchmark"
# Queue variants exercised. skip_locked_batch (reviewer concern #3) is the
# apples-to-apples comparison against Valkey's batched XREADGROUP.
QUEUE_TYPES=("skip_locked" "delete_returning" "partitioned" "skip_locked_batch")
SCENARIOS=("cold" "warm" "load")
NUM_RUNS=${1:-5}  # Default 5 runs per scenario, override via first argument

# Durability mode: none | matched | strict. Default 'none' preserves legacy
# behavior (synchronous_commit on, Valkey appendonly off). 'matched' sets
# synchronous_commit=off so PG and Valkey (appendfsync everysec) have
# comparable durability ceilings (reviewer concern #2).
DURABILITY_MODE="${DURABILITY_MODE:-none}"
export DURABILITY_MODE

# Worker count. Partitioned queue (reviewer concern #5) benefits from higher
# concurrency so there are >=2 workers per partition; override via env.
NUM_WORKERS="${NUM_WORKERS:-20}"
NUM_WORKERS_PARTITIONED="${NUM_WORKERS_PARTITIONED:-48}"

# Simulated processing time per job (reviewer concern #4). Default 5ms
# preserves legacy; set JOB_PROCESSING_TIME_MS=50 for a realistic workload
# that exercises row-lock contention.
export JOB_PROCESSING_TIME_MS="${JOB_PROCESSING_TIME_MS:-5}"

# Producer arrival rate. If PRODUCER_AUTO_CAP=1, the producer caps at 90%
# of measured service capacity from results/capacity.json so arrival rate
# never exceeds service rate (reviewer concern #1).
PRODUCER_AUTO_CAP="${PRODUCER_AUTO_CAP:-0}"
PRODUCER_RATE="${PRODUCER_RATE:-1000}"
CAPACITY_FILE="${CAPACITY_FILE:-results/capacity.json}"

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

# Save environment info
save_environment() {
    local env_file="$RESULTS_DIR/environment.txt"
    echo "Test Environment" > "$env_file"
    echo "================" >> "$env_file"
    echo "Scope: 1 PostgreSQL node (single-node deployment)" >> "$env_file"
    echo "Date: $(date -Iseconds)" >> "$env_file"
    echo "OS: $(lsb_release -ds 2>/dev/null || cat /etc/os-release | head -1)" >> "$env_file"
    echo "Kernel: $(uname -r)" >> "$env_file"
    echo "CPU: $(nproc) vCPU ($(lscpu | grep 'Model name' | sed 's/Model name: *//'))" >> "$env_file"
    echo "RAM: $(free -h | awk '/Mem:/{print $2}')" >> "$env_file"
    echo "Disk: $(df -h / | awk 'NR==2{print $2, $5}')" >> "$env_file"
    echo "PostgreSQL: $(psql --version)" >> "$env_file"
    echo "Python: $(python3 --version)" >> "$env_file"
    echo "psycopg2: $(python3 -c 'import psycopg2; print(psycopg2.__version__)')" >> "$env_file"
    echo "" >> "$env_file"
    echo "Load Scenario Configuration:" >> "$env_file"
    echo "  Tool: pgbench" >> "$env_file"
    echo "  Clients: 10" >> "$env_file"
    echo "  Threads: 4" >> "$env_file"
    echo "  Duration: 320s" >> "$env_file"
    echo "  Scale factor: 10" >> "$env_file"
    echo "  Query type: Standard TPC-B-like OLTP" >> "$env_file"
    echo "" >> "$env_file"
    echo "Benchmark Parameters:" >> "$env_file"
    echo "  Production rate: 1000 jobs/sec" >> "$env_file"
    echo "  Duration: 180 seconds" >> "$env_file"
    echo "  Workers: 20" >> "$env_file"
    echo "  Job size: 512 bytes" >> "$env_file"
    echo "  Processing time: 5ms (simulated)" >> "$env_file"
    echo "  Runs per scenario: $NUM_RUNS" >> "$env_file"
    echo "  Workers (default): $NUM_WORKERS" >> "$env_file"
    echo "  Workers (partitioned): $NUM_WORKERS_PARTITIONED" >> "$env_file"
    echo "  Job processing time: ${JOB_PROCESSING_TIME_MS}ms (simulated)" >> "$env_file"
    echo "  Durability mode: $DURABILITY_MODE" >> "$env_file"
    echo "  Producer auto-cap: $PRODUCER_AUTO_CAP (fraction=0.9, capacity=$CAPACITY_FILE)" >> "$env_file"
    echo "  synchronous_commit: $(psql -h localhost -t -A -c 'SHOW synchronous_commit;' 2>/dev/null || echo unknown)" >> "$env_file"
    echo "" >> "$env_file"
    echo "Queue variants tested: ${QUEUE_TYPES[*]}" >> "$env_file"
    echo "Saved environment info to $env_file"
}

# Apply the requested durability mode. ALTER SYSTEM + pg_reload_conf()
# require PG superuser privileges, so these commands run as the 'postgres'
# OS user via sudo rather than as bench_user. Only PostgreSQL is affected
# here; Valkey mode is handled in run_valkey_tests.sh.
apply_durability_mode() {
    local pg_admin
    if command -v sudo >/dev/null 2>&1 && sudo -n true 2>/dev/null; then
        pg_admin="sudo -u postgres psql"
    elif [ "$(id -un)" = "postgres" ]; then
        pg_admin="psql"
    else
        pg_admin="sudo -u postgres psql"
    fi

    case "$DURABILITY_MODE" in
        none|strict)
            echo "Durability mode = $DURABILITY_MODE (synchronous_commit=on)"
            $pg_admin -c "ALTER SYSTEM SET synchronous_commit = on;" > /dev/null
            ;;
        matched)
            echo "Durability mode = matched (synchronous_commit=off, equivalent to Valkey appendfsync everysec)"
            $pg_admin -c "ALTER SYSTEM SET synchronous_commit = off;" > /dev/null
            ;;
        *)
            echo "ERROR: unknown DURABILITY_MODE=$DURABILITY_MODE"
            exit 1
            ;;
    esac
    $pg_admin -c "SELECT pg_reload_conf();" > /dev/null
}

# Function to clean database
clean_database() {
    local queue_type=$1
    echo "Cleaning database for $queue_type..."

    case $queue_type in
        "skip_locked"|"skip_locked_batch")
            # skip_locked_batch shares the queue_jobs table with skip_locked.
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

# Choose worker count per queue variant. Partitioned gets more workers so
# concurrency per partition is >=2 (reviewer concern #5).
workers_for() {
    case "$1" in
        partitioned) echo "$NUM_WORKERS_PARTITIONED" ;;
        *) echo "$NUM_WORKERS" ;;
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
    local run_num=$3
    local result_prefix="$RESULTS_DIR/${queue_type}_${scenario}_run${run_num}"

    echo ""
    echo "=========================================="
    echo "Running: $queue_type - $scenario (run $run_num/$NUM_RUNS)"
    echo "=========================================="

    # Clean database
    clean_database "$queue_type"

    local worker_count
    worker_count=$(workers_for "$queue_type")

    # Build producer args once so warmup and the main run are identical
    # in rate-capping behavior.
    local producer_extra=()
    if [ "$PRODUCER_AUTO_CAP" == "1" ]; then
        producer_extra=(--auto-cap --capacity-file "$CAPACITY_FILE")
    fi

    # Handle scenario-specific setup
    if [ "$scenario" == "warm" ]; then
        echo "Warming up system..."

        python3 "$BENCHMARK_DIR/worker_pg.py" \
            --queue-type "$queue_type" \
            --workers "$worker_count" \
            --output "${result_prefix}_warmup_metrics.jsonl" &
        WORKER_PID=$!

        python3 "$BENCHMARK_DIR/producer.py" \
            --backend pg \
            --queue-type "$queue_type" \
            --rate "$PRODUCER_RATE" \
            --duration 60 \
            "${producer_extra[@]}"

        echo "Waiting for workers to process warmup jobs..."
        sleep 30

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
        echo "Starting background load..."
        echo "  Tool: pgbench (TPC-B-like OLTP)"
        echo "  Clients: 10, Threads: 4, Duration: 320s"
        pgbench -h localhost \
            -c 10 -j 4 -T 320 \
            > "${result_prefix}_pgbench.log" 2>&1 &
        PGBENCH_PID=$!

        # Also measure app query latency during the benchmark
        python3 "$BENCHMARK_DIR/measure_app_queries.py" \
            --mode pg-queue \
            --num-queries 500 \
            --output "${result_prefix}_app_queries.json" &
        APP_QUERY_PID=$!

        sleep 5
    fi

    # Start workers
    echo "Starting workers ($worker_count)..."
    python3 "$BENCHMARK_DIR/worker_pg.py" \
        --queue-type "$queue_type" \
        --workers "$worker_count" \
        --output "${result_prefix}_metrics.jsonl" &
    WORKER_PID=$!

    sleep 2

    # Start producer
    echo "Starting producer (${PRODUCER_RATE} jobs/sec for 180 seconds, auto-cap=$PRODUCER_AUTO_CAP)..."
    python3 "$BENCHMARK_DIR/producer.py" \
        --backend pg \
        --queue-type "$queue_type" \
        --rate "$PRODUCER_RATE" \
        --duration 180 \
        "${producer_extra[@]}" \
        > "${result_prefix}_producer.log" 2>&1

    echo "Producer finished. Waiting for workers to complete remaining jobs..."

    # Wait for queue to be empty
    MAX_WAIT=1800
    ELAPSED=0
    INTERVAL=10

    while [ $ELAPSED -lt $MAX_WAIT ]; do
        case $queue_type in
            "skip_locked"|"skip_locked_batch")
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
    kill $DB_STATS_PID 2>/dev/null || true
    wait $SYSTEM_PID 2>/dev/null || true
    wait $DB_STATS_PID 2>/dev/null || true

    echo "Benchmark complete: $queue_type - $scenario (run $run_num)"
    echo "Results saved to: $result_prefix"
}

# Main execution
echo "PostgreSQL Queue Benchmark Suite"
echo "Results directory: $RESULTS_DIR"
echo "Runs per scenario: $NUM_RUNS"
echo ""

# Apply durability mode before saving environment so SHOW synchronous_commit
# reflects the active setting in environment.txt.
apply_durability_mode

# Save environment
save_environment

# Initialize pgbench
echo "Initializing pgbench..."
pgbench -h localhost -i -s 10 > /dev/null 2>&1 || true

# Measure baseline app query latency (no queue activity)
echo "Measuring baseline app query latency..."
python3 "$BENCHMARK_DIR/measure_app_queries.py" \
    --mode baseline \
    --num-queries 500 \
    --output "$RESULTS_DIR/app_queries_baseline.json" 2>/dev/null || true

# Run all combinations with multiple runs
for queue_type in "${QUEUE_TYPES[@]}"; do
    for scenario in "${SCENARIOS[@]}"; do
        for run_num in $(seq 1 $NUM_RUNS); do
            run_benchmark "$queue_type" "$scenario" "$run_num"

            echo "Cooling down..."
            sleep 30
        done
    done
done

echo ""
echo "=========================================="
echo "All PostgreSQL benchmarks complete!"
echo "=========================================="
echo "Results location: $RESULTS_DIR"
echo "Total runs: $((${#QUEUE_TYPES[@]} * ${#SCENARIOS[@]} * NUM_RUNS))"
echo ""
echo "To analyze results:"
echo "  cd ../analysis"
echo "  python3 analyze.py --pg-results $RESULTS_DIR"
