#!/bin/bash
# Run RabbitMQ benchmarks for classic + quorum queue variants.

set -e

RESULTS_DIR="${RESULTS_DIR:-./results/vm4_rabbitmq}"
BENCHMARK_DIR="./benchmark"
QUEUE_TYPES=("classic" "quorum")
SCENARIOS=("cold" "warm" "load")
NUM_RUNS=${1:-5}

DURABILITY_MODE="${DURABILITY_MODE:-none}"
export DURABILITY_MODE

NUM_WORKERS="${NUM_WORKERS:-20}"
export JOB_PROCESSING_TIME_MS="${JOB_PROCESSING_TIME_MS:-5}"

PGBENCH_CLIENTS="${PGBENCH_CLIENTS:-10}"
PGBENCH_THREADS="${PGBENCH_THREADS:-4}"
PGBENCH_DURATION="${PGBENCH_DURATION:-320}"
PGBENCH_SCALE="${PGBENCH_SCALE:-10}"

PRODUCER_AUTO_CAP="${PRODUCER_AUTO_CAP:-0}"
PRODUCER_RATE="${PRODUCER_RATE:-1000}"
CAPACITY_FILE="${CAPACITY_FILE:-results/capacity.json}"

RABBITMQ_USER="${RABBITMQ_USER:-bench_user}"
RABBITMQ_PASS="${RABBITMQ_PASS:-bench_pass}"
RABBITMQ_HOST="${RABBITMQ_HOST:-localhost}"
ADMIN=(rabbitmqadmin -H "$RABBITMQ_HOST" -u "$RABBITMQ_USER" -p "$RABBITMQ_PASS")

if [ -z "$PGPASSWORD" ]; then
    echo "ERROR: PGPASSWORD environment variable not set (needed for pgbench load)"
    exit 1
fi
export PGHOST=localhost PGPORT=5432 PGDATABASE=bench_db PGUSER=bench_user

mkdir -p "$RESULTS_DIR"

save_environment() {
    local env_file="$RESULTS_DIR/environment.txt"
    {
        echo "Test Environment"
        echo "================"
        echo "Scope: 1 RabbitMQ node (single-node deployment)"
        echo "Date: $(date -Iseconds)"
        echo "OS: $(lsb_release -ds 2>/dev/null || cat /etc/os-release | head -1)"
        echo "Kernel: $(uname -r)"
        echo "CPU: $(nproc) vCPU ($(lscpu | grep 'Model name' | sed 's/Model name: *//'))"
        echo "RAM: $(free -h | awk '/Mem:/{print $2}')"
        echo "Disk: $(df -h / | awk 'NR==2{print $2, $5}')"
        echo "RabbitMQ: $(sudo rabbitmqctl status 2>/dev/null | grep -i 'RabbitMQ version' || echo unknown)"
        echo "Erlang: $(sudo rabbitmqctl status 2>/dev/null | grep -i 'Erlang' | head -1 || echo unknown)"
        echo "PostgreSQL: $(psql --version)"
        echo "Python: $(python3 --version)"
        echo ""
        echo "RabbitMQ Configuration:"
        echo "  Queues: bench_queue_classic (classic, durable), bench_queue_quorum (quorum, durable)"
        echo "  Durability mode: $DURABILITY_MODE"
        echo "  Publisher confirms: $([ "$DURABILITY_MODE" = "none" ] && echo disabled || echo enabled)"
        echo "  Consumer prefetch: 50"
        echo "  Memory high-watermark: 0.6 (raised from default 0.4)"
        echo ""
        echo "Load Scenario Configuration (identical to VM1/VM2/VM3 for symmetry):"
        echo "  Tool: pgbench"
        echo "  Clients: $PGBENCH_CLIENTS"
        echo "  Threads: $PGBENCH_THREADS"
        echo "  Duration: ${PGBENCH_DURATION}s"
        echo "  Scale factor: $PGBENCH_SCALE"
        echo ""
        echo "Benchmark Parameters:"
        echo "  Production rate: ${PRODUCER_RATE} jobs/sec (pre-cap)"
        echo "  Auto-cap: $PRODUCER_AUTO_CAP"
        echo "  Duration: 180 seconds"
        echo "  Workers: $NUM_WORKERS"
        echo "  Job size: 512 bytes"
        echo "  Processing time: ${JOB_PROCESSING_TIME_MS}ms (simulated)"
        echo "  Runs per scenario: $NUM_RUNS"
        echo ""
        echo "Fairness caveats:"
        echo "  Quorum queue on single node: the Raft durability guarantee is"
        echo "  degenerate (no replication). Throughput/latency are still"
        echo "  informative but the durability story requires ≥3 nodes."
    } > "$env_file"
    echo "Saved environment info to $env_file"
}

clean_rabbitmq() {
    echo "Purging RabbitMQ benchmark queues..."
    "${ADMIN[@]}" purge queue name=bench_queue_classic 2>/dev/null || true
    "${ADMIN[@]}" purge queue name=bench_queue_quorum 2>/dev/null || true
}

get_queue_depth() {
    local queue=$1
    "${ADMIN[@]}" list queues name messages --format raw_json 2>/dev/null \
        | python3 -c "
import sys, json
try:
    data = json.load(sys.stdin)
    for q in data:
        if q.get('name') == '$queue':
            print(q.get('messages', 0)); exit()
    print(0)
except Exception:
    print(0)
"
}

collect_system_metrics() {
    local output_file=$1
    local duration=$2
    {
        echo "timestamp,cpu_user,cpu_system,cpu_idle,mem_used_pct,disk_read_kb,disk_write_kb"
        for i in $(seq 1 "$duration"); do
            timestamp=$(date -Iseconds)
            cpu_stats=$(mpstat 1 1 | awk '/Average:/ {print $3","$5","$12}')
            mem_used=$(free | awk '/Mem:/ {printf "%.2f", $3/$2 * 100}')
            disk_io=$(iostat -x 1 1 | awk '/^[sv]d/ {print $6","$7}' | head -1)
            echo "$timestamp,$cpu_stats,$mem_used,$disk_io"
        done
    } > "$output_file"
}

run_benchmark() {
    local queue_type=$1
    local scenario=$2
    local run_num=$3
    local result_prefix="$RESULTS_DIR/${queue_type}_${scenario}_run${run_num}"
    local queue_name="bench_queue_${queue_type}"

    echo ""
    echo "=========================================="
    echo "Running: RabbitMQ $queue_type - $scenario (run $run_num/$NUM_RUNS)"
    echo "=========================================="

    clean_rabbitmq

    local producer_extra=()
    if [ "$PRODUCER_AUTO_CAP" == "1" ]; then
        producer_extra=(--auto-cap --capacity-file "$CAPACITY_FILE")
    fi

    if [ "$scenario" == "warm" ]; then
        echo "Warming up..."
        python3 "$BENCHMARK_DIR/worker_rabbitmq.py" \
            --queue-type "$queue_type" --workers "$NUM_WORKERS" \
            --output "${result_prefix}_warmup_metrics.jsonl" &
        WORKER_PID=$!
        python3 "$BENCHMARK_DIR/producer.py" \
            --backend rabbitmq --queue-type "$queue_type" \
            --rate "$PRODUCER_RATE" --duration 60 "${producer_extra[@]}"
        sleep 30
        kill "$WORKER_PID" 2>/dev/null || true
        wait "$WORKER_PID" 2>/dev/null || true
        clean_rabbitmq
        sleep 5
    fi

    collect_system_metrics "${result_prefix}_system.csv" 320 &
    SYSTEM_PID=$!

    if [ "$scenario" == "load" ]; then
        echo "Starting background pgbench load..."
        pgbench -h localhost -c "$PGBENCH_CLIENTS" -j "$PGBENCH_THREADS" -T "$PGBENCH_DURATION" \
            > "${result_prefix}_pgbench.log" 2>&1 &
        PGBENCH_PID=$!
        sleep 5
    fi

    echo "Starting workers ($NUM_WORKERS)..."
    python3 "$BENCHMARK_DIR/worker_rabbitmq.py" \
        --queue-type "$queue_type" --workers "$NUM_WORKERS" \
        --output "${result_prefix}_metrics.jsonl" &
    WORKER_PID=$!
    sleep 2

    echo "Starting producer..."
    python3 "$BENCHMARK_DIR/producer.py" \
        --backend rabbitmq --queue-type "$queue_type" \
        --rate "$PRODUCER_RATE" --duration 180 "${producer_extra[@]}" \
        > "${result_prefix}_producer.log" 2>&1

    echo "Producer finished. Draining queue..."
    MAX_WAIT=600
    ELAPSED=0
    INTERVAL=5
    while [ $ELAPSED -lt $MAX_WAIT ]; do
        DEPTH=$(get_queue_depth "$queue_name")
        if [ "$DEPTH" -eq 0 ] 2>/dev/null; then
            echo "Queue drained."
            break
        fi
        echo "Still processing... ($DEPTH messages remaining)"
        sleep $INTERVAL
        ELAPSED=$((ELAPSED + INTERVAL))
    done

    sleep 5
    kill "$WORKER_PID" 2>/dev/null || true
    wait "$WORKER_PID" 2>/dev/null || true

    if [ "$scenario" == "load" ]; then
        kill "$PGBENCH_PID" 2>/dev/null || true
        wait "$PGBENCH_PID" 2>/dev/null || true
    fi

    kill "$SYSTEM_PID" 2>/dev/null || true
    wait "$SYSTEM_PID" 2>/dev/null || true

    echo "Benchmark complete: RabbitMQ $queue_type - $scenario (run $run_num)"
}

echo "RabbitMQ Benchmark Suite"
echo "Results: $RESULTS_DIR"
echo "Runs per scenario: $NUM_RUNS"

sudo rabbitmqctl status > /dev/null 2>&1 || {
    echo "ERROR: RabbitMQ not running. sudo systemctl start rabbitmq-server"
    exit 1
}

save_environment

echo "Initializing pgbench..."
pgbench -h localhost -i -s "$PGBENCH_SCALE" > /dev/null 2>&1 || true

echo "Declaring RabbitMQ queues..."
bash ./schema/init_rabbitmq.sh

echo "Validating baseline RabbitMQ latency..."
python3 "$BENCHMARK_DIR/validate_rabbitmq_latency.py" 2>/dev/null || echo "  (validation skipped)"

for queue_type in "${QUEUE_TYPES[@]}"; do
    for scenario in "${SCENARIOS[@]}"; do
        for run_num in $(seq 1 "$NUM_RUNS"); do
            run_benchmark "$queue_type" "$scenario" "$run_num"
            echo "Cooling down..."
            sleep 30
        done
    done
done

echo ""
echo "All RabbitMQ benchmarks complete. Results: $RESULTS_DIR"
