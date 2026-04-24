#!/bin/bash
# Run Kafka benchmarks. Mirrors run_valkey_tests.sh in structure so results
# files line up with the rest of the pipeline (analyze.py auto-discovers).

set -e

RESULTS_DIR="${RESULTS_DIR:-./results/vm3_kafka}"
BENCHMARK_DIR="./benchmark"
QUEUE_TYPES=("standard")
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

TOPIC="${TOPIC:-bench_queue}"
BOOTSTRAP="${BOOTSTRAP:-localhost:9092}"
CONSUMER_GROUP="${CONSUMER_GROUP:-bench_workers}"

if [ -z "$PGPASSWORD" ]; then
    echo "ERROR: PGPASSWORD environment variable not set (needed for pgbench load)"
    echo "Run: export PGPASSWORD=bench_pass"
    exit 1
fi

export PGHOST=localhost PGPORT=5432 PGDATABASE=bench_db PGUSER=bench_user

mkdir -p "$RESULTS_DIR"

save_environment() {
    local env_file="$RESULTS_DIR/environment.txt"
    {
        echo "Test Environment"
        echo "================"
        echo "Scope: 1 Kafka broker (KRaft mode, single-node deployment)"
        echo "Date: $(date -Iseconds)"
        echo "OS: $(lsb_release -ds 2>/dev/null || cat /etc/os-release | head -1)"
        echo "Kernel: $(uname -r)"
        echo "CPU: $(nproc) vCPU ($(lscpu | grep 'Model name' | sed 's/Model name: *//'))"
        echo "RAM: $(free -h | awk '/Mem:/{print $2}')"
        echo "Disk: $(df -h / | awk 'NR==2{print $2, $5}')"
        echo "Kafka: $(timeout 5 /opt/kafka/bin/kafka-topics.sh --version 2>&1 | head -1 || echo 'unknown (kafka-topics.sh not reachable)')"
        echo "PostgreSQL: $(psql --version)"
        echo "Python: $(python3 --version)"
        echo ""
        echo "Kafka Configuration:"
        echo "  Bootstrap: $BOOTSTRAP"
        echo "  Topic: $TOPIC"
        echo "  Partitions: 8"
        echo "  Replication factor: 1 (single broker — NOT a production setting)"
        echo "  Consumer group: $CONSUMER_GROUP"
        echo "  Durability mode: $DURABILITY_MODE"
        echo "  Producer acks: $([ "$DURABILITY_MODE" = "none" ] && echo 1 || echo all)"
        echo "  Worker batch size: 50 (max.poll.records equivalent)"
        echo ""
        echo "Load Scenario Configuration (identical to VM1/VM2 for symmetry):"
        echo "  Tool: pgbench"
        echo "  Clients: $PGBENCH_CLIENTS"
        echo "  Threads: $PGBENCH_THREADS"
        echo "  Duration: ${PGBENCH_DURATION}s"
        echo "  Scale factor: $PGBENCH_SCALE"
        echo ""
        echo "Benchmark Parameters:"
        echo "  Production rate: ${PRODUCER_RATE} jobs/sec (pre-cap)"
        echo "  Auto-cap: $PRODUCER_AUTO_CAP (fraction=0.9, capacity=$CAPACITY_FILE)"
        echo "  Duration: 180 seconds"
        echo "  Workers: $NUM_WORKERS"
        echo "  Job size: 512 bytes"
        echo "  Processing time: ${JOB_PROCESSING_TIME_MS}ms (simulated)"
        echo "  Runs per scenario: $NUM_RUNS"
        echo ""
        echo "Fairness caveat:"
        echo "  Single-broker Kafka is atypical; production deployments use ≥3"
        echo "  brokers with replication_factor=3. This benchmark measures"
        echo "  single-broker performance to enable apples-to-apples comparison"
        echo "  with single-node PG/Valkey/RabbitMQ."
    } > "$env_file"
    echo "Saved environment info to $env_file"
}

apply_durability_mode() {
    # Delegates to benchmark/kafka_admin.py (librdkafka, no JVM) because
    # kafka-configs.sh hangs on single-node KRaft in VM environments.
    BOOTSTRAP="$BOOTSTRAP" python3 "$BENCHMARK_DIR/kafka_admin.py" \
        set-durability "$DURABILITY_MODE" "$TOPIC" || {
            echo "ERROR: failed to apply DURABILITY_MODE=$DURABILITY_MODE"
            exit 1
        }
}

clean_kafka() {
    echo "Resetting topic (recreate to drain)..."
    bash ./schema/init_kafka.sh > /dev/null
    # Reset consumer group offsets via librdkafka (no JVM hang).
    BOOTSTRAP="$BOOTSTRAP" python3 "$BENCHMARK_DIR/kafka_admin.py" \
        reset-offsets "$CONSUMER_GROUP" "$TOPIC" > /dev/null 2>&1 || true
}

get_total_lag() {
    BOOTSTRAP="$BOOTSTRAP" python3 "$BENCHMARK_DIR/kafka_admin.py" \
        lag "$CONSUMER_GROUP" 2>/dev/null || echo 0
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

    echo ""
    echo "=========================================="
    echo "Running: Kafka $queue_type - $scenario (run $run_num/$NUM_RUNS)"
    echo "=========================================="

    clean_kafka

    local producer_extra=()
    if [ "$PRODUCER_AUTO_CAP" == "1" ]; then
        producer_extra=(--auto-cap --capacity-file "$CAPACITY_FILE")
    fi

    if [ "$scenario" == "warm" ]; then
        echo "Warming up..."
        python3 "$BENCHMARK_DIR/worker_kafka.py" \
            --queue-type "$queue_type" --workers "$NUM_WORKERS" \
            --output "${result_prefix}_warmup_metrics.jsonl" &
        WORKER_PID=$!
        python3 "$BENCHMARK_DIR/producer.py" \
            --backend kafka --queue-type "$queue_type" \
            --rate "$PRODUCER_RATE" --duration 60 "${producer_extra[@]}"
        sleep 30
        kill "$WORKER_PID" 2>/dev/null || true
        wait "$WORKER_PID" 2>/dev/null || true
        clean_kafka
        echo "Warmup complete"
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
    python3 "$BENCHMARK_DIR/worker_kafka.py" \
        --queue-type "$queue_type" --workers "$NUM_WORKERS" \
        --output "${result_prefix}_metrics.jsonl" &
    WORKER_PID=$!
    sleep 2

    echo "Starting producer (${PRODUCER_RATE} jobs/sec for 180s, auto-cap=$PRODUCER_AUTO_CAP)..."
    python3 "$BENCHMARK_DIR/producer.py" \
        --backend kafka --queue-type "$queue_type" \
        --rate "$PRODUCER_RATE" --duration 180 "${producer_extra[@]}" \
        > "${result_prefix}_producer.log" 2>&1

    echo "Producer finished. Waiting for consumer lag to drain..."
    MAX_WAIT=300
    ELAPSED=0
    INTERVAL=5
    while [ $ELAPSED -lt $MAX_WAIT ]; do
        LAG=$(get_total_lag)
        if [ "$LAG" -eq 0 ] 2>/dev/null; then
            echo "All messages consumed."
            break
        fi
        echo "Still processing... ($LAG messages lag)"
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

    echo "Benchmark complete: Kafka $queue_type - $scenario (run $run_num)"
    echo "Results: $result_prefix"
}

# Main
echo "Kafka Benchmark Suite"
echo "Results: $RESULTS_DIR"
echo "Runs per scenario: $NUM_RUNS"

# Health check via librdkafka (no JVM, returns fast)
BOOTSTRAP="$BOOTSTRAP" python3 "$BENCHMARK_DIR/kafka_admin.py" health > /dev/null || {
    echo "ERROR: Kafka not running. sudo systemctl start kafka"
    exit 1
}

apply_durability_mode
save_environment

echo "Initializing pgbench..."
pgbench -h localhost -i -s "$PGBENCH_SCALE" > /dev/null 2>&1 || true

echo "Initializing Kafka topic..."
bash ./schema/init_kafka.sh

echo "Validating baseline Kafka latency..."
python3 "$BENCHMARK_DIR/validate_kafka_latency.py" 2>/dev/null || echo "  (validation skipped)"

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
echo "All Kafka benchmarks complete. Results: $RESULTS_DIR"
