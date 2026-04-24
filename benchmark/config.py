"""
Benchmark configuration
"""
import os

# Database configuration
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': int(os.getenv('DB_PORT', '5432')),
    'database': os.getenv('DB_NAME', 'bench_db'),
    'user': os.getenv('DB_USER', 'bench_user'),
    'password': os.getenv('DB_PASS', 'bench_pass'),
}

# Valkey configuration
VALKEY_CONFIG = {
    'host': os.getenv('VALKEY_HOST', 'localhost'),
    'port': int(os.getenv('VALKEY_PORT', '6379')),
    'stream_name': 'bench_queue',          # prefix for partitioned keys
    'consumer_group': 'bench_workers',
    'num_stream_partitions': 8,            # keys: bench_queue:0 .. bench_queue:7
}

# Kafka configuration (VM3). Single broker, KRaft mode, 8 partitions to
# match Valkey's 8 stream keys so "best single-node config" is comparable.
KAFKA_CONFIG = {
    'bootstrap_servers': os.getenv('KAFKA_BOOTSTRAP', 'localhost:9092'),
    'topic': 'bench_queue',
    'num_partitions': 8,
    'consumer_group': 'bench_workers',
    'client_id_prefix': 'bench',
}

# RabbitMQ configuration (VM4). Two queue variants exposed via BROKER_QUEUES.
RABBITMQ_CONFIG = {
    'host': os.getenv('RABBITMQ_HOST', 'localhost'),
    'port': int(os.getenv('RABBITMQ_PORT', '5672')),
    'user': os.getenv('RABBITMQ_USER', 'bench_user'),
    'password': os.getenv('RABBITMQ_PASS', 'bench_pass'),
    'vhost': os.getenv('RABBITMQ_VHOST', '/'),
}

def get_stream_keys():
    """Return list of partitioned stream keys"""
    n = VALKEY_CONFIG['num_stream_partitions']
    prefix = VALKEY_CONFIG['stream_name']
    if n <= 1:
        return [prefix]
    return [f"{prefix}:{i}" for i in range(n)]

# Benchmark parameters
BENCHMARK = {
    # Producer settings
    'production_rate': 1000,  # jobs per second
    'production_duration': 180,  # seconds
    'total_jobs': None,  # calculated as rate * duration

    # Producer rate-capping (reviewer concern #1): optionally cap the producer
    # at a fraction of measured sustained throughput, so arrival rate never
    # exceeds service rate and p95 doesn't just measure queue buildup.
    # Set via env var PRODUCER_AUTO_CAP=1 or --auto-cap on producer.py.
    'producer_auto_cap_fraction': float(os.getenv('PRODUCER_AUTO_CAP_FRACTION', '0.9')),

    # Job settings
    'job_size_bytes': 512,  # payload size
    # Simulated job processing time. Set via env var JOB_PROCESSING_TIME_MS to
    # override per-run. 0 = near-instant (measures raw queue overhead);
    # 50 = realistic I/O-ish workload (measures lock contention). Reviewer
    # concern #4: the default 5ms hides row-lock contention because the lock
    # is only held for microseconds of actual work.
    'job_processing_time_ms': float(os.getenv('JOB_PROCESSING_TIME_MS', '5')),

    # Worker settings - DIFFERENT FOR PG vs VALKEY
    'num_workers': 10,  # concurrent workers

    # PostgreSQL worker settings (row-level granularity)
    'pg_worker_batch_size': 1,  # PG skip_locked/delete_returning/partitioned: single-row fetches
    'pg_worker_poll_interval_ms': 10,  # fast polling for PG

    # PG batch-dequeue variant (reviewer concern #3): multi-row SELECT ... LIMIT N
    # inside a single transaction, batched UPDATE/DELETE for completion. Makes
    # the PG vs Valkey comparison apples-to-apples on batching.
    'pg_batch_worker_batch_size': 100,
    'pg_batch_worker_poll_interval_ms': 10,

    # Valkey worker settings (stream-level granularity - needs batching!)
    'valkey_worker_batch_size': 50,  # Fetch 50 messages per XREADGROUP call
    'valkey_worker_poll_interval_ms': 100,  # Longer block time for batching

    # Kafka consumer settings. max.poll.records=50 matches Valkey batch size
    # so the reviewer's batched-vs-unbatched asymmetry (#3) doesn't re-emerge.
    'kafka_worker_batch_size': 50,           # max.poll.records
    'kafka_worker_poll_timeout_ms': 100,     # poll() timeout
    'kafka_producer_linger_ms': 5,           # small linger to allow batching
    'kafka_producer_batch_size': 16384,      # bytes, the librdkafka default

    # RabbitMQ settings. prefetch_count=50 matches the batching convention.
    # Confirms are enabled only when DURABILITY_MODE != 'none'.
    'rabbitmq_prefetch_count': 50,
    'rabbitmq_publisher_confirms_default': False,  # overridden by durability mode

    # Test scenarios
    'scenarios': ['cold', 'warm', 'load'],
    'warmup_duration': 60,  # seconds before warm test

    # Multi-run for statistical rigor
    'num_runs': 5,  # run each scenario this many times

    # Metrics collection
    'metrics_interval': 1,  # seconds between metric snapshots
    'latency_percentiles': [50, 95, 99, 99.9],
}

# Calculate total jobs
BENCHMARK['total_jobs'] = BENCHMARK['production_rate'] * BENCHMARK['production_duration']

# Durability modes (reviewer concern #2). Default 'none' matches existing
# results and the historical README claim. 'matched' enables fsync-every-second
# on Valkey (appendfsync everysec) and disables synchronous commit on PG
# (synchronous_commit=off) — roughly equivalent durability ceilings.
# 'strict' is PG default + Valkey appendfsync always.
DURABILITY_MODE = os.getenv('DURABILITY_MODE', 'none')
assert DURABILITY_MODE in ('none', 'matched', 'strict'), \
    f"DURABILITY_MODE must be one of none|matched|strict, got {DURABILITY_MODE!r}"

# PostgreSQL queue types
QUEUE_TYPES = {
    'skip_locked': {
        'table': 'queue_jobs',
        'get_function': 'get_next_job',
        'complete_function': 'complete_job',
        'fail_function': 'fail_job',
    },
    'delete_returning': {
        'table': 'queue_jobs_dr',
        'get_function': 'get_next_job_dr',
        'complete_function': 'complete_job_dr',
        'fail_function': 'requeue_job_dr',
    },
    'partitioned': {
        'table': 'queue_jobs_part',
        'get_function': 'get_next_job_part',
        'complete_function': 'complete_job_part',
        'fail_function': 'fail_job_part',
        'partitions': 16,
    },
    # Reviewer concern #3: batched SKIP LOCKED dequeue. Same underlying table
    # as 'skip_locked' (queue_jobs); the only difference is the fetch function,
    # which returns up to pg_batch_worker_batch_size rows per call.
    'skip_locked_batch': {
        'table': 'queue_jobs',
        'get_function': 'get_next_jobs_batch',
        'complete_function': 'complete_jobs_batch',
        'fail_function': 'fail_job',
        'batched': True,
    },
}

# Kafka topic registry — one entry per tested variant. Currently only
# 'standard' (8 partitions, consumer group). A 'single_partition' variant
# could be added here later for a no-parallelism baseline; not included
# by default because it just measures serial XREADGROUP equivalent.
BROKER_TOPICS = {
    'standard': {
        'topic': KAFKA_CONFIG['topic'],
        'num_partitions': KAFKA_CONFIG['num_partitions'],
        'replication_factor': 1,
    },
}

# RabbitMQ queue registry — two variants per reviewer-mirroring principle.
# 'classic' is the historical default (mnesia-backed). 'quorum' is Raft-
# backed; on a single node its durability guarantee is degenerate but
# throughput/latency characteristics are still informative (caveat
# documented in environment.txt and README).
BROKER_QUEUES = {
    'classic': {
        'queue_name': 'bench_queue_classic',
        'queue_type': 'classic',
        'durable': True,
    },
    'quorum': {
        'queue_name': 'bench_queue_quorum',
        'queue_type': 'quorum',
        'durable': True,  # quorum queues are always durable; kept for symmetry
    },
}

# Results directory
RESULTS_DIR = 'results'
os.makedirs(RESULTS_DIR, exist_ok=True)

# Monitoring
MONITORING = {
    'collect_system_metrics': True,
    'collect_db_metrics': True,
    'collect_valkey_metrics': True,
    'system_metrics_interval': 1,  # seconds
}

# Load testing (for mixed load scenario). Kept identical on both VMs so that
# the Valkey-under-load and PG-under-load comparisons are symmetric
# (reviewer smaller note: VM2 pgbench equivalence).
LOAD_TEST = {
    'pgbench': {
        'enabled': True,
        'clients': 10,
        'threads': 4,
        'duration_sec': 320,
        'scale_factor': 10,
        'transactions': 10000,  # legacy; unused when duration_sec is set
    },
    'sysbench': {
        'enabled': False,  # optional
        'threads': 4,
        'test': 'cpu',
    }
}

# Test environment documentation (fill in when running)
ENVIRONMENT = {
    'scope': '1 Valkey standalone node vs 1 PostgreSQL node',
    'os': os.popen('lsb_release -ds 2>/dev/null || cat /etc/os-release 2>/dev/null | head -1').read().strip(),
    'kernel': os.popen('uname -r').read().strip(),
    'cpu': os.popen("nproc").read().strip() + ' vCPU',
    'ram': os.popen("free -h | awk '/Mem:/{print $2}'").read().strip(),
    'pg_version': '16',
    'valkey_version': '8.0.1',
    'python_version': os.popen('python3 --version').read().strip(),
    'psycopg2_version': '2.9.9',
    'valkey_driver': 'valkey-py',
    'durability_mode': DURABILITY_MODE,
}


def get_connection_string():
    """Get PostgreSQL connection string"""
    return f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"

def get_valkey_url():
    """Get Valkey connection URL"""
    return f"valkey://{VALKEY_CONFIG['host']}:{VALKEY_CONFIG['port']}"


def get_kafka_producer_acks():
    """Producer acks setting per durability mode.

    acks=1      : leader ack only (durability 'none')
    acks=all    : leader + all ISR ack (durability 'matched' / 'strict')
    """
    return '1' if DURABILITY_MODE == 'none' else 'all'


def get_rabbitmq_confirms_enabled():
    """Whether to enable publisher confirms per durability mode."""
    return DURABILITY_MODE != 'none'


def get_amqp_url():
    """Get RabbitMQ AMQP URL"""
    c = RABBITMQ_CONFIG
    return f"amqp://{c['user']}:{c['password']}@{c['host']}:{c['port']}{c['vhost']}"
