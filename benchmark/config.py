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
    'stream_name': 'bench_queue',
    'consumer_group': 'bench_workers',
}

# Benchmark parameters
BENCHMARK = {
    # Producer settings
    'production_rate': 1000,  # jobs per second
    'production_duration': 180,  # seconds
    'total_jobs': None,  # calculated as rate * duration
    
    # Job settings
    'job_size_bytes': 512,  # payload size
    'job_processing_time_ms': 5,  # simulated processing time
    
    # Worker settings - DIFFERENT FOR PG vs VALKEY
    'num_workers': 10,  # concurrent workers
    
    # PostgreSQL worker settings (row-level granularity)
    'pg_worker_batch_size': 1,  # PG works well with single-row fetches
    'pg_worker_poll_interval_ms': 10,  # fast polling for PG
    
    # Valkey worker settings (stream-level granularity - needs batching!)
    'valkey_worker_batch_size': 50,  # Fetch 50 messages per XREADGROUP call
    'valkey_worker_poll_interval_ms': 100,  # Longer block time for batching
    
    # For backwards compatibility, keep old field but mark as deprecated
    'worker_batch_size': 1,  # DEPRECATED: use pg_worker_batch_size or valkey_worker_batch_size
    'worker_poll_interval_ms': 10,  # DEPRECATED
    
    # Test scenarios
    'scenarios': ['cold', 'warm', 'load'],
    'warmup_duration': 60,  # seconds before warm test
    
    # Metrics collection
    'metrics_interval': 1,  # seconds between metric snapshots
    'latency_percentiles': [50, 95, 99, 99.9],
}

# Calculate total jobs
BENCHMARK['total_jobs'] = BENCHMARK['production_rate'] * BENCHMARK['production_duration']

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

# Load testing (for mixed load scenario)
LOAD_TEST = {
    'pgbench': {
        'enabled': True,
        'clients': 10,
        'threads': 4,
        'transactions': 10000,
    },
    'sysbench': {
        'enabled': False,  # optional
        'threads': 4,
        'test': 'cpu',
    }
}

def get_connection_string():
    """Get PostgreSQL connection string"""
    return f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"

def get_valkey_url():
    """Get Valkey connection URL"""
    return f"valkey://{VALKEY_CONFIG['host']}:{VALKEY_CONFIG['port']}"
