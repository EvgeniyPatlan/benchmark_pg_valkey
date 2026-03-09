#!/usr/bin/env python3
"""
Measure Application Query Impact
Measures regular PostgreSQL query latency to show resource contention effects.
Run during queue benchmarks to demonstrate that PG queue ops slow down app queries
while Valkey queue ops (on separate infra) do not.
"""
import argparse
import json
import time
import random
import statistics
import sys
import psycopg2

from config import DB_CONFIG


def measure_queries(conn, num_queries=1000):
    """Run simple app-like queries and measure latency"""
    cursor = conn.cursor()

    select_latencies = []
    update_latencies = []

    for i in range(num_queries):
        # SELECT query (read a random account)
        aid = random.randint(1, 100000)
        start = time.monotonic()
        cursor.execute("SELECT abalance FROM pgbench_accounts WHERE aid = %s", (aid,))
        cursor.fetchone()
        select_latencies.append((time.monotonic() - start) * 1000)

        # UPDATE query (update a random account balance)
        aid = random.randint(1, 100000)
        delta = random.randint(-100, 100)
        start = time.monotonic()
        cursor.execute("UPDATE pgbench_accounts SET abalance = abalance + %s WHERE aid = %s",
                       (delta, aid))
        conn.commit()
        update_latencies.append((time.monotonic() - start) * 1000)

        # Small delay to spread queries over time
        if i % 10 == 0:
            time.sleep(0.01)

    return select_latencies, update_latencies


def compute_stats(latencies):
    """Compute percentile statistics"""
    if not latencies:
        return {}
    latencies.sort()
    n = len(latencies)
    return {
        'count': n,
        'mean': statistics.mean(latencies),
        'stddev': statistics.stdev(latencies) if n > 1 else 0,
        'min': latencies[0],
        'max': latencies[-1],
        'p50': latencies[int(n * 0.50)],
        'p95': latencies[int(n * 0.95)],
        'p99': latencies[int(n * 0.99)],
    }


def main():
    parser = argparse.ArgumentParser(description='Measure app query latency')
    parser.add_argument('--mode', choices=['baseline', 'pg-queue', 'valkey-queue'],
                       default='baseline',
                       help='Measurement context')
    parser.add_argument('--num-queries', type=int, default=1000,
                       help='Number of queries to run')
    parser.add_argument('--output', default='app_queries.json',
                       help='Output JSON file')

    args = parser.parse_args()

    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = False
    except Exception as e:
        print(f"Cannot connect to PostgreSQL: {e}")
        sys.exit(1)

    # Check pgbench tables exist
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT count(*) FROM pgbench_accounts")
        count = cursor.fetchone()[0]
        if count == 0:
            print("WARNING: pgbench_accounts is empty. Run: pgbench -i -s 10")
            sys.exit(1)
    except Exception as e:
        print(f"pgbench tables not found: {e}. Run: pgbench -i -s 10")
        conn.rollback()
        sys.exit(1)

    print(f"Measuring app query latency ({args.mode}, {args.num_queries} queries)...")
    start = time.time()
    select_lats, update_lats = measure_queries(conn, args.num_queries)
    elapsed = time.time() - start

    select_stats = compute_stats(select_lats)
    update_stats = compute_stats(update_lats)

    results = {
        'mode': args.mode,
        'num_queries': args.num_queries,
        'elapsed_sec': elapsed,
        'select_p50_ms': select_stats.get('p50', 0),
        'select_p95_ms': select_stats.get('p95', 0),
        'select_p99_ms': select_stats.get('p99', 0),
        'select_mean_ms': select_stats.get('mean', 0),
        'select_stddev_ms': select_stats.get('stddev', 0),
        'update_p50_ms': update_stats.get('p50', 0),
        'update_p95_ms': update_stats.get('p95', 0),
        'update_p99_ms': update_stats.get('p99', 0),
        'update_mean_ms': update_stats.get('mean', 0),
        'update_stddev_ms': update_stats.get('stddev', 0),
    }

    with open(args.output, 'w') as f:
        json.dump(results, f, indent=2)

    print(f"\nResults ({args.mode}):")
    print(f"  SELECT  p50={select_stats['p50']:.2f}ms  p95={select_stats['p95']:.2f}ms  "
          f"p99={select_stats['p99']:.2f}ms  mean={select_stats['mean']:.2f}ms")
    print(f"  UPDATE  p50={update_stats['p50']:.2f}ms  p95={update_stats['p95']:.2f}ms  "
          f"p99={update_stats['p99']:.2f}ms  mean={update_stats['mean']:.2f}ms")
    print(f"Saved to: {args.output}")

    conn.close()


if __name__ == '__main__':
    main()
