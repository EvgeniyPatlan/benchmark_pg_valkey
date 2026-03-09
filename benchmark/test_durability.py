#!/usr/bin/env python3
"""
Durability / Crash-Recovery Test
Tests job loss under different Valkey persistence modes and compares to PostgreSQL.
Requires sudo for killing/restarting services.

Usage: sudo python3 test_durability.py --num-jobs 10000
"""
import argparse
import json
import os
import signal
import subprocess
import sys
import time
import random
import string

import psycopg2

try:
    import valkey as valkey_lib
except ImportError:
    valkey_lib = None

from config import DB_CONFIG, VALKEY_CONFIG, get_stream_keys


def generate_payload(job_id, size=512):
    """Generate a test payload"""
    data = {
        'id': job_id,
        'data': ''.join(random.choices(string.ascii_letters, k=size - 50))
    }
    return json.dumps(data)


def test_pg_durability(num_jobs):
    """Test PostgreSQL crash recovery"""
    print("\n" + "=" * 60)
    print("Testing PostgreSQL Durability")
    print("=" * 60)

    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = False
    cursor = conn.cursor()

    # Clean
    cursor.execute("TRUNCATE queue_jobs")
    conn.commit()

    # Insert jobs
    print(f"Inserting {num_jobs} jobs...")
    start = time.time()
    for i in range(0, num_jobs, 100):
        batch_size = min(100, num_jobs - i)
        values = []
        for j in range(batch_size):
            payload = generate_payload(i + j)
            values.append(f"('{payload}'::jsonb, {random.randint(0, 10)})")
        cursor.execute(f"INSERT INTO queue_jobs (payload, priority) VALUES {','.join(values)}")
        conn.commit()
    elapsed = time.time() - start
    throughput = num_jobs / elapsed
    print(f"  Inserted {num_jobs} jobs in {elapsed:.1f}s ({throughput:.0f} j/s)")

    # Verify count before crash
    cursor.execute("SELECT count(*) FROM queue_jobs")
    before_count = cursor.fetchone()[0]
    print(f"  Jobs before crash: {before_count}")

    conn.close()

    # Kill PostgreSQL
    print("  Killing PostgreSQL (SIGKILL)...")
    try:
        subprocess.run(['sudo', 'pkill', '-9', 'postgres'], check=False, timeout=5)
    except Exception as e:
        print(f"  Warning: {e}")

    time.sleep(3)

    # Restart PostgreSQL
    print("  Restarting PostgreSQL...")
    try:
        subprocess.run(['sudo', 'systemctl', 'start', 'postgresql'], check=True, timeout=30)
    except Exception as e:
        print(f"  Error restarting: {e}")
        return {'mode': 'postgresql', 'crash_safe': 'error', 'jobs_before': before_count}

    time.sleep(5)

    # Count surviving jobs
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute("SELECT count(*) FROM queue_jobs")
        after_count = cursor.fetchone()[0]
        conn.close()
    except Exception as e:
        print(f"  Error counting: {e}")
        return {'mode': 'postgresql', 'crash_safe': 'error', 'jobs_before': before_count}

    jobs_lost = before_count - after_count
    print(f"  Jobs after crash:  {after_count}")
    print(f"  Jobs lost:         {jobs_lost}")

    return {
        'mode': 'postgresql (WAL)',
        'crash_safe': 'Yes' if jobs_lost == 0 else 'Partial',
        'throughput': f'{throughput:.0f} j/s',
        'jobs_before': before_count,
        'jobs_after': after_count,
        'jobs_lost': jobs_lost,
    }


def test_valkey_durability(num_jobs, persistence_mode):
    """Test Valkey crash recovery with a specific persistence mode"""
    if valkey_lib is None:
        print("ERROR: valkey module not installed")
        return None

    print(f"\n{'=' * 60}")
    print(f"Testing Valkey Durability: {persistence_mode}")
    print("=" * 60)

    client = valkey_lib.Valkey(
        host=VALKEY_CONFIG['host'],
        port=VALKEY_CONFIG['port'],
        decode_responses=False
    )

    # Configure persistence mode
    try:
        if persistence_mode == 'none':
            client.config_set('appendonly', 'no')
            client.config_set('save', '')
        elif persistence_mode == 'aof-everysec':
            client.config_set('appendonly', 'yes')
            client.config_set('appendfsync', 'everysec')
        elif persistence_mode == 'aof-always':
            client.config_set('appendonly', 'yes')
            client.config_set('appendfsync', 'always')
        elif persistence_mode == 'rdb':
            client.config_set('appendonly', 'no')
            client.config_set('save', '1 1')  # Save after 1 second if 1 key changed
    except Exception as e:
        print(f"  Warning configuring persistence: {e}")

    # Clean
    stream_keys = get_stream_keys()
    for sk in stream_keys:
        client.delete(sk)

    # Insert jobs
    print(f"  Inserting {num_jobs} jobs across {len(stream_keys)} partitions...")
    start = time.time()
    pipe = client.pipeline()
    for i in range(num_jobs):
        sk = stream_keys[i % len(stream_keys)]
        payload = generate_payload(i)
        pipe.xadd(sk, {b'payload': payload.encode(), b'id': str(i).encode()})
        if (i + 1) % 1000 == 0:
            pipe.execute()
            pipe = client.pipeline()
    pipe.execute()
    elapsed = time.time() - start
    throughput = num_jobs / elapsed
    print(f"  Inserted {num_jobs} jobs in {elapsed:.1f}s ({throughput:.0f} j/s)")

    # Count before crash
    before_count = sum(client.xlen(sk) for sk in stream_keys)
    print(f"  Jobs before crash: {before_count}")

    # Wait for persistence to potentially flush
    if persistence_mode == 'aof-everysec':
        print("  Waiting 2s for AOF flush...")
        time.sleep(2)
    elif persistence_mode == 'rdb':
        print("  Waiting 3s for RDB snapshot...")
        time.sleep(3)

    client.close()

    # Kill Valkey
    print("  Killing Valkey (SIGKILL)...")
    try:
        subprocess.run(['sudo', 'pkill', '-9', '-f', 'valkey-server'], check=False, timeout=5)
    except Exception as e:
        print(f"  Warning: {e}")

    time.sleep(3)

    # Restart Valkey
    print("  Restarting Valkey...")
    try:
        subprocess.run(['sudo', 'systemctl', 'start', 'valkey'], check=True, timeout=30)
    except Exception as e:
        print(f"  Error restarting: {e}")
        return {'mode': f'valkey ({persistence_mode})', 'crash_safe': 'error'}

    time.sleep(3)

    # Count surviving jobs
    try:
        client = valkey_lib.Valkey(
            host=VALKEY_CONFIG['host'],
            port=VALKEY_CONFIG['port'],
            decode_responses=False
        )
        after_count = sum(client.xlen(sk) for sk in stream_keys)
        client.close()
    except Exception as e:
        print(f"  Error counting: {e}")
        return {'mode': f'valkey ({persistence_mode})', 'crash_safe': 'error'}

    jobs_lost = before_count - after_count
    loss_pct = (jobs_lost / before_count * 100) if before_count > 0 else 0
    print(f"  Jobs after crash:  {after_count}")
    print(f"  Jobs lost:         {jobs_lost} ({loss_pct:.1f}%)")

    crash_safe = 'Yes' if jobs_lost == 0 else ('Partial' if after_count > 0 else 'No')

    return {
        'mode': f'valkey ({persistence_mode})',
        'crash_safe': crash_safe,
        'throughput': f'{throughput:.0f} j/s',
        'jobs_before': before_count,
        'jobs_after': after_count,
        'jobs_lost': jobs_lost,
        'loss_pct': f'{loss_pct:.1f}%',
    }


def main():
    parser = argparse.ArgumentParser(description='Test queue durability under crash')
    parser.add_argument('--num-jobs', type=int, default=10000,
                       help='Number of jobs to insert before crash')
    parser.add_argument('--output', default='durability_results.json',
                       help='Output file for results')

    args = parser.parse_args()

    # Check sudo
    if os.geteuid() != 0:
        print("WARNING: This script requires sudo to kill/restart services.")
        print("Run with: sudo python3 test_durability.py")
        print("Continuing anyway (kill/restart may fail)...\n")

    results = []

    # Test PostgreSQL
    try:
        pg_result = test_pg_durability(args.num_jobs)
        results.append(pg_result)
    except Exception as e:
        print(f"PostgreSQL test failed: {e}")

    # Test Valkey with different persistence modes
    if valkey_lib:
        for mode in ['none', 'aof-everysec', 'aof-always', 'rdb']:
            try:
                result = test_valkey_durability(args.num_jobs, mode)
                if result:
                    results.append(result)
            except Exception as e:
                print(f"Valkey ({mode}) test failed: {e}")

        # Restore Valkey to no-persistence (benchmark default)
        try:
            client = valkey_lib.Valkey(
                host=VALKEY_CONFIG['host'],
                port=VALKEY_CONFIG['port'],
                decode_responses=False
            )
            client.config_set('appendonly', 'no')
            client.config_set('save', '')
            client.close()
            print("\nRestored Valkey to no-persistence mode (benchmark default)")
        except Exception:
            pass

    # Print summary table
    print("\n" + "=" * 90)
    print("DURABILITY TEST RESULTS")
    print("=" * 90)
    print(f"{'Persistence Mode':<25} {'Crash-safe?':<14} {'Throughput':<14} "
          f"{'Jobs Lost':<12} {'Loss %':<10}")
    print("-" * 90)

    for r in results:
        mode = r.get('mode', '?')
        crash = r.get('crash_safe', '?')
        tp = r.get('throughput', '?')
        lost = r.get('jobs_lost', '?')
        loss_pct = r.get('loss_pct', 'N/A')
        print(f"{mode:<25} {crash:<14} {tp:<14} {lost:<12} {loss_pct:<10}")

    print("=" * 90)

    # Save results
    with open(args.output, 'w') as f:
        json.dump(results, f, indent=2)
    print(f"\nResults saved to: {args.output}")


if __name__ == '__main__':
    main()
