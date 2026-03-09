#!/usr/bin/env python3
"""
Validate Baseline Valkey Latency
Diagnoses hot-shard, network RTT, and configuration issues.
Run this before the full benchmark to ensure Valkey is properly configured.
"""
import time
import statistics
import sys

try:
    import valkey
except ImportError:
    print("ERROR: valkey module not installed. pip3 install valkey")
    sys.exit(1)

from config import VALKEY_CONFIG, get_stream_keys


def measure_latency(client, stream_key, group_name, consumer_name, num_ops=1000):
    """Measure XADD + XREADGROUP round-trip latency"""
    # Ensure consumer group exists
    try:
        client.xgroup_create(stream_key, group_name, '0', mkstream=True)
    except Exception as e:
        if 'BUSYGROUP' not in str(e):
            raise

    latencies = []
    for i in range(num_ops):
        start = time.monotonic()

        # XADD
        msg_id = client.xadd(stream_key, {b'test': b'1', b'seq': str(i).encode()})

        # XREADGROUP
        msgs = client.xreadgroup(
            groupname=group_name,
            consumername=consumer_name,
            streams={stream_key: '>'},
            count=1,
            block=1000
        )

        # XACK
        if msgs:
            for sname, smsgs in msgs:
                for mid, _ in smsgs:
                    client.xack(stream_key, group_name, mid)

        elapsed_ms = (time.monotonic() - start) * 1000
        latencies.append(elapsed_ms)

    return latencies


def measure_batch_latency(client, stream_keys, group_name, consumer_name,
                          num_batches=200, batch_size=50):
    """Measure batch XADD + multi-stream XREADGROUP latency"""
    # Ensure consumer groups exist
    for sk in stream_keys:
        try:
            client.xgroup_create(sk, group_name, '0', mkstream=True)
        except Exception as e:
            if 'BUSYGROUP' not in str(e):
                raise

    latencies = []
    for batch_idx in range(num_batches):
        start = time.monotonic()

        # Batch XADD across partitions
        pipe = client.pipeline()
        for i in range(batch_size):
            sk = stream_keys[i % len(stream_keys)]
            pipe.xadd(sk, {b'test': b'1', b'seq': str(batch_idx * batch_size + i).encode()})
        pipe.execute()

        # Multi-stream XREADGROUP
        streams_dict = {sk: '>' for sk in stream_keys}
        msgs = client.xreadgroup(
            groupname=group_name,
            consumername=consumer_name,
            streams=streams_dict,
            count=batch_size,
            block=1000
        )

        # Batch XACK
        if msgs:
            for sname, smsgs in msgs:
                if smsgs:
                    ids = [mid for mid, _ in smsgs]
                    sname_str = sname.decode() if isinstance(sname, bytes) else sname
                    client.xack(sname_str, group_name, *ids)

        elapsed_ms = (time.monotonic() - start) * 1000
        per_msg_ms = elapsed_ms / batch_size
        latencies.append(per_msg_ms)

    return latencies


def print_stats(name, latencies):
    """Print latency statistics"""
    latencies.sort()
    n = len(latencies)
    p50 = latencies[int(n * 0.50)]
    p95 = latencies[int(n * 0.95)]
    p99 = latencies[int(n * 0.99)]
    avg = statistics.mean(latencies)
    sd = statistics.stdev(latencies) if n > 1 else 0

    print(f"  {name}:")
    print(f"    p50:  {p50:.3f} ms")
    print(f"    p95:  {p95:.3f} ms")
    print(f"    p99:  {p99:.3f} ms")
    print(f"    avg:  {avg:.3f} ms  (stddev: {sd:.3f})")
    print(f"    min:  {latencies[0]:.3f} ms")
    print(f"    max:  {latencies[-1]:.3f} ms")
    return p50, p95, p99


def main():
    print("=" * 60)
    print("Valkey Baseline Latency Validation")
    print("=" * 60)

    client = valkey.Valkey(
        host=VALKEY_CONFIG['host'],
        port=VALKEY_CONFIG['port'],
        decode_responses=False
    )

    # Test connection
    try:
        client.ping()
        print(f"Connected to Valkey at {VALKEY_CONFIG['host']}:{VALKEY_CONFIG['port']}")
    except Exception as e:
        print(f"ERROR: Cannot connect to Valkey: {e}")
        sys.exit(1)

    # Check network latency
    print("\n--- Network Latency (PING) ---")
    ping_lats = []
    for _ in range(100):
        start = time.monotonic()
        client.ping()
        ping_lats.append((time.monotonic() - start) * 1000)
    print_stats("PING round-trip", ping_lats)

    # Test 1: Single stream key (the hot-shard scenario)
    print("\n--- Test 1: Single Stream Key (hot-shard) ---")
    test_stream = '__validate_single'
    client.delete(test_stream)
    single_lats = measure_latency(client, test_stream, '__validate_group', '__validate_consumer', 1000)
    s_p50, s_p95, s_p99 = print_stats("Single-key XADD+XREADGROUP+XACK", single_lats)
    client.delete(test_stream)

    # Test 2: Partitioned stream keys
    stream_keys = get_stream_keys()
    print(f"\n--- Test 2: Partitioned Streams ({len(stream_keys)} keys) ---")
    test_keys = [f'__validate_part:{i}' for i in range(len(stream_keys))]
    for k in test_keys:
        client.delete(k)
    part_lats = measure_batch_latency(client, test_keys, '__validate_group', '__validate_consumer',
                                       num_batches=200, batch_size=50)
    p_p50, p_p95, p_p99 = print_stats("Partitioned batch (per-message latency)", part_lats)
    for k in test_keys:
        client.delete(k)

    # Summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"  Single-key p50:    {s_p50:.3f} ms")
    print(f"  Partitioned p50:   {p_p50:.3f} ms")
    if s_p50 > 0:
        print(f"  Improvement:       {(s_p50 - p_p50) / s_p50 * 100:.1f}%")

    print()
    if p_p50 < 5:
        print("  PASS: Partitioned Valkey latency is healthy (< 5ms p50)")
    elif p_p50 < 20:
        print("  WARN: Latency is elevated. Check persistence mode and network.")
    else:
        print("  FAIL: Latency is too high (> 20ms). Investigate:")
        print("    - valkey-cli CONFIG GET appendonly")
        print("    - valkey-cli CONFIG GET save")
        print("    - Check if running on same host as benchmark")

    if s_p50 > 5 and p_p50 < s_p50 * 0.7:
        print(f"\n  Hot-shard detected: single-key is {s_p50/p_p50:.1f}x slower than partitioned")

    client.close()


if __name__ == '__main__':
    main()
