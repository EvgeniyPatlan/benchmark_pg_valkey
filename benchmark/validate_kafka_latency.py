#!/usr/bin/env python3
"""
Baseline Kafka round-trip latency validator.

Mirrors validate_valkey_latency.py: produces N small messages to a
throwaway topic and measures the time from produce() to the first
successful consume(), reporting p50/p95/p99 over the sample.

Use this to catch configuration issues (replication_factor>1 on single
broker, unexpected linger, disk latency on log dir) BEFORE kicking off
the long benchmark.
"""
import argparse
import os
import sys
import time
from datetime import datetime
from confluent_kafka import Producer, Consumer, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

from config import KAFKA_CONFIG


def _percentile(sorted_vals, p):
    return sorted_vals[min(len(sorted_vals) - 1, int(len(sorted_vals) * p))]


def validate(num_messages=1000, topic='bench_validation', partitions=1):
    admin = AdminClient({'bootstrap.servers': KAFKA_CONFIG['bootstrap_servers']})

    # Force-recreate the validation topic
    existing = admin.list_topics(timeout=5).topics
    if topic in existing:
        fs = admin.delete_topics([topic], operation_timeout=10)
        for t, f in fs.items():
            try:
                f.result(timeout=10)
            except Exception:
                pass
        time.sleep(2)

    fs = admin.create_topics([NewTopic(topic, partitions, replication_factor=1)])
    for t, f in fs.items():
        try:
            f.result(timeout=10)
        except Exception as e:
            print(f"Could not create validation topic: {e}")
            sys.exit(1)

    producer = Producer({
        'bootstrap.servers': KAFKA_CONFIG['bootstrap_servers'],
        'acks': '1',
        'linger.ms': 0,
    })
    consumer = Consumer({
        'bootstrap.servers': KAFKA_CONFIG['bootstrap_servers'],
        'group.id': 'bench_validation_group',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,
    })
    consumer.subscribe([topic])

    # Drain any bootstrap metadata poll
    consumer.poll(1.0)

    latencies_ms = []
    print(f"Sending {num_messages} validation messages...")
    sent = 0
    seen = 0
    while seen < num_messages:
        if sent < num_messages:
            sent_ts = time.time()
            producer.produce(
                topic=topic,
                value=b'v',
                headers=[('t', str(sent_ts).encode('utf-8'))],
            )
            producer.poll(0)
            sent += 1

        msgs = consumer.consume(num_messages=10, timeout=0.1)
        now = time.time()
        for m in msgs or []:
            if m.error():
                continue
            h = dict(m.headers() or [])
            t = h.get('t')
            if t is None:
                continue
            sent_ts = float(t.decode('utf-8'))
            latencies_ms.append((now - sent_ts) * 1000)
            seen += 1

    producer.flush(5)
    consumer.close()

    # Cleanup
    admin.delete_topics([topic], operation_timeout=10)

    latencies_ms.sort()
    print("\nKafka baseline latency:")
    print(f"  n       : {len(latencies_ms)}")
    print(f"  p50     : {_percentile(latencies_ms, 0.50):.2f} ms")
    print(f"  p95     : {_percentile(latencies_ms, 0.95):.2f} ms")
    print(f"  p99     : {_percentile(latencies_ms, 0.99):.2f} ms")
    print(f"  max     : {latencies_ms[-1]:.2f} ms")
    print("")
    print("Expected ranges on a healthy single-node install:")
    print("  p50 < 5ms; p95 < 15ms; p99 < 40ms")
    print("If p50 > 20ms: investigate log dir IO, linger.ms, or replication config.")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--num-messages', type=int, default=1000)
    args = parser.parse_args()
    validate(num_messages=args.num_messages)


if __name__ == '__main__':
    main()
