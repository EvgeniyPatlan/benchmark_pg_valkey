#!/usr/bin/env python3
"""
Kafka topic initializer — via librdkafka (confluent-kafka).

Replaces the kafka-topics.sh shell wrapper because:
  1. JVM cold-start adds ~10-15s per invocation; orchestration calls
     init before every scenario run (15+ times per suite).
  2. The Java admin client on single-node KRaft occasionally hangs
     indefinitely during initial metadata fetch — observed in
     benchmark VM runs.
librdkafka has different retry semantics and returns in ~100ms.

Environment overrides:
  BOOTSTRAP   (default: localhost:9092)
  TOPIC       (default: bench_queue)
  PARTITIONS  (default: 8)
  REPLICATION (default: 1)
"""
import os
import sys
import time
from confluent_kafka.admin import AdminClient, NewTopic

BOOTSTRAP = os.getenv('BOOTSTRAP', 'localhost:9092')
TOPIC = os.getenv('TOPIC', 'bench_queue')
PARTITIONS = int(os.getenv('PARTITIONS', '8'))
REPLICATION = int(os.getenv('REPLICATION', '1'))


def main():
    print(f"Initializing Kafka topic '{TOPIC}' with {PARTITIONS} partitions...")

    admin = AdminClient({
        'bootstrap.servers': BOOTSTRAP,
        'socket.timeout.ms': 10000,
        'request.timeout.ms': 15000,
    })

    # Reachability + discovery
    try:
        md = admin.list_topics(timeout=10)
    except Exception as e:
        print(f"ERROR: Kafka not reachable at {BOOTSTRAP}: {e}")
        sys.exit(1)
    print(f"Broker reachable. {len(md.brokers)} broker(s), "
          f"{len(md.topics)} topic(s) known.")

    # Idempotent reset: delete if present
    if TOPIC in md.topics:
        fs = admin.delete_topics([TOPIC], operation_timeout=30)
        for t, f in fs.items():
            try:
                f.result(timeout=30)
                print(f"Deleted existing topic: {t}")
            except Exception as e:
                print(f"Delete {t}: {e}")
        time.sleep(2)

    # Create fresh
    fs = admin.create_topics([
        NewTopic(
            TOPIC, PARTITIONS, replication_factor=REPLICATION,
            config={
                'retention.ms': '21600000',
                'segment.bytes': '1073741824',
            },
        )
    ])
    for t, f in fs.items():
        try:
            f.result(timeout=30)
            print(f"Created topic: {t} ({PARTITIONS} partitions, rf={REPLICATION})")
        except Exception as e:
            print(f"ERROR creating {t}: {e}")
            sys.exit(1)

    print("")
    print(f"Bootstrap:   {BOOTSTRAP}")
    print(f"Topic:       {TOPIC}")
    print(f"Partitions:  {PARTITIONS}")
    print(f"Replication: {REPLICATION}")


if __name__ == '__main__':
    main()
