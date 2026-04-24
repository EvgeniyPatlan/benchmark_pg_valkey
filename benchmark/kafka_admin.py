#!/usr/bin/env python3
"""
Kafka admin operations via librdkafka — replaces the JVM-based
kafka-topics.sh / kafka-configs.sh / kafka-consumer-groups.sh which hang
for 30+ minutes on single-node KRaft in some VM setups.

Subcommands:
  health                        exit 0 if broker reachable; nonzero otherwise
  reset-offsets GROUP TOPIC     seek consumer group to earliest on all partitions
  lag GROUP                     print total lag (int) across all topics+partitions
  set-durability MODE TOPIC     set topic flush config per DURABILITY_MODE
                                  (none|matched|strict)

Env: BOOTSTRAP (default localhost:9092).
"""
import os
import sys
from confluent_kafka import Consumer, TopicPartition, KafkaException
from confluent_kafka.admin import (
    AdminClient, ConfigResource, ConfigEntry, AlterConfigOpType
)

BOOTSTRAP = os.getenv('BOOTSTRAP', 'localhost:9092')


def _admin():
    return AdminClient({
        'bootstrap.servers': BOOTSTRAP,
        'socket.timeout.ms': 10000,
        'request.timeout.ms': 15000,
    })


def health():
    try:
        md = _admin().list_topics(timeout=10)
        print(f"Kafka reachable at {BOOTSTRAP}: "
              f"{len(md.brokers)} broker(s), {len(md.topics)} topic(s)")
        return 0
    except Exception as e:
        print(f"Kafka UNREACHABLE at {BOOTSTRAP}: {e}")
        return 1


def reset_offsets(group, topic):
    """Seek consumer group to earliest on every partition of topic."""
    admin = _admin()
    md = admin.list_topics(topic=topic, timeout=10)
    if topic not in md.topics or md.topics[topic].error is not None:
        print(f"Topic '{topic}' missing or errored; nothing to reset.")
        return 0

    partitions = [TopicPartition(topic, p) for p in md.topics[topic].partitions]

    # Use a throwaway consumer assigned to that group to commit earliest
    # offsets for each partition.
    c = Consumer({
        'bootstrap.servers': BOOTSTRAP,
        'group.id': group,
        'enable.auto.commit': False,
        'socket.timeout.ms': 10000,
    })
    try:
        # Query low watermark per partition to commit "earliest" accurately
        committed = []
        for tp in partitions:
            low, _high = c.get_watermark_offsets(tp, timeout=5, cached=False)
            committed.append(TopicPartition(topic, tp.partition, low))
        c.commit(offsets=committed, asynchronous=False)
        print(f"Reset offsets for group={group} topic={topic}: "
              f"{len(committed)} partition(s) -> earliest")
        return 0
    finally:
        c.close()


def lag(group):
    """Print total lag (int) for a consumer group across all its topics."""
    admin = _admin()
    try:
        groups = admin.list_consumer_group_offsets(
            [type('G', (), {'group_id': group})()], require_stable=True)
    except Exception:
        # Older librdkafka: fall back to AdminClient.describe_consumer_groups
        # + Consumer-based lag probe. Simplest fallback: use a Consumer.
        pass

    c = Consumer({
        'bootstrap.servers': BOOTSTRAP,
        'group.id': group,
        'enable.auto.commit': False,
        'socket.timeout.ms': 10000,
    })
    try:
        md = c.list_topics(timeout=10)
        total_lag = 0
        # For each topic, if the group has committed offsets, compute lag
        for topic_name, topic_meta in md.topics.items():
            if topic_name.startswith('__'):  # skip internal topics
                continue
            for p_id in topic_meta.partitions:
                tp = TopicPartition(topic_name, p_id)
                try:
                    committed = c.committed([tp], timeout=5)
                    if not committed:
                        continue
                    comm_off = committed[0].offset
                    if comm_off < 0:
                        # -1001 = no committed offset for this group/partition
                        continue
                    _low, high = c.get_watermark_offsets(tp, timeout=5,
                                                         cached=False)
                    total_lag += max(0, high - comm_off)
                except KafkaException:
                    continue
        print(total_lag)
        return 0
    finally:
        c.close()


def set_durability(mode, topic):
    """Apply flush config to a topic per DURABILITY_MODE."""
    admin = _admin()
    res = ConfigResource(ConfigResource.Type.TOPIC, topic)

    if mode == 'none':
        ops = [
            ConfigEntry('flush.messages', None,
                        incremental_operation=AlterConfigOpType.DELETE),
            ConfigEntry('flush.ms', None,
                        incremental_operation=AlterConfigOpType.DELETE),
        ]
    elif mode == 'matched':
        ops = [
            ConfigEntry('flush.messages', None,
                        incremental_operation=AlterConfigOpType.DELETE),
            ConfigEntry('flush.ms', '1000',
                        incremental_operation=AlterConfigOpType.SET),
        ]
    elif mode == 'strict':
        ops = [
            ConfigEntry('flush.messages', '1',
                        incremental_operation=AlterConfigOpType.SET),
            ConfigEntry('flush.ms', '1000',
                        incremental_operation=AlterConfigOpType.SET),
        ]
    else:
        print(f"Unknown DURABILITY_MODE={mode}")
        return 1

    for op in ops:
        res.add_incremental_config(op)

    fs = admin.incremental_alter_configs([res])
    for r, f in fs.items():
        try:
            f.result(timeout=15)
        except Exception as e:
            print(f"Alter config on {r.name}: {e}")
            return 1
    print(f"Durability={mode} applied to topic '{topic}'")
    return 0


def main():
    if len(sys.argv) < 2:
        print(__doc__, file=sys.stderr)
        sys.exit(2)

    cmd = sys.argv[1]
    if cmd == 'health':
        sys.exit(health())
    elif cmd == 'reset-offsets' and len(sys.argv) >= 4:
        sys.exit(reset_offsets(sys.argv[2], sys.argv[3]))
    elif cmd == 'lag' and len(sys.argv) >= 3:
        sys.exit(lag(sys.argv[2]))
    elif cmd == 'set-durability' and len(sys.argv) >= 4:
        sys.exit(set_durability(sys.argv[2], sys.argv[3]))
    else:
        print(__doc__, file=sys.stderr)
        sys.exit(2)


if __name__ == '__main__':
    main()
