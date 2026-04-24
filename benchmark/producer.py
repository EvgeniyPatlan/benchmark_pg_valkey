#!/usr/bin/env python3
"""
Job Producer — one producer, four backends.

Keeps the arrival-side logic (rate, batching, auto-cap, timestamp
embedding) identical across pg/valkey/kafka/rabbitmq so the latency
comparison is valid.

Per backend:
  pg        : execute_values INSERT into the right queue table.
  valkey    : pipelined XADD round-robining across 8 stream partitions.
  kafka     : Producer.produce() round-robining partition key; linger_ms
              + batch.size let librdkafka batch; manual flush per tick.
  rabbitmq  : basic_publish into the selected queue (classic or quorum);
              publisher confirms enabled when DURABILITY_MODE != 'none'.
"""
import argparse
import json
import os
import time
import sys
import random
import string
from datetime import datetime
import psycopg2
from psycopg2.extras import execute_values

# Optional imports — each backend's client is imported lazily so a VM that
# doesn't need it doesn't have to install all of them.
try:
    import valkey
except ImportError:
    valkey = None

try:
    from confluent_kafka import Producer as _KafkaProducer
except ImportError:
    _KafkaProducer = None

try:
    import pika
except ImportError:
    pika = None

from config import (
    BENCHMARK, DB_CONFIG, VALKEY_CONFIG, KAFKA_CONFIG, RABBITMQ_CONFIG,
    QUEUE_TYPES, BROKER_TOPICS, BROKER_QUEUES,
    DURABILITY_MODE, get_stream_keys,
    get_kafka_producer_acks, get_rabbitmq_confirms_enabled,
)


def resolve_rate(backend, queue_type, requested_rate, auto_cap=False, capacity_file=None):
    """Resolve the producer arrival rate.

    If auto_cap, cap at producer_auto_cap_fraction * measured sustained
    throughput from capacity_file (reviewer concern #1).

    capacity_file schema:
      {
        "pg:skip_locked":        448.0,
        "pg:skip_locked_batch":  950.0,
        "pg:delete_returning":   520.0,
        "pg:partitioned":        693.0,
        "valkey:streams":       2100.0,
        "kafka:standard":       1800.0,
        "rabbitmq:classic":      800.0,
        "rabbitmq:quorum":       600.0
      }
    """
    if not auto_cap:
        return requested_rate

    if capacity_file is None or not os.path.exists(capacity_file):
        print(f"WARNING: --auto-cap requested but capacity file missing: {capacity_file}")
        print(f"         Falling back to requested rate ({requested_rate} j/s).")
        return requested_rate

    with open(capacity_file) as f:
        caps = json.load(f)

    if backend == 'valkey':
        key = 'valkey:streams'
    else:
        key = f"{backend}:{queue_type}"
    measured = caps.get(key)
    if measured is None:
        print(f"WARNING: no capacity measurement for {key} in {capacity_file}")
        print(f"         Falling back to requested rate ({requested_rate} j/s).")
        return requested_rate

    fraction = BENCHMARK['producer_auto_cap_fraction']
    capped = int(measured * fraction)
    if capped < requested_rate:
        print(f"AUTO-CAP: {key} service capacity = {measured:.0f} j/s; "
              f"capping producer at {fraction:.0%} = {capped} j/s "
              f"(requested {requested_rate}).")
        return capped
    return requested_rate


class Producer:
    def __init__(self, backend='pg', queue_type='skip_locked', rate=5000, duration=300):
        self.backend = backend
        self.queue_type = queue_type
        self.rate = rate
        self.duration = duration
        self.total_jobs = rate * duration

        self.jobs_produced = 0
        self.start_time = None

        if backend == 'pg':
            self.conn = psycopg2.connect(**DB_CONFIG)
            self.conn.autocommit = False
            self.table = QUEUE_TYPES[queue_type]['table']
        elif backend == 'valkey':
            self._init_valkey()
        elif backend == 'kafka':
            self._init_kafka()
        elif backend == 'rabbitmq':
            self._init_rabbitmq()
        else:
            raise ValueError(f"Unknown backend: {backend}")

    def _init_valkey(self):
        if valkey is None:
            print("ERROR: valkey module not installed! Install: pip3 install valkey")
            sys.exit(1)
        self.valkey = valkey.Valkey(
            host=VALKEY_CONFIG['host'],
            port=VALKEY_CONFIG['port'],
            decode_responses=False,
        )
        self.stream_keys = get_stream_keys()
        self.num_partitions = len(self.stream_keys)
        self.consumer_group = VALKEY_CONFIG['consumer_group']
        for stream_key in self.stream_keys:
            try:
                self.valkey.xgroup_create(
                    name=stream_key, groupname=self.consumer_group,
                    id='0', mkstream=True)
            except Exception as e:
                if 'BUSYGROUP' not in str(e):
                    print(f"Warning creating consumer group on {stream_key}: {e}")

    def _init_kafka(self):
        if _KafkaProducer is None:
            print("ERROR: confluent-kafka not installed! Install: pip3 install confluent-kafka")
            sys.exit(1)
        topic_cfg = BROKER_TOPICS[self.queue_type]
        self.topic = topic_cfg['topic']
        self.num_partitions = topic_cfg['num_partitions']
        self.kafka = _KafkaProducer({
            'bootstrap.servers': KAFKA_CONFIG['bootstrap_servers'],
            'client.id': f"{KAFKA_CONFIG['client_id_prefix']}-producer",
            'acks': get_kafka_producer_acks(),
            'linger.ms': BENCHMARK['kafka_producer_linger_ms'],
            'batch.size': BENCHMARK['kafka_producer_batch_size'],
            'compression.type': 'none',  # keep payload-size comparison honest
        })

    def _init_rabbitmq(self):
        if pika is None:
            print("ERROR: pika not installed! Install: pip3 install pika")
            sys.exit(1)
        q_cfg = BROKER_QUEUES[self.queue_type]
        self.rabbitmq_queue = q_cfg['queue_name']
        self.rabbitmq_queue_type = q_cfg['queue_type']
        creds = pika.PlainCredentials(RABBITMQ_CONFIG['user'], RABBITMQ_CONFIG['password'])
        params = pika.ConnectionParameters(
            host=RABBITMQ_CONFIG['host'],
            port=RABBITMQ_CONFIG['port'],
            virtual_host=RABBITMQ_CONFIG['vhost'],
            credentials=creds,
            heartbeat=30,
        )
        self.rabbitmq_conn = pika.BlockingConnection(params)
        self.rabbitmq_ch = self.rabbitmq_conn.channel()
        # Queue declaration is idempotent; workers also declare.
        arguments = {}
        if self.rabbitmq_queue_type == 'quorum':
            arguments['x-queue-type'] = 'quorum'
        self.rabbitmq_ch.queue_declare(
            queue=self.rabbitmq_queue,
            durable=q_cfg['durable'],
            arguments=arguments,
        )
        if get_rabbitmq_confirms_enabled():
            self.rabbitmq_ch.confirm_delivery()
        self.rabbitmq_confirms = get_rabbitmq_confirms_enabled()

    def generate_payload(self):
        size = BENCHMARK['job_size_bytes']
        data = {
            'id': self.jobs_produced,
            'timestamp': datetime.now().isoformat(),
            'data': ''.join(random.choices(string.ascii_letters + string.digits, k=size - 100))
        }
        return json.dumps(data)

    def produce_pg_batch(self, batch_size):
        cursor = self.conn.cursor()
        jobs = []
        for _ in range(batch_size):
            payload = self.generate_payload()
            priority = random.randint(0, 10)
            if self.queue_type == 'partitioned':
                partition_key = random.randint(0, 15)
                jobs.append((partition_key, payload, priority))
            else:
                jobs.append((payload, priority))
        try:
            if self.queue_type == 'partitioned':
                execute_values(cursor,
                    f"INSERT INTO {self.table} (partition_key, payload, priority) VALUES %s",
                    jobs)
            else:
                execute_values(cursor,
                    f"INSERT INTO {self.table} (payload, priority) VALUES %s",
                    jobs)
            self.conn.commit()
            self.jobs_produced += batch_size
            return batch_size
        except Exception as e:
            self.conn.rollback()
            print(f"Error producing PG batch: {e}")
            return 0

    def produce_valkey_batch(self, batch_size):
        try:
            pipeline = self.valkey.pipeline()
            for _ in range(batch_size):
                payload = self.generate_payload()
                priority = random.randint(0, 10)
                partition_idx = self.jobs_produced % self.num_partitions
                stream_key = self.stream_keys[partition_idx]
                pipeline.xadd(stream_key, {
                    b'payload': payload.encode('utf-8'),
                    b'priority': str(priority).encode('utf-8'),
                    b'created_at': datetime.now().isoformat().encode('utf-8'),
                })
                self.jobs_produced += 1
            pipeline.execute()
            return batch_size
        except Exception as e:
            print(f"Error producing Valkey batch: {e}")
            return 0

    def produce_kafka_batch(self, batch_size):
        try:
            for _ in range(batch_size):
                payload = self.generate_payload()
                priority = random.randint(0, 10)
                # Round-robin partition key — explicit so the comparison
                # mirrors Valkey's round-robin across stream_keys.
                partition = self.jobs_produced % self.num_partitions
                headers = [
                    ('created_at', datetime.now().isoformat().encode('utf-8')),
                    ('priority', str(priority).encode('utf-8')),
                ]
                self.kafka.produce(
                    topic=self.topic,
                    value=payload.encode('utf-8'),
                    partition=partition,
                    headers=headers,
                )
                self.jobs_produced += 1
            # Force send — we want to measure latency, not producer-side
            # buffering time.
            self.kafka.poll(0)
            self.kafka.flush(timeout=10)
            return batch_size
        except Exception as e:
            print(f"Error producing Kafka batch: {e}")
            return 0

    def produce_rabbitmq_batch(self, batch_size):
        try:
            props_kwargs = {
                'delivery_mode': 2,  # persistent (queue durability-respecting)
                'headers': {'priority': 0},
            }
            for _ in range(batch_size):
                payload = self.generate_payload()
                priority = random.randint(0, 10)
                # created_at lives in headers (matches Valkey/Kafka convention
                # so payload bytes stay comparable across backends).
                props = pika.BasicProperties(
                    delivery_mode=2 if self.rabbitmq_queue_type == 'classic' else None,
                    headers={
                        'created_at': datetime.now().isoformat(),
                        'priority': priority,
                    },
                )
                self.rabbitmq_ch.basic_publish(
                    exchange='',
                    routing_key=self.rabbitmq_queue,
                    body=payload.encode('utf-8'),
                    properties=props,
                    mandatory=self.rabbitmq_confirms,
                )
                self.jobs_produced += 1
            return batch_size
        except Exception as e:
            # pika raises on unroutable if confirms enabled; treat as
            # partial success — report what we incremented.
            print(f"Error producing RabbitMQ batch: {e}")
            return 0

    def run(self):
        print(f"Starting producer: {self.backend}/{self.queue_type}")
        print(f"Rate: {self.rate} jobs/sec, duration: {self.duration}s, total: {self.total_jobs}")
        if self.backend == 'valkey':
            print(f"Stream partitions: {self.num_partitions} ({', '.join(self.stream_keys)})")
        elif self.backend == 'kafka':
            print(f"Topic: {self.topic}, partitions: {self.num_partitions}, "
                  f"acks={get_kafka_producer_acks()}")
        elif self.backend == 'rabbitmq':
            print(f"Queue: {self.rabbitmq_queue} (type={self.rabbitmq_queue_type}), "
                  f"confirms={self.rabbitmq_confirms}")
        print("-" * 50)

        self.start_time = time.time()
        batch_size = max(1, self.rate // 100)  # 100 batches per second
        interval = batch_size / self.rate

        dispatch = {
            'pg': self.produce_pg_batch,
            'valkey': self.produce_valkey_batch,
            'kafka': self.produce_kafka_batch,
            'rabbitmq': self.produce_rabbitmq_batch,
        }
        produce = dispatch[self.backend]

        next_batch_time = self.start_time

        try:
            while self.jobs_produced < self.total_jobs:
                produce(batch_size)

                next_batch_time += interval
                sleep_time = next_batch_time - time.time()
                if sleep_time > 0:
                    time.sleep(sleep_time)

                elapsed = time.time() - self.start_time
                if self.jobs_produced % (self.rate * 10) == 0:
                    actual_rate = self.jobs_produced / elapsed if elapsed > 0 else 0
                    print(f"Produced: {self.jobs_produced}/{self.total_jobs} "
                          f"({actual_rate:.0f} jobs/sec)")

        except KeyboardInterrupt:
            print("\nProducer interrupted")
        finally:
            elapsed = time.time() - self.start_time
            actual_rate = self.jobs_produced / elapsed if elapsed > 0 else 0
            print("-" * 50)
            print(f"Producer finished")
            print(f"Total jobs produced: {self.jobs_produced}")
            print(f"Elapsed time: {elapsed:.1f} seconds")
            print(f"Actual rate: {actual_rate:.0f} jobs/sec")

            if self.backend == 'pg':
                self.conn.close()
            elif self.backend == 'valkey':
                self.valkey.close()
            elif self.backend == 'kafka':
                self.kafka.flush(timeout=10)
            elif self.backend == 'rabbitmq':
                try:
                    self.rabbitmq_conn.close()
                except Exception:
                    pass


def _queue_type_choices():
    """Union of PG queue types, Kafka topics, RabbitMQ queues."""
    return sorted(set(QUEUE_TYPES) | set(BROKER_TOPICS) | set(BROKER_QUEUES) | {'streams'})


def main():
    parser = argparse.ArgumentParser(description='Benchmark job producer')
    parser.add_argument('--backend', choices=['pg', 'valkey', 'kafka', 'rabbitmq'],
                        required=True,
                        help='Target backend')
    parser.add_argument('--queue-type',
                        choices=_queue_type_choices(),
                        default='skip_locked',
                        help='PG: skip_locked|skip_locked_batch|delete_returning|partitioned. '
                             'Valkey: streams (default). Kafka: standard. '
                             'RabbitMQ: classic|quorum.')
    parser.add_argument('--rate', type=int, default=BENCHMARK['production_rate'],
                        help='Production rate (jobs/sec) — pre-cap')
    parser.add_argument('--duration', type=int, default=BENCHMARK['production_duration'],
                        help='Production duration (seconds)')
    parser.add_argument('--auto-cap', action='store_true',
                        help='Cap arrival rate at producer_auto_cap_fraction '
                             '(default 90%%) of measured service capacity, '
                             'read from --capacity-file. Reviewer concern #1.')
    parser.add_argument('--capacity-file', default='results/capacity.json',
                        help='JSON file with measured service capacity '
                             'per backend/queue (populated by a prior run).')

    args = parser.parse_args()

    # Default queue_type per backend if user left it as the global default.
    if args.backend == 'valkey' and args.queue_type == 'skip_locked':
        args.queue_type = 'streams'
    elif args.backend == 'kafka' and args.queue_type == 'skip_locked':
        args.queue_type = 'standard'
    elif args.backend == 'rabbitmq' and args.queue_type == 'skip_locked':
        args.queue_type = 'classic'

    effective_rate = resolve_rate(
        backend=args.backend,
        queue_type=args.queue_type,
        requested_rate=args.rate,
        auto_cap=args.auto_cap,
        capacity_file=args.capacity_file,
    )

    producer = Producer(
        backend=args.backend,
        queue_type=args.queue_type,
        rate=effective_rate,
        duration=args.duration,
    )

    producer.run()


if __name__ == '__main__':
    main()
