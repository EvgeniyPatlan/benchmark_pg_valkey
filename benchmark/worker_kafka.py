#!/usr/bin/env python3
"""
Kafka Consumer Worker.

Uses confluent-kafka (librdkafka) with manual commit to match the per-
message-ack semantics of the other backends as closely as possible.
Records both service latency (poll-return -> commit) and end-to-end
latency (produce-timestamp-in-header -> commit) so reviewer concern #1
applies uniformly across backends.
"""
import argparse
import json
import time
import signal
import sys
from datetime import datetime
from threading import Thread, Event
from confluent_kafka import Consumer, TopicPartition, KafkaException

from config import BENCHMARK, KAFKA_CONFIG, BROKER_TOPICS


class MetricsCollector:
    def __init__(self, output_file):
        self.output_file = output_file
        self.jobs_processed = 0
        self.latencies_e2e = []
        self.latencies_service = []
        self.latencies_broker = []
        self.start_time = time.time()
        self.lock = __import__('threading').Lock()

    def record_jobs(self, triples):
        with self.lock:
            self.jobs_processed += len(triples)
            for e2e, svc, brk in triples:
                self.latencies_e2e.append(e2e)
                self.latencies_service.append(svc)
                self.latencies_broker.append(brk)

    @staticmethod
    def _p(sorted_vals, pct):
        if not sorted_vals:
            return 0.0
        idx = min(len(sorted_vals) - 1, int(len(sorted_vals) * pct))
        return sorted_vals[idx]

    def get_metrics(self):
        with self.lock:
            if not self.latencies_e2e:
                return None
            e2e = sorted(self.latencies_e2e)
            svc = sorted(self.latencies_service)
            brk = sorted(self.latencies_broker)
            elapsed = time.time() - self.start_time
            return {
                'timestamp': datetime.now().isoformat(),
                'elapsed': elapsed,
                'jobs_processed': self.jobs_processed,
                'throughput': self.jobs_processed / elapsed if elapsed > 0 else 0,
                'latency_p50': self._p(e2e, 0.50),
                'latency_p95': self._p(e2e, 0.95),
                'latency_p99': self._p(e2e, 0.99),
                'latency_min': e2e[0], 'latency_max': e2e[-1],
                'latency_avg': sum(e2e) / len(e2e),
                'e2e_p50': self._p(e2e, 0.50),
                'e2e_p95': self._p(e2e, 0.95),
                'e2e_p99': self._p(e2e, 0.99),
                'service_p50': self._p(svc, 0.50),
                'service_p95': self._p(svc, 0.95),
                'service_p99': self._p(svc, 0.99),
                'service_min': svc[0], 'service_max': svc[-1],
                'service_avg': sum(svc) / len(svc),
                'broker_p50': self._p(brk, 0.50),
                'broker_p95': self._p(brk, 0.95),
                'broker_p99': self._p(brk, 0.99),
                'broker_min': brk[0], 'broker_max': brk[-1],
                'broker_avg': sum(brk) / len(brk),
            }

    def save_metrics(self):
        metrics = self.get_metrics()
        if metrics:
            with open(self.output_file, 'a') as f:
                f.write(json.dumps(metrics) + '\n')


class KafkaWorker:
    def __init__(self, worker_id, queue_type, metrics_collector):
        self.worker_id = f"worker_{worker_id}"
        self.metrics = metrics_collector
        self.running = Event()
        self.running.set()

        topic_cfg = BROKER_TOPICS[queue_type]
        self.topic = topic_cfg['topic']
        self.batch_size = BENCHMARK['kafka_worker_batch_size']
        self.poll_timeout_s = BENCHMARK['kafka_worker_poll_timeout_ms'] / 1000.0

        # Manual commit. enable.auto.commit=False lets us commit only after
        # simulated processing, which is the fair semantic match to
        # Valkey XACK / PG complete_job.
        self.consumer = Consumer({
            'bootstrap.servers': KAFKA_CONFIG['bootstrap_servers'],
            'group.id': KAFKA_CONFIG['consumer_group'],
            'client.id': self.worker_id,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
            'max.poll.interval.ms': 600000,  # 10 minutes, in case a batch stalls
            'session.timeout.ms': 30000,
            'fetch.min.bytes': 1,
            'fetch.wait.max.ms': BENCHMARK['kafka_worker_poll_timeout_ms'],
        })
        self.consumer.subscribe([self.topic])
        self.jobs_processed = 0

    def simulate_processing(self):
        ms = BENCHMARK['job_processing_time_ms']
        if ms > 0:
            time.sleep(ms / 1000.0)

    def poll_batch(self):
        """Pull up to batch_size messages with a single-shot poll loop."""
        cycle_start = time.monotonic()
        try:
            msgs = self.consumer.consume(num_messages=self.batch_size,
                                         timeout=self.poll_timeout_s)
        except KafkaException as e:
            print(f"[{self.worker_id}] Kafka error: {e}")
            return 0

        if not msgs:
            return 0

        real_msgs = []
        for m in msgs:
            if m.error():
                print(f"[{self.worker_id}] Consumer error: {m.error()}")
                continue
            real_msgs.append(m)

        if not real_msgs:
            return 0

        dequeue_ts = datetime.now()
        e2e_svc = []

        for m in real_msgs:
            try:
                created_at = self._extract_created_at(m)
                self.simulate_processing()
                ack_ts = datetime.now()
                e2e_ms = (ack_ts - created_at).total_seconds() * 1000
                service_ms = (ack_ts - dequeue_ts).total_seconds() * 1000
                e2e_svc.append((e2e_ms, service_ms))
                self.jobs_processed += 1
            except Exception as e:
                print(f"[{self.worker_id}] Error processing msg: {e}")

        try:
            self.consumer.commit(asynchronous=False)
        except KafkaException as e:
            print(f"[{self.worker_id}] Commit error: {e}")

        cycle_end = time.monotonic()
        n = len(e2e_svc)
        if n > 0:
            total_ms = (cycle_end - cycle_start) * 1000
            proc_ms = n * BENCHMARK['job_processing_time_ms']
            broker_ms_per = max(0.0, (total_ms - proc_ms) / n)
            triples = [(e2e, svc, broker_ms_per) for (e2e, svc) in e2e_svc]
            self.metrics.record_jobs(triples)
        return n

    @staticmethod
    def _extract_created_at(msg):
        """Pull created_at from message headers; fall back to broker timestamp."""
        headers = msg.headers() or []
        for k, v in headers:
            if k == 'created_at':
                try:
                    return datetime.fromisoformat(v.decode('utf-8'))
                except Exception:
                    break
        # Broker timestamp fallback (ms since epoch, tuple (type, ts))
        _, ts_ms = msg.timestamp()
        if ts_ms > 0:
            return datetime.fromtimestamp(ts_ms / 1000.0)
        return datetime.now()

    def run(self):
        print(f"[{self.worker_id}] Started (topic={self.topic}, batch={self.batch_size})")
        try:
            while self.running.is_set():
                self.poll_batch()
        except KeyboardInterrupt:
            pass
        finally:
            print(f"[{self.worker_id}] Stopped. Processed {self.jobs_processed} jobs")
            try:
                self.consumer.close()
            except Exception:
                pass

    def stop(self):
        self.running.clear()


def main():
    parser = argparse.ArgumentParser(description='Kafka consumer worker')
    parser.add_argument('--queue-type', choices=list(BROKER_TOPICS.keys()),
                        default='standard', help='Kafka topic variant')
    parser.add_argument('--workers', type=int, default=BENCHMARK['num_workers'],
                        help='Number of worker threads')
    parser.add_argument('--output', default='metrics_kafka.jsonl',
                        help='Metrics output file')

    args = parser.parse_args()

    topic_cfg = BROKER_TOPICS[args.queue_type]
    print(f"Starting {args.workers} Kafka workers")
    print(f"Topic: {topic_cfg['topic']} ({topic_cfg['num_partitions']} partitions)")
    print(f"Consumer group: {KAFKA_CONFIG['consumer_group']}")
    print(f"Batch size: {BENCHMARK['kafka_worker_batch_size']} (max.poll.records)")
    print(f"Processing time per job: {BENCHMARK['job_processing_time_ms']}ms (simulated)")
    print(f"Metrics output: {args.output}")
    print("-" * 50)

    metrics = MetricsCollector(args.output)

    workers = []
    threads = []
    for i in range(args.workers):
        worker = KafkaWorker(i, args.queue_type, metrics)
        thread = Thread(target=worker.run, daemon=True)
        thread.start()
        workers.append(worker)
        threads.append(thread)

    def collect_metrics():
        while True:
            time.sleep(BENCHMARK['metrics_interval'])
            metrics.save_metrics()

    metrics_thread = Thread(target=collect_metrics, daemon=True)
    metrics_thread.start()

    def signal_handler(sig, frame):
        print("\nStopping workers...")
        for worker in workers:
            worker.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    for thread in threads:
        thread.join()


if __name__ == '__main__':
    main()
