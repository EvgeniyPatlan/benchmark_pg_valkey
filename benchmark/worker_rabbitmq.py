#!/usr/bin/env python3
"""
RabbitMQ Consumer Worker.

Uses pika BlockingConnection with basic_qos(prefetch_count=50) and manual
basic_ack. Records service latency (delivery -> ack) and end-to-end
latency (created_at header -> ack) like the other backends.

One worker process per variant (classic or quorum) — don't run both
variants concurrently against the same RabbitMQ instance unless the
hardware budget is enough that they're not interfering.
"""
import argparse
import json
import time
import signal
import sys
from datetime import datetime
from threading import Thread, Event
import pika

from config import BENCHMARK, RABBITMQ_CONFIG, BROKER_QUEUES


class MetricsCollector:
    def __init__(self, output_file):
        self.output_file = output_file
        self.jobs_processed = 0
        self.latencies_e2e = []
        self.latencies_service = []
        self.start_time = time.time()
        self.lock = __import__('threading').Lock()

    def record_job(self, e2e_ms, service_ms):
        with self.lock:
            self.jobs_processed += 1
            self.latencies_e2e.append(e2e_ms)
            self.latencies_service.append(service_ms)

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
            }

    def save_metrics(self):
        metrics = self.get_metrics()
        if metrics:
            with open(self.output_file, 'a') as f:
                f.write(json.dumps(metrics) + '\n')


class RabbitMQWorker:
    def __init__(self, worker_id, queue_type, metrics_collector):
        self.worker_id = f"worker_{worker_id}"
        self.metrics = metrics_collector
        self.running = Event()
        self.running.set()

        q_cfg = BROKER_QUEUES[queue_type]
        self.queue_name = q_cfg['queue_name']
        self.queue_type = q_cfg['queue_type']
        self.prefetch = BENCHMARK['rabbitmq_prefetch_count']

        creds = pika.PlainCredentials(RABBITMQ_CONFIG['user'], RABBITMQ_CONFIG['password'])
        params = pika.ConnectionParameters(
            host=RABBITMQ_CONFIG['host'],
            port=RABBITMQ_CONFIG['port'],
            virtual_host=RABBITMQ_CONFIG['vhost'],
            credentials=creds,
            heartbeat=30,
            blocked_connection_timeout=30,
        )
        self.conn = pika.BlockingConnection(params)
        self.ch = self.conn.channel()

        # Declare queue (idempotent — matches producer's declaration).
        arguments = {'x-queue-type': 'quorum'} if self.queue_type == 'quorum' else {}
        self.ch.queue_declare(
            queue=self.queue_name,
            durable=q_cfg['durable'],
            arguments=arguments,
        )
        self.ch.basic_qos(prefetch_count=self.prefetch)
        self.jobs_processed = 0

    def simulate_processing(self):
        ms = BENCHMARK['job_processing_time_ms']
        if ms > 0:
            time.sleep(ms / 1000.0)

    def _on_message(self, ch, method, properties, body):
        """Per-message callback. pika dispatches one at a time even with
        prefetch>1 (prefetch is about flow control, not batching), so we
        measure service latency as delivery-time → ack-time."""
        dequeue_ts = datetime.now()
        try:
            created_at = datetime.now()
            if properties and properties.headers:
                ts_str = properties.headers.get('created_at')
                if ts_str:
                    try:
                        created_at = datetime.fromisoformat(ts_str)
                    except Exception:
                        pass

            self.simulate_processing()
            ack_ts = datetime.now()

            ch.basic_ack(delivery_tag=method.delivery_tag)

            e2e_ms = (ack_ts - created_at).total_seconds() * 1000
            service_ms = (ack_ts - dequeue_ts).total_seconds() * 1000
            self.metrics.record_job(e2e_ms, service_ms)
            self.jobs_processed += 1
        except Exception as e:
            print(f"[{self.worker_id}] Error processing: {e}")
            try:
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
            except Exception:
                pass

    def run(self):
        print(f"[{self.worker_id}] Started (queue={self.queue_name}, type={self.queue_type}, "
              f"prefetch={self.prefetch})")
        try:
            self.ch.basic_consume(
                queue=self.queue_name,
                on_message_callback=self._on_message,
                auto_ack=False,
            )
            # Use a short blocking loop so stop() can interrupt cleanly.
            while self.running.is_set():
                self.conn.process_data_events(time_limit=1.0)
        except KeyboardInterrupt:
            pass
        except Exception as e:
            print(f"[{self.worker_id}] Consumer loop exited: {e}")
        finally:
            print(f"[{self.worker_id}] Stopped. Processed {self.jobs_processed} jobs")
            try:
                self.conn.close()
            except Exception:
                pass

    def stop(self):
        self.running.clear()


def main():
    parser = argparse.ArgumentParser(description='RabbitMQ consumer worker')
    parser.add_argument('--queue-type', choices=list(BROKER_QUEUES.keys()),
                        required=True, help='Queue variant: classic or quorum')
    parser.add_argument('--workers', type=int, default=BENCHMARK['num_workers'],
                        help='Number of worker threads')
    parser.add_argument('--output', default='metrics_rabbitmq.jsonl',
                        help='Metrics output file')

    args = parser.parse_args()

    q_cfg = BROKER_QUEUES[args.queue_type]
    print(f"Starting {args.workers} RabbitMQ workers")
    print(f"Queue: {q_cfg['queue_name']} (type={q_cfg['queue_type']})")
    print(f"Prefetch: {BENCHMARK['rabbitmq_prefetch_count']}")
    print(f"Processing time per job: {BENCHMARK['job_processing_time_ms']}ms (simulated)")
    print(f"Metrics output: {args.output}")
    print("-" * 50)

    metrics = MetricsCollector(args.output)

    workers = []
    threads = []
    for i in range(args.workers):
        worker = RabbitMQWorker(i, args.queue_type, metrics)
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
