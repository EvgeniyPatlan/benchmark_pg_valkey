#!/usr/bin/env python3
"""
PostgreSQL Queue Worker

Records both SERVICE latency (dequeue -> ack, the database's contribution)
and END-TO-END latency (enqueue -> ack, including queue wait time), so an
overdriven producer can't inflate p95 with queueing delay without that being
visible in the results (reviewer concern #1).

Supports all four queue types:
  - skip_locked        : SELECT FOR UPDATE SKIP LOCKED (single-row)
  - delete_returning   : atomic DELETE RETURNING
  - partitioned        : hash-partitioned queue table
  - skip_locked_batch  : SKIP LOCKED with LIMIT N dequeue + batched completion
"""
import argparse
import json
import time
import signal
import sys
from datetime import datetime
from threading import Thread, Event
import psycopg2
from psycopg2.extras import RealDictCursor

from config import BENCHMARK, DB_CONFIG, QUEUE_TYPES


class MetricsCollector:
    """Tracks two latency distributions per run.

    e2e_ms     : enqueue_ts -> ack_ts. Reflects user-visible latency but is
                 dominated by queue wait time when arrival > service rate.
    service_ms : dequeue_ts -> ack_ts. Reflects the database's processing
                 time and is the fair number to compare across backends when
                 the arrival rate is held below service capacity.
    """
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

    def record_jobs(self, pairs):
        """Record multiple (e2e_ms, service_ms) pairs at once (batched)."""
        with self.lock:
            self.jobs_processed += len(pairs)
            for e2e, svc in pairs:
                self.latencies_e2e.append(e2e)
                self.latencies_service.append(svc)

    @staticmethod
    def _percentile(sorted_vals, p):
        if not sorted_vals:
            return 0.0
        idx = min(len(sorted_vals) - 1, int(len(sorted_vals) * p))
        return sorted_vals[idx]

    def get_metrics(self):
        with self.lock:
            if not self.latencies_e2e:
                return None

            e2e_sorted = sorted(self.latencies_e2e)
            svc_sorted = sorted(self.latencies_service)
            elapsed = time.time() - self.start_time

            return {
                'timestamp': datetime.now().isoformat(),
                'elapsed': elapsed,
                'jobs_processed': self.jobs_processed,
                'throughput': self.jobs_processed / elapsed if elapsed > 0 else 0,
                # End-to-end (enqueue -> ack). Backward-compatible names so
                # existing analyze.py/generate_graphs.py keep working.
                'latency_p50': self._percentile(e2e_sorted, 0.50),
                'latency_p95': self._percentile(e2e_sorted, 0.95),
                'latency_p99': self._percentile(e2e_sorted, 0.99),
                'latency_min': e2e_sorted[0],
                'latency_max': e2e_sorted[-1],
                'latency_avg': sum(e2e_sorted) / len(e2e_sorted),
                # Explicit e2e copy (so future readers can tell which is which).
                'e2e_p50': self._percentile(e2e_sorted, 0.50),
                'e2e_p95': self._percentile(e2e_sorted, 0.95),
                'e2e_p99': self._percentile(e2e_sorted, 0.99),
                # Service latency (dequeue -> ack). The fair database metric.
                'service_p50': self._percentile(svc_sorted, 0.50),
                'service_p95': self._percentile(svc_sorted, 0.95),
                'service_p99': self._percentile(svc_sorted, 0.99),
                'service_min': svc_sorted[0],
                'service_max': svc_sorted[-1],
                'service_avg': sum(svc_sorted) / len(svc_sorted),
            }

    def save_metrics(self):
        metrics = self.get_metrics()
        if metrics:
            with open(self.output_file, 'a') as f:
                f.write(json.dumps(metrics) + '\n')


class PostgreSQLWorker:
    def __init__(self, worker_id, queue_type, metrics_collector):
        self.worker_id_num = worker_id
        self.worker_id = f"worker_{worker_id}"
        self.queue_type = queue_type
        self.metrics = metrics_collector
        self.running = Event()
        self.running.set()

        self.conn = psycopg2.connect(**DB_CONFIG)
        self.conn.autocommit = True

        self.config = QUEUE_TYPES[queue_type]
        self.jobs_processed = 0
        self.is_batched = bool(self.config.get('batched'))

        if queue_type == 'partitioned':
            num_workers = BENCHMARK['num_workers']
            num_partitions = self.config['partitions']
            partitions_per_worker = max(1, num_partitions // num_workers)
            start_partition = (worker_id % num_partitions)
            self.partitions = [
                (start_partition + i) % num_partitions
                for i in range(partitions_per_worker)
            ]
        else:
            self.partitions = None

    def simulate_processing(self):
        """Simulate per-job processing work."""
        ms = BENCHMARK['job_processing_time_ms']
        if ms > 0:
            time.sleep(ms / 1000.0)

    def process_job_basic(self):
        cursor = self.conn.cursor(cursor_factory=RealDictCursor)
        try:
            cursor.execute(
                f"SELECT * FROM {self.config['get_function']}(%s)",
                (self.worker_id,)
            )
            job = cursor.fetchone()

            if not job:
                time.sleep(BENCHMARK['pg_worker_poll_interval_ms'] / 1000.0)
                return False

            dequeue_ts = datetime.now()
            job_id = job['job_id']
            created_at = job['job_created_at']

            self.simulate_processing()

            cursor.execute(
                f"SELECT {self.config['complete_function']}(%s)",
                (job_id,)
            )

            ack_ts = datetime.now()
            e2e_ms = (ack_ts - created_at).total_seconds() * 1000
            service_ms = (ack_ts - dequeue_ts).total_seconds() * 1000
            self.metrics.record_job(e2e_ms, service_ms)
            self.jobs_processed += 1
            return True
        except Exception as e:
            print(f"[{self.worker_id}] Error processing job: {e}")
            return False

    def process_job_delete_returning(self):
        cursor = self.conn.cursor(cursor_factory=RealDictCursor)
        try:
            cursor.execute(
                f"SELECT * FROM {self.config['get_function']}(%s)",
                (self.worker_id,)
            )
            job = cursor.fetchone()

            if not job:
                time.sleep(BENCHMARK['pg_worker_poll_interval_ms'] / 1000.0)
                return False

            dequeue_ts = datetime.now()
            job_id = job['job_id']
            created_at = job['job_created_at']
            payload = job['job_payload']
            priority = job['job_priority']

            self.simulate_processing()

            cursor.execute(
                f"SELECT {self.config['complete_function']}(%s, %s, %s, %s, %s)",
                (job_id, json.dumps(payload), priority, created_at, self.worker_id)
            )

            ack_ts = datetime.now()
            e2e_ms = (ack_ts - created_at).total_seconds() * 1000
            service_ms = (ack_ts - dequeue_ts).total_seconds() * 1000
            self.metrics.record_job(e2e_ms, service_ms)
            self.jobs_processed += 1
            return True
        except Exception as e:
            print(f"[{self.worker_id}] Error processing job: {e}")
            return False

    def process_job_partitioned(self):
        cursor = self.conn.cursor(cursor_factory=RealDictCursor)
        for partition_key in self.partitions:
            try:
                cursor.execute(
                    f"SELECT * FROM {self.config['get_function']}(%s, %s)",
                    (partition_key, self.worker_id)
                )
                job = cursor.fetchone()
                if not job:
                    continue

                dequeue_ts = datetime.now()
                job_id = job['job_id']
                created_at = job['job_created_at']

                self.simulate_processing()

                cursor.execute(
                    f"SELECT {self.config['complete_function']}(%s, %s)",
                    (partition_key, job_id)
                )

                ack_ts = datetime.now()
                e2e_ms = (ack_ts - created_at).total_seconds() * 1000
                service_ms = (ack_ts - dequeue_ts).total_seconds() * 1000
                self.metrics.record_job(e2e_ms, service_ms)
                self.jobs_processed += 1
                return True
            except Exception as e:
                print(f"[{self.worker_id}] Error processing job from partition {partition_key}: {e}")
                continue

        time.sleep(BENCHMARK['pg_worker_poll_interval_ms'] / 1000.0)
        return False

    def process_job_batch(self):
        """SKIP LOCKED with LIMIT N (reviewer concern #3).

        Fetches up to pg_batch_worker_batch_size rows in one transaction,
        processes them, then bulk-completes in one UPDATE. Service latency
        is measured per-row (dequeue_ts is the common fetch timestamp for
        the batch, ack_ts is per-row after simulated work).
        """
        cursor = self.conn.cursor(cursor_factory=RealDictCursor)
        batch_size = BENCHMARK['pg_batch_worker_batch_size']
        try:
            cursor.execute(
                f"SELECT * FROM {self.config['get_function']}(%s, %s)",
                (self.worker_id, batch_size)
            )
            rows = cursor.fetchall()
            if not rows:
                time.sleep(BENCHMARK['pg_batch_worker_poll_interval_ms'] / 1000.0)
                return False

            dequeue_ts = datetime.now()

            # Simulate per-job work sequentially. Using one sleep for the
            # whole batch would artificially help the batched variant; we
            # want realistic per-job CPU/IO time.
            pairs = []
            for job in rows:
                self.simulate_processing()
                ack_ts = datetime.now()
                e2e_ms = (ack_ts - job['job_created_at']).total_seconds() * 1000
                service_ms = (ack_ts - dequeue_ts).total_seconds() * 1000
                pairs.append((e2e_ms, service_ms))

            # Bulk-complete in one statement.
            job_ids = [row['job_id'] for row in rows]
            cursor.execute(
                f"SELECT {self.config['complete_function']}(%s::bigint[])",
                (job_ids,)
            )

            self.metrics.record_jobs(pairs)
            self.jobs_processed += len(rows)
            return True
        except Exception as e:
            print(f"[{self.worker_id}] Error processing batch: {e}")
            return False

    def run(self):
        print(f"[{self.worker_id}] Started ({self.queue_type})")
        try:
            while self.running.is_set():
                if self.queue_type == 'delete_returning':
                    self.process_job_delete_returning()
                elif self.queue_type == 'partitioned':
                    self.process_job_partitioned()
                elif self.is_batched:
                    self.process_job_batch()
                else:
                    self.process_job_basic()
        except KeyboardInterrupt:
            pass
        finally:
            print(f"[{self.worker_id}] Stopped. Processed {self.jobs_processed} jobs")
            self.conn.close()

    def stop(self):
        self.running.clear()


def main():
    parser = argparse.ArgumentParser(description='PostgreSQL queue worker')
    parser.add_argument('--queue-type',
                        choices=list(QUEUE_TYPES.keys()),
                        required=True, help='Queue type')
    parser.add_argument('--workers', type=int, default=BENCHMARK['num_workers'],
                        help='Number of worker threads')
    parser.add_argument('--output', default='metrics_pg.jsonl',
                        help='Metrics output file')

    args = parser.parse_args()

    print(f"Starting {args.workers} PostgreSQL workers ({args.queue_type})")
    print(f"Processing time per job: {BENCHMARK['job_processing_time_ms']}ms (simulated)")
    print(f"Metrics output: {args.output}")
    print("-" * 50)

    metrics = MetricsCollector(args.output)

    workers = []
    threads = []
    for i in range(args.workers):
        worker = PostgreSQLWorker(i, args.queue_type, metrics)
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
