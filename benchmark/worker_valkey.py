#!/usr/bin/env python3
"""
Valkey Streams Worker - partitioned streams, proper batching

Records both SERVICE latency (dequeue -> ack) and END-TO-END latency
(enqueue -> ack) so that queue buildup from an overdriven producer can't
be conflated with Valkey's actual processing time (reviewer concern #1).
"""
import argparse
import json
import time
import signal
import sys
from datetime import datetime
from threading import Thread, Event
import valkey

from config import BENCHMARK, VALKEY_CONFIG, get_stream_keys


class MetricsCollector:
    def __init__(self, output_file):
        self.output_file = output_file
        self.jobs_processed = 0
        self.latencies_e2e = []
        self.latencies_service = []
        self.start_time = time.time()
        self.lock = __import__('threading').Lock()
        self._window_e2e = []
        self._window_service = []

    def record_jobs(self, pairs):
        """pairs: list of (e2e_ms, service_ms) tuples."""
        with self.lock:
            self.jobs_processed += len(pairs)
            for e2e, svc in pairs:
                self.latencies_e2e.append(e2e)
                self.latencies_service.append(svc)
                self._window_e2e.append(e2e)
                self._window_service.append(svc)

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

            e2e_sorted = sorted(self.latencies_e2e)
            svc_sorted = sorted(self.latencies_service)
            wsvc_sorted = sorted(self._window_service) if self._window_service else svc_sorted
            we2e_sorted = sorted(self._window_e2e) if self._window_e2e else e2e_sorted
            elapsed = time.time() - self.start_time

            metrics = {
                'timestamp': datetime.now().isoformat(),
                'elapsed': elapsed,
                'jobs_processed': self.jobs_processed,
                'throughput': self.jobs_processed / elapsed if elapsed > 0 else 0,
                # Backward-compatible names = end-to-end.
                'latency_p50': self._p(e2e_sorted, 0.50),
                'latency_p95': self._p(e2e_sorted, 0.95),
                'latency_p99': self._p(e2e_sorted, 0.99),
                'latency_min': e2e_sorted[0],
                'latency_max': e2e_sorted[-1],
                'latency_avg': sum(e2e_sorted) / len(e2e_sorted),
                # Explicit e2e.
                'e2e_p50': self._p(e2e_sorted, 0.50),
                'e2e_p95': self._p(e2e_sorted, 0.95),
                'e2e_p99': self._p(e2e_sorted, 0.99),
                # Service latency.
                'service_p50': self._p(svc_sorted, 0.50),
                'service_p95': self._p(svc_sorted, 0.95),
                'service_p99': self._p(svc_sorted, 0.99),
                'service_min': svc_sorted[0],
                'service_max': svc_sorted[-1],
                'service_avg': sum(svc_sorted) / len(svc_sorted),
                # Window (since last snapshot) for time-series plots.
                'window_e2e_p50': self._p(we2e_sorted, 0.50),
                'window_e2e_p95': self._p(we2e_sorted, 0.95),
                'window_e2e_p99': self._p(we2e_sorted, 0.99),
                'window_service_p50': self._p(wsvc_sorted, 0.50),
                'window_service_p95': self._p(wsvc_sorted, 0.95),
                'window_service_p99': self._p(wsvc_sorted, 0.99),
                'window_size': len(we2e_sorted),
            }

            self._window_e2e = []
            self._window_service = []
            return metrics

    def save_metrics(self):
        metrics = self.get_metrics()
        if metrics:
            with open(self.output_file, 'a') as f:
                f.write(json.dumps(metrics) + '\n')


class ValkeyWorker:
    def __init__(self, worker_id, metrics_collector):
        self.worker_id = f"worker_{worker_id}"
        self.metrics = metrics_collector
        self.running = Event()
        self.running.set()

        self.valkey = valkey.Valkey(
            host=VALKEY_CONFIG['host'],
            port=VALKEY_CONFIG['port'],
            decode_responses=False
        )

        self.stream_keys = get_stream_keys()
        self.consumer_group = VALKEY_CONFIG['consumer_group']
        self.jobs_processed = 0

        self.batch_size = BENCHMARK['valkey_worker_batch_size']
        self.poll_interval = BENCHMARK['valkey_worker_poll_interval_ms']

        for stream_key in self.stream_keys:
            try:
                self.valkey.xgroup_create(
                    name=stream_key,
                    groupname=self.consumer_group,
                    id='0',
                    mkstream=True
                )
            except Exception as e:
                if 'BUSYGROUP' not in str(e):
                    print(f"[{self.worker_id}] Warning on {stream_key}: {e}")

    def simulate_processing(self):
        ms = BENCHMARK['job_processing_time_ms']
        if ms > 0:
            time.sleep(ms / 1000.0)

    def process_messages(self):
        try:
            streams_dict = {key: '>' for key in self.stream_keys}
            messages = self.valkey.xreadgroup(
                groupname=self.consumer_group,
                consumername=self.worker_id,
                streams=streams_dict,
                count=self.batch_size,
                block=self.poll_interval
            )

            if not messages:
                return 0

            # Dequeue timestamp is the moment XREADGROUP returned; service
            # latency measures from there to each per-message ack.
            dequeue_ts = datetime.now()

            ack_by_stream = {}
            pairs = []

            for stream_name, stream_messages in messages:
                stream_name_str = stream_name.decode('utf-8') if isinstance(stream_name, bytes) else stream_name

                for message_id, message_data in stream_messages:
                    try:
                        created_at_str = message_data.get(b'created_at', b'').decode('utf-8')
                        created_at = datetime.fromisoformat(created_at_str) if created_at_str else datetime.now()

                        self.simulate_processing()

                        ack_ts = datetime.now()
                        e2e_ms = (ack_ts - created_at).total_seconds() * 1000
                        service_ms = (ack_ts - dequeue_ts).total_seconds() * 1000
                        pairs.append((e2e_ms, service_ms))

                        ack_by_stream.setdefault(stream_name_str, []).append(message_id)
                        self.jobs_processed += 1
                    except Exception as e:
                        print(f"[{self.worker_id}] Error processing message {message_id}: {e}")
                        continue

            for stream_key, msg_ids in ack_by_stream.items():
                try:
                    self.valkey.xack(stream_key, self.consumer_group, *msg_ids)
                except Exception as e:
                    print(f"[{self.worker_id}] Error acking on {stream_key}: {e}")

            if pairs:
                self.metrics.record_jobs(pairs)

            return sum(len(ids) for ids in ack_by_stream.values())

        except Exception as e:
            if 'NOGROUP' in str(e):
                print(f"[{self.worker_id}] Consumer group not found, recreating...")
                for stream_key in self.stream_keys:
                    try:
                        self.valkey.xgroup_create(
                            name=stream_key, groupname=self.consumer_group,
                            id='0', mkstream=True)
                    except Exception:
                        pass
            else:
                print(f"[{self.worker_id}] Error reading streams: {e}")
            return 0

    def claim_pending_messages(self):
        """Claim pending messages that timed out from other consumers."""
        total_claimed = 0

        for stream_key in self.stream_keys:
            try:
                pending = self.valkey.xpending_range(
                    name=stream_key, groupname=self.consumer_group,
                    min='-', max='+', count=10)

                if not pending:
                    continue

                claimed_ids = []
                pairs = []

                for msg in pending:
                    message_id = msg['message_id']
                    idle_time = msg['time_since_delivered']

                    if idle_time > 5000:
                        try:
                            result = self.valkey.xclaim(
                                name=stream_key,
                                groupname=self.consumer_group,
                                consumername=self.worker_id,
                                min_idle_time=5000,
                                message_ids=[message_id]
                            )

                            if result:
                                claim_ts = datetime.now()
                                for msg_id, msg_data in result:
                                    created_at_str = msg_data.get(b'created_at', b'').decode('utf-8')
                                    created_at = datetime.fromisoformat(created_at_str) if created_at_str else datetime.now()

                                    self.simulate_processing()

                                    ack_ts = datetime.now()
                                    e2e_ms = (ack_ts - created_at).total_seconds() * 1000
                                    service_ms = (ack_ts - claim_ts).total_seconds() * 1000
                                    pairs.append((e2e_ms, service_ms))
                                    claimed_ids.append(msg_id)
                                    self.jobs_processed += 1
                        except Exception as e:
                            print(f"[{self.worker_id}] Error claiming {message_id}: {e}")
                            continue

                if claimed_ids:
                    try:
                        self.valkey.xack(stream_key, self.consumer_group, *claimed_ids)
                    except Exception as e:
                        print(f"[{self.worker_id}] Error acking claimed: {e}")

                if pairs:
                    self.metrics.record_jobs(pairs)

                total_claimed += len(claimed_ids)
            except Exception as e:
                print(f"[{self.worker_id}] Error claiming on {stream_key}: {e}")
                continue

        return total_claimed

    def run(self):
        print(f"[{self.worker_id}] Started (batch={self.batch_size}, partitions={len(self.stream_keys)})")
        claim_counter = 0
        try:
            while self.running.is_set():
                self.process_messages()
                claim_counter += 1
                if claim_counter >= 10:
                    self.claim_pending_messages()
                    claim_counter = 0
        except KeyboardInterrupt:
            pass
        finally:
            print(f"[{self.worker_id}] Stopped. Processed {self.jobs_processed} jobs")
            self.valkey.close()

    def stop(self):
        self.running.clear()


def main():
    parser = argparse.ArgumentParser(description='Valkey Streams worker (partitioned, batched)')
    parser.add_argument('--workers', type=int, default=BENCHMARK['num_workers'],
                        help='Number of worker threads')
    parser.add_argument('--output', default='metrics_valkey.jsonl',
                        help='Metrics output file')
    parser.add_argument('--batch-size', type=int, default=None,
                        help='Override batch size from config')

    args = parser.parse_args()

    if args.batch_size:
        BENCHMARK['valkey_worker_batch_size'] = args.batch_size

    stream_keys = get_stream_keys()

    print(f"Starting {args.workers} Valkey workers")
    print(f"Stream partitions: {len(stream_keys)} ({', '.join(stream_keys)})")
    print(f"Consumer group: {VALKEY_CONFIG['consumer_group']}")
    print(f"Batch size: {BENCHMARK['valkey_worker_batch_size']} messages per fetch")
    print(f"Processing time per job: {BENCHMARK['job_processing_time_ms']}ms (simulated)")
    print(f"Poll interval: {BENCHMARK['valkey_worker_poll_interval_ms']} ms")
    print(f"Metrics output: {args.output}")
    print("-" * 50)

    metrics = MetricsCollector(args.output)

    workers = []
    threads = []
    for i in range(args.workers):
        worker = ValkeyWorker(i, metrics)
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
