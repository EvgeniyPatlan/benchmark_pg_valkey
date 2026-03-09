#!/usr/bin/env python3
"""
Valkey Streams Worker - WITH PARTITIONED STREAMS AND PROPER BATCHING
Reads from multiple stream keys to avoid hot-shard antipattern.
Uses batch XREADGROUP across partitions + batch XACK.
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
        self.latencies = []
        self.start_time = time.time()
        self.lock = __import__('threading').Lock()
        self._window_start = time.time()
        self._window_latencies = []

    def record_jobs(self, latencies_list):
        """Record multiple jobs at once (for batch processing)"""
        with self.lock:
            self.jobs_processed += len(latencies_list)
            self.latencies.extend(latencies_list)
            self._window_latencies.extend(latencies_list)

    def get_metrics(self):
        with self.lock:
            if not self.latencies:
                return None

            sorted_latencies = sorted(self.latencies)
            n = len(sorted_latencies)

            # Window stats (since last snapshot) for more accurate recent metrics
            window_sorted = sorted(self._window_latencies) if self._window_latencies else sorted_latencies
            wn = len(window_sorted)

            metrics = {
                'timestamp': datetime.now().isoformat(),
                'elapsed': time.time() - self.start_time,
                'jobs_processed': self.jobs_processed,
                'throughput': self.jobs_processed / (time.time() - self.start_time),
                # Cumulative percentiles
                'latency_p50': sorted_latencies[int(n * 0.50)] if n > 0 else 0,
                'latency_p95': sorted_latencies[int(n * 0.95)] if n > 0 else 0,
                'latency_p99': sorted_latencies[int(n * 0.99)] if n > 0 else 0,
                'latency_min': sorted_latencies[0] if n > 0 else 0,
                'latency_max': sorted_latencies[-1] if n > 0 else 0,
                'latency_avg': sum(sorted_latencies) / n if n > 0 else 0,
                # Window percentiles (recent)
                'window_p50': window_sorted[int(wn * 0.50)] if wn > 0 else 0,
                'window_p95': window_sorted[int(wn * 0.95)] if wn > 0 else 0,
                'window_p99': window_sorted[int(wn * 0.99)] if wn > 0 else 0,
                'window_size': wn,
            }

            # Reset window
            self._window_latencies = []
            self._window_start = time.time()

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

        # Use Valkey-specific batch size
        self.batch_size = BENCHMARK['valkey_worker_batch_size']
        self.poll_interval = BENCHMARK['valkey_worker_poll_interval_ms']

        # Ensure consumer group exists on all partitions
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
        """Simulate job processing"""
        time.sleep(BENCHMARK['job_processing_time_ms'] / 1000.0)

    def process_messages(self):
        """Process messages from partitioned Valkey Streams"""
        try:
            # Build streams dict for XREADGROUP across ALL partitions
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

            # Group message IDs by stream key for batch acknowledgment
            ack_by_stream = {}
            batch_latencies = []

            for stream_name, stream_messages in messages:
                # stream_name is bytes
                if isinstance(stream_name, bytes):
                    stream_name_str = stream_name.decode('utf-8')
                else:
                    stream_name_str = stream_name

                for message_id, message_data in stream_messages:
                    try:
                        created_at_str = message_data.get(b'created_at', b'').decode('utf-8')

                        if created_at_str:
                            created_at = datetime.fromisoformat(created_at_str)
                        else:
                            created_at = datetime.now()

                        # Simulate processing
                        self.simulate_processing()

                        # Calculate latency
                        latency_ms = (datetime.now() - created_at).total_seconds() * 1000
                        batch_latencies.append(latency_ms)

                        # Add to per-stream ack batch
                        if stream_name_str not in ack_by_stream:
                            ack_by_stream[stream_name_str] = []
                        ack_by_stream[stream_name_str].append(message_id)
                        self.jobs_processed += 1

                    except Exception as e:
                        print(f"[{self.worker_id}] Error processing message {message_id}: {e}")
                        continue

            # Batch acknowledge per stream partition
            for stream_key, msg_ids in ack_by_stream.items():
                try:
                    self.valkey.xack(stream_key, self.consumer_group, *msg_ids)
                except Exception as e:
                    print(f"[{self.worker_id}] Error acking on {stream_key}: {e}")

            # Record metrics for entire batch
            if batch_latencies:
                self.metrics.record_jobs(batch_latencies)

            return sum(len(ids) for ids in ack_by_stream.values())

        except Exception as e:
            if 'NOGROUP' in str(e):
                print(f"[{self.worker_id}] Consumer group not found, recreating...")
                for stream_key in self.stream_keys:
                    try:
                        self.valkey.xgroup_create(
                            name=stream_key,
                            groupname=self.consumer_group,
                            id='0',
                            mkstream=True
                        )
                    except:
                        pass
            else:
                print(f"[{self.worker_id}] Error reading streams: {e}")
            return 0

    def claim_pending_messages(self):
        """Claim pending messages that timed out from other consumers (across all partitions)"""
        total_claimed = 0

        for stream_key in self.stream_keys:
            try:
                pending = self.valkey.xpending_range(
                    name=stream_key,
                    groupname=self.consumer_group,
                    min='-',
                    max='+',
                    count=10
                )

                if not pending:
                    continue

                claimed_ids = []
                batch_latencies = []

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
                                for msg_id, msg_data in result:
                                    created_at_str = msg_data.get(b'created_at', b'').decode('utf-8')
                                    if created_at_str:
                                        created_at = datetime.fromisoformat(created_at_str)
                                    else:
                                        created_at = datetime.now()

                                    self.simulate_processing()

                                    latency_ms = (datetime.now() - created_at).total_seconds() * 1000
                                    batch_latencies.append(latency_ms)
                                    claimed_ids.append(msg_id)
                                    self.jobs_processed += 1

                        except Exception as e:
                            print(f"[{self.worker_id}] Error claiming {message_id} on {stream_key}: {e}")
                            continue

                if claimed_ids:
                    try:
                        self.valkey.xack(stream_key, self.consumer_group, *claimed_ids)
                    except Exception as e:
                        print(f"[{self.worker_id}] Error acking claimed on {stream_key}: {e}")

                if batch_latencies:
                    self.metrics.record_jobs(batch_latencies)

                total_claimed += len(claimed_ids)

            except Exception as e:
                print(f"[{self.worker_id}] Error claiming on {stream_key}: {e}")
                continue

        return total_claimed

    def run(self):
        """Main worker loop"""
        print(f"[{self.worker_id}] Started (batch={self.batch_size}, partitions={len(self.stream_keys)})")

        claim_counter = 0

        try:
            while self.running.is_set():
                processed = self.process_messages()

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
    print(f"Poll interval: {BENCHMARK['valkey_worker_poll_interval_ms']} ms")
    print(f"Metrics output: {args.output}")
    print("-" * 50)

    # Metrics collector
    metrics = MetricsCollector(args.output)

    # Create workers
    workers = []
    threads = []

    for i in range(args.workers):
        worker = ValkeyWorker(i, metrics)
        thread = Thread(target=worker.run)
        thread.daemon = True
        thread.start()

        workers.append(worker)
        threads.append(thread)

    # Periodic metrics collection
    def collect_metrics():
        while True:
            time.sleep(BENCHMARK['metrics_interval'])
            metrics.save_metrics()

    metrics_thread = Thread(target=collect_metrics)
    metrics_thread.daemon = True
    metrics_thread.start()

    # Wait for interrupt
    def signal_handler(sig, frame):
        print("\nStopping workers...")
        for worker in workers:
            worker.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Wait for all threads
    for thread in threads:
        thread.join()


if __name__ == '__main__':
    main()
