#!/usr/bin/env python3
"""
Valkey Streams Worker - CORRECTED WITH PROPER BATCHING
Processes jobs from Valkey Streams with batch acknowledgments
"""
import argparse
import json
import time
import signal
import sys
from datetime import datetime
from threading import Thread, Event
import valkey

from config import BENCHMARK, VALKEY_CONFIG


class MetricsCollector:
    def __init__(self, output_file):
        self.output_file = output_file
        self.jobs_processed = 0
        self.latencies = []
        self.start_time = time.time()
        self.lock = __import__('threading').Lock()
    
    def record_jobs(self, latencies_list):
        """Record multiple jobs at once (for batch processing)"""
        with self.lock:
            self.jobs_processed += len(latencies_list)
            self.latencies.extend(latencies_list)
    
    def get_metrics(self):
        with self.lock:
            if not self.latencies:
                return None
            
            sorted_latencies = sorted(self.latencies)
            n = len(sorted_latencies)
            
            metrics = {
                'timestamp': datetime.now().isoformat(),
                'elapsed': time.time() - self.start_time,
                'jobs_processed': self.jobs_processed,
                'throughput': self.jobs_processed / (time.time() - self.start_time),
                'latency_p50': sorted_latencies[int(n * 0.50)] if n > 0 else 0,
                'latency_p95': sorted_latencies[int(n * 0.95)] if n > 0 else 0,
                'latency_p99': sorted_latencies[int(n * 0.99)] if n > 0 else 0,
                'latency_min': sorted_latencies[0] if n > 0 else 0,
                'latency_max': sorted_latencies[-1] if n > 0 else 0,
                'latency_avg': sum(sorted_latencies) / n if n > 0 else 0,
            }
            
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
        
        self.stream_name = VALKEY_CONFIG['stream_name']
        self.consumer_group = VALKEY_CONFIG['consumer_group']
        self.jobs_processed = 0
        
        # Use Valkey-specific batch size
        self.batch_size = BENCHMARK['valkey_worker_batch_size']
        self.poll_interval = BENCHMARK['valkey_worker_poll_interval_ms']
        
        # Ensure consumer group exists
        try:
            self.valkey.xgroup_create(
                name=self.stream_name,
                groupname=self.consumer_group,
                id='0',
                mkstream=True
            )
        except Exception as e:
            if 'BUSYGROUP' not in str(e):
                print(f"[{self.worker_id}] Warning: {e}")
    
    def simulate_processing(self):
        """Simulate job processing"""
        time.sleep(BENCHMARK['job_processing_time_ms'] / 1000.0)
    
    def process_messages(self):
        """Process messages from Valkey Stream - WITH BATCH ACKNOWLEDGMENT"""
        try:
            # Read BATCH of messages from stream
            messages = self.valkey.xreadgroup(
                groupname=self.consumer_group,
                consumername=self.worker_id,
                streams={self.stream_name: '>'},
                count=self.batch_size,  # Fetch multiple messages at once!
                block=self.poll_interval
            )
            
            if not messages:
                return 0
            
            # Collect message IDs and latencies for batch acknowledgment
            message_ids_to_ack = []
            batch_latencies = []
            
            # Process each message
            for stream_name, stream_messages in messages:
                for message_id, message_data in stream_messages:
                    try:
                        # Extract message data
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
                        
                        # Add to acknowledgment batch
                        message_ids_to_ack.append(message_id)
                        self.jobs_processed += 1
                        
                    except Exception as e:
                        print(f"[{self.worker_id}] Error processing message {message_id}: {e}")
                        # Don't add failed messages to ack batch
                        continue
            
            # BATCH ACKNOWLEDGE all successfully processed messages in ONE call
            if message_ids_to_ack:
                try:
                    self.valkey.xack(self.stream_name, self.consumer_group, *message_ids_to_ack)
                except Exception as e:
                    print(f"[{self.worker_id}] Error acknowledging batch: {e}")
            
            # Record metrics for entire batch
            if batch_latencies:
                self.metrics.record_jobs(batch_latencies)
            
            return len(message_ids_to_ack)
            
        except Exception as e:
            if 'NOGROUP' in str(e):
                print(f"[{self.worker_id}] Consumer group not found, recreating...")
                try:
                    self.valkey.xgroup_create(
                        name=self.stream_name,
                        groupname=self.consumer_group,
                        id='0',
                        mkstream=True
                    )
                except:
                    pass
            else:
                print(f"[{self.worker_id}] Error reading stream: {e}")
            return 0
    
    def claim_pending_messages(self):
        """Claim pending messages that timed out from other consumers"""
        try:
            # Get pending messages older than 5 seconds
            pending = self.valkey.xpending_range(
                name=self.stream_name,
                groupname=self.consumer_group,
                min='-',
                max='+',
                count=10
            )
            
            if not pending:
                return 0
            
            claimed_ids = []
            batch_latencies = []
            
            for msg in pending:
                message_id = msg['message_id']
                idle_time = msg['time_since_delivered']
                
                # Claim messages idle for more than 5 seconds
                if idle_time > 5000:
                    try:
                        result = self.valkey.xclaim(
                            name=self.stream_name,
                            groupname=self.consumer_group,
                            consumername=self.worker_id,
                            min_idle_time=5000,
                            message_ids=[message_id]
                        )
                        
                        if result:
                            # Process claimed messages
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
                        print(f"[{self.worker_id}] Error claiming message {message_id}: {e}")
                        continue
            
            # Batch acknowledge claimed messages
            if claimed_ids:
                try:
                    self.valkey.xack(self.stream_name, self.consumer_group, *claimed_ids)
                except Exception as e:
                    print(f"[{self.worker_id}] Error acknowledging claimed batch: {e}")
            
            # Record metrics
            if batch_latencies:
                self.metrics.record_jobs(batch_latencies)
            
            return len(claimed_ids)
            
        except Exception as e:
            print(f"[{self.worker_id}] Error claiming pending messages: {e}")
            return 0
    
    def run(self):
        """Main worker loop"""
        print(f"[{self.worker_id}] Started (batch_size={self.batch_size})")
        
        claim_counter = 0
        
        try:
            while self.running.is_set():
                # Process new messages in batches
                processed = self.process_messages()
                
                # Periodically claim pending messages (every 10 iterations)
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
    parser = argparse.ArgumentParser(description='Valkey Streams worker (with proper batching)')
    parser.add_argument('--workers', type=int, default=BENCHMARK['num_workers'],
                       help='Number of worker threads')
    parser.add_argument('--output', default='metrics_valkey.jsonl',
                       help='Metrics output file')
    parser.add_argument('--batch-size', type=int, default=None,
                       help='Override batch size from config')
    
    args = parser.parse_args()
    
    # Allow command-line override
    if args.batch_size:
        BENCHMARK['valkey_worker_batch_size'] = args.batch_size
    
    print(f"Starting {args.workers} Valkey workers")
    print(f"Stream: {VALKEY_CONFIG['stream_name']}")
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