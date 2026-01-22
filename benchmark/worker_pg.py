#!/usr/bin/env python3
"""
PostgreSQL Queue Worker
Processes jobs from PostgreSQL queue with metrics collection
"""
import argparse
import json
import time
import signal
import sys
import random
from datetime import datetime
from threading import Thread, Event
import psycopg2
from psycopg2.extras import RealDictCursor

from config import BENCHMARK, DB_CONFIG, QUEUE_TYPES


class MetricsCollector:
    def __init__(self, output_file):
        self.output_file = output_file
        self.jobs_processed = 0
        self.latencies = []
        self.start_time = time.time()
        self.lock = __import__('threading').Lock()
    
    def record_job(self, latency_ms):
        with self.lock:
            self.jobs_processed += 1
            self.latencies.append(latency_ms)
    
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


class PostgreSQLWorker:
    def __init__(self, worker_id, queue_type, metrics_collector):
        self.worker_id = f"worker_{worker_id}"
        self.queue_type = queue_type
        self.metrics = metrics_collector
        self.running = Event()
        self.running.set()
        
        self.conn = psycopg2.connect(**DB_CONFIG)
        self.conn.autocommit = True
        
        self.config = QUEUE_TYPES[queue_type]
        self.jobs_processed = 0
        
        # For partitioned queues, assign worker to specific partitions
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
        """Simulate job processing"""
        time.sleep(BENCHMARK['job_processing_time_ms'] / 1000.0)
    
    def process_job_basic(self):
        """Process job using basic SKIP LOCKED pattern"""
        cursor = self.conn.cursor(cursor_factory=RealDictCursor)
        
        try:
            # Get next job
            cursor.execute(
                f"SELECT * FROM {self.config['get_function']}(%s)",
                (self.worker_id,)
            )
            job = cursor.fetchone()
            
            if not job:
                time.sleep(BENCHMARK['worker_poll_interval_ms'] / 1000.0)
                return False
            
            job_id = job['job_id']
            created_at = job['job_created_at']
            
            # Simulate processing
            self.simulate_processing()
            
            # Mark as completed
            cursor.execute(
                f"SELECT {self.config['complete_function']}(%s)",
                (job_id,)
            )
            
            # Record metrics
            latency_ms = (datetime.now() - created_at).total_seconds() * 1000
            self.metrics.record_job(latency_ms)
            self.jobs_processed += 1
            
            return True
            
        except Exception as e:
            print(f"[{self.worker_id}] Error processing job: {e}")
            return False
    
    def process_job_delete_returning(self):
        """Process job using DELETE RETURNING pattern"""
        cursor = self.conn.cursor(cursor_factory=RealDictCursor)
        
        try:
            # Get next job (atomically deleted from queue)
            cursor.execute(
                f"SELECT * FROM {self.config['get_function']}(%s)",
                (self.worker_id,)
            )
            job = cursor.fetchone()
            
            if not job:
                time.sleep(BENCHMARK['worker_poll_interval_ms'] / 1000.0)
                return False
            
            job_id = job['job_id']
            created_at = job['job_created_at']
            payload = job['job_payload']
            priority = job['job_priority']
            
            # Simulate processing
            self.simulate_processing()
            
            # Mark as completed (insert into archive)
            cursor.execute(
                f"SELECT {self.config['complete_function']}(%s, %s, %s, %s, %s)",
                (job_id, json.dumps(payload), priority, created_at, self.worker_id)
            )
            
            # Record metrics
            latency_ms = (datetime.now() - created_at).total_seconds() * 1000
            self.metrics.record_job(latency_ms)
            self.jobs_processed += 1
            
            return True
            
        except Exception as e:
            print(f"[{self.worker_id}] Error processing job: {e}")
            return False
    
    def process_job_partitioned(self):
        """Process job from partitioned queue"""
        cursor = self.conn.cursor(cursor_factory=RealDictCursor)
        
        # Try each assigned partition
        for partition_key in self.partitions:
            try:
                cursor.execute(
                    f"SELECT * FROM {self.config['get_function']}(%s, %s)",
                    (partition_key, self.worker_id)
                )
                job = cursor.fetchone()
                
                if not job:
                    continue
                
                job_id = job['job_id']
                created_at = job['job_created_at']
                
                # Simulate processing
                self.simulate_processing()
                
                # Mark as completed
                cursor.execute(
                    f"SELECT {self.config['complete_function']}(%s, %s)",
                    (partition_key, job_id)
                )
                
                # Record metrics
                latency_ms = (datetime.now() - created_at).total_seconds() * 1000
                self.metrics.record_job(latency_ms)
                self.jobs_processed += 1
                
                return True
                
            except Exception as e:
                print(f"[{self.worker_id}] Error processing job from partition {partition_key}: {e}")
                continue
        
        # No jobs found in any partition
        time.sleep(BENCHMARK['worker_poll_interval_ms'] / 1000.0)
        return False
    
    def run(self):
        """Main worker loop"""
        print(f"[{self.worker_id}] Started")
        
        try:
            while self.running.is_set():
                if self.queue_type == 'delete_returning':
                    self.process_job_delete_returning()
                elif self.queue_type == 'partitioned':
                    self.process_job_partitioned()
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
    parser.add_argument('--queue-type', choices=['skip_locked', 'delete_returning', 'partitioned'],
                       required=True, help='Queue type')
    parser.add_argument('--workers', type=int, default=BENCHMARK['num_workers'],
                       help='Number of worker threads')
    parser.add_argument('--output', default='metrics_pg.jsonl',
                       help='Metrics output file')
    
    args = parser.parse_args()
    
    print(f"Starting {args.workers} PostgreSQL workers ({args.queue_type})")
    print(f"Metrics output: {args.output}")
    print("-" * 50)
    
    # Metrics collector
    metrics = MetricsCollector(args.output)
    
    # Create workers
    workers = []
    threads = []
    
    for i in range(args.workers):
        worker = PostgreSQLWorker(i, args.queue_type, metrics)
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
