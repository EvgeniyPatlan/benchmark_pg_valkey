#!/usr/bin/env python3
"""
Job Producer
Generates jobs at a specified rate for benchmarking
"""
import argparse
import json
import time
import sys
import random
import string
from datetime import datetime
import psycopg2
from psycopg2.extras import execute_values

# Make valkey import optional
try:
    import valkey
except ImportError:
    valkey = None

from config import BENCHMARK, DB_CONFIG, VALKEY_CONFIG, QUEUE_TYPES


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
            if valkey is None:
                print("ERROR: valkey module not installed!")
                print("Install with: pip3 install valkey")
                sys.exit(1)
            
            self.valkey = valkey.Valkey(
                host=VALKEY_CONFIG['host'],
                port=VALKEY_CONFIG['port'],
                decode_responses=False
            )
            self.stream_name = VALKEY_CONFIG['stream_name']
            
            # Create consumer group if it doesn't exist
            try:
                self.valkey.xgroup_create(
                    name=self.stream_name,
                    groupname=VALKEY_CONFIG['consumer_group'],
                    id='0',
                    mkstream=True
                )
                print(f"Created consumer group: {VALKEY_CONFIG['consumer_group']}")
            except Exception as e:
                if 'BUSYGROUP' not in str(e):
                    print(f"Warning creating consumer group: {e}")
    
    def generate_payload(self):
        """Generate random payload of specified size"""
        size = BENCHMARK['job_size_bytes']
        data = {
            'id': self.jobs_produced,
            'timestamp': datetime.now().isoformat(),
            'data': ''.join(random.choices(string.ascii_letters + string.digits, k=size - 100))
        }
        return json.dumps(data)
    
    def produce_pg_batch(self, batch_size):
        """Produce a batch of jobs to PostgreSQL"""
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
                execute_values(
                    cursor,
                    f"INSERT INTO {self.table} (partition_key, payload, priority) VALUES %s",
                    jobs
                )
            else:
                execute_values(
                    cursor,
                    f"INSERT INTO {self.table} (payload, priority) VALUES %s",
                    jobs
                )
            
            self.conn.commit()
            self.jobs_produced += batch_size
            return batch_size
        except Exception as e:
            self.conn.rollback()
            print(f"Error producing batch: {e}")
            return 0
    
    def produce_valkey_batch(self, batch_size):
        """Produce a batch of jobs to Valkey Streams"""
        try:
            pipeline = self.valkey.pipeline()
            
            for _ in range(batch_size):
                payload = self.generate_payload()
                priority = random.randint(0, 10)
                
                pipeline.xadd(
                    self.stream_name,
                    {
                        b'payload': payload.encode('utf-8'),
                        b'priority': str(priority).encode('utf-8'),
                        b'created_at': datetime.now().isoformat().encode('utf-8')
                    }
                )
            
            pipeline.execute()
            self.jobs_produced += batch_size
            return batch_size
        except Exception as e:
            print(f"Error producing batch: {e}")
            return 0
    
    def run(self):
        """Run the producer"""
        print(f"Starting producer: {self.backend}/{self.queue_type}")
        print(f"Rate: {self.rate} jobs/sec")
        print(f"Duration: {self.duration} seconds")
        print(f"Total jobs: {self.total_jobs}")
        print("-" * 50)
        
        self.start_time = time.time()
        batch_size = max(1, self.rate // 100)  # 100 batches per second
        interval = batch_size / self.rate  # seconds between batches
        
        next_batch_time = self.start_time
        
        try:
            while self.jobs_produced < self.total_jobs:
                # Produce batch
                if self.backend == 'pg':
                    produced = self.produce_pg_batch(batch_size)
                else:
                    produced = self.produce_valkey_batch(batch_size)
                
                # Rate limiting
                next_batch_time += interval
                sleep_time = next_batch_time - time.time()
                if sleep_time > 0:
                    time.sleep(sleep_time)
                
                # Progress update
                elapsed = time.time() - self.start_time
                if self.jobs_produced % (self.rate * 10) == 0:  # Every 10 seconds
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
            else:
                self.valkey.close()


def main():
    parser = argparse.ArgumentParser(description='Benchmark job producer')
    parser.add_argument('--backend', choices=['pg', 'valkey'], required=True,
                       help='Backend type: pg (PostgreSQL) or valkey')
    parser.add_argument('--queue-type', choices=['skip_locked', 'delete_returning', 'partitioned'],
                       default='skip_locked',
                       help='PostgreSQL queue type (ignored for valkey)')
    parser.add_argument('--rate', type=int, default=BENCHMARK['production_rate'],
                       help='Production rate (jobs/sec)')
    parser.add_argument('--duration', type=int, default=BENCHMARK['production_duration'],
                       help='Production duration (seconds)')
    
    args = parser.parse_args()
    
    producer = Producer(
        backend=args.backend,
        queue_type=args.queue_type,
        rate=args.rate,
        duration=args.duration
    )
    
    producer.run()


if __name__ == '__main__':
    main()
