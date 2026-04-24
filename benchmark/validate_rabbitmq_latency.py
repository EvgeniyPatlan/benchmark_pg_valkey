#!/usr/bin/env python3
"""
Baseline RabbitMQ round-trip latency validator.

Publishes N messages to a throwaway queue and measures publish->consume
time per message. Same shape as validate_valkey_latency / kafka so pre-
benchmark sanity checks are symmetric across backends.
"""
import argparse
import time
from datetime import datetime
import pika

from config import RABBITMQ_CONFIG


def _p(sorted_vals, pct):
    return sorted_vals[min(len(sorted_vals) - 1, int(len(sorted_vals) * pct))]


def validate(num_messages=1000, queue_name='bench_validation'):
    creds = pika.PlainCredentials(RABBITMQ_CONFIG['user'], RABBITMQ_CONFIG['password'])
    params = pika.ConnectionParameters(
        host=RABBITMQ_CONFIG['host'], port=RABBITMQ_CONFIG['port'],
        virtual_host=RABBITMQ_CONFIG['vhost'], credentials=creds,
    )
    conn = pika.BlockingConnection(params)
    ch = conn.channel()

    ch.queue_delete(queue=queue_name)
    ch.queue_declare(queue=queue_name, durable=False)

    # Produce all messages with current timestamp in a header.
    for i in range(num_messages):
        props = pika.BasicProperties(headers={'t': str(time.time())})
        ch.basic_publish(exchange='', routing_key=queue_name, body=b'v', properties=props)

    # Consume + measure.
    latencies_ms = []
    while len(latencies_ms) < num_messages:
        method, props, body = ch.basic_get(queue=queue_name, auto_ack=True)
        if method is None:
            break
        now = time.time()
        if props and props.headers and 't' in props.headers:
            sent_ts = float(props.headers['t'])
            latencies_ms.append((now - sent_ts) * 1000)

    ch.queue_delete(queue=queue_name)
    conn.close()

    latencies_ms.sort()
    print(f"\nRabbitMQ baseline latency ({len(latencies_ms)} samples):")
    print(f"  p50     : {_p(latencies_ms, 0.50):.2f} ms")
    print(f"  p95     : {_p(latencies_ms, 0.95):.2f} ms")
    print(f"  p99     : {_p(latencies_ms, 0.99):.2f} ms")
    print(f"  max     : {latencies_ms[-1]:.2f} ms")
    print("")
    print("Expected ranges on a healthy single-node install:")
    print("  p50 < 5ms; p95 < 10ms; p99 < 20ms")
    print("If p50 > 20ms: check disk IO, memory high-watermark, or queue type.")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--num-messages', type=int, default=1000)
    args = parser.parse_args()
    validate(num_messages=args.num_messages)


if __name__ == '__main__':
    main()
