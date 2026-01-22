#!/bin/bash
# Initialize Valkey Streams for benchmark

set -e

echo "Initializing Valkey Streams..."

# Test connection
valkey-cli ping || {
    echo "Error: Valkey is not running"
    exit 1
}

# Create consumer group for the benchmark
# Stream will be created automatically when first message is added
# We'll create the consumer group in the Python code

# Flush any existing data (optional - comment out if you want to preserve data)
# valkey-cli FLUSHDB

echo "Valkey Streams ready for benchmark"
echo ""
echo "Stream name: bench_queue"
echo "Consumer group: bench_workers"
echo ""
echo "Useful commands:"
echo "  valkey-cli XINFO STREAM bench_queue     # Stream info"
echo "  valkey-cli XINFO GROUPS bench_queue     # Consumer groups"
echo "  valkey-cli XLEN bench_queue             # Stream length"
echo "  valkey-cli XPENDING bench_queue bench_workers  # Pending messages"
