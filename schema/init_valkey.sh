#!/bin/bash
# Initialize Valkey Streams for benchmark
# Creates partitioned stream keys to avoid hot-shard antipattern

set -e

echo "Initializing Valkey Streams (partitioned)..."

# Test connection
valkey-cli ping || {
    echo "Error: Valkey is not running"
    exit 1
}

# Number of stream partitions (must match config.py)
NUM_PARTITIONS=${1:-8}
STREAM_PREFIX="bench_queue"
CONSUMER_GROUP="bench_workers"

echo "Creating $NUM_PARTITIONS stream partitions..."

for i in $(seq 0 $((NUM_PARTITIONS - 1))); do
    STREAM_KEY="${STREAM_PREFIX}:${i}"

    # Delete existing stream
    valkey-cli DEL "$STREAM_KEY" > /dev/null 2>&1

    # Create stream with consumer group
    # MKSTREAM creates the stream if it doesn't exist
    valkey-cli XGROUP CREATE "$STREAM_KEY" "$CONSUMER_GROUP" 0 MKSTREAM > /dev/null 2>&1 || true

    echo "  Created: $STREAM_KEY (group: $CONSUMER_GROUP)"
done

echo ""
echo "Valkey Streams ready for benchmark"
echo ""
echo "Stream partitions: ${NUM_PARTITIONS}"
echo "Stream prefix: ${STREAM_PREFIX}"
echo "Consumer group: ${CONSUMER_GROUP}"
echo ""
echo "Useful commands:"
echo "  # Check all partition lengths:"
echo "  for i in \$(seq 0 $((NUM_PARTITIONS - 1))); do echo \"${STREAM_PREFIX}:\$i: \$(valkey-cli XLEN ${STREAM_PREFIX}:\$i)\"; done"
echo ""
echo "  # Check consumer group info:"
echo "  valkey-cli XINFO GROUPS ${STREAM_PREFIX}:0"
echo ""
echo "  # Total messages across all partitions:"
echo "  total=0; for i in \$(seq 0 $((NUM_PARTITIONS - 1))); do n=\$(valkey-cli XLEN ${STREAM_PREFIX}:\$i); total=\$((total + n)); done; echo \"Total: \$total\""
