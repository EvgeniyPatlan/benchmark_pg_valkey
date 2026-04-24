#!/bin/bash
# Declare the two benchmark queues on RabbitMQ:
#   bench_queue_classic : classic queue, durable
#   bench_queue_quorum  : quorum queue (Raft-backed)
#
# Idempotent — safe to run between benchmark runs to reset state.

set -e

RABBITMQ_USER="${RABBITMQ_USER:-bench_user}"
RABBITMQ_PASS="${RABBITMQ_PASS:-bench_pass}"
RABBITMQ_HOST="${RABBITMQ_HOST:-localhost}"
RABBITMQ_MGMT_PORT="${RABBITMQ_MGMT_PORT:-15672}"

ADMIN=(rabbitmqadmin -H "$RABBITMQ_HOST" -P "$RABBITMQ_MGMT_PORT" \
    -u "$RABBITMQ_USER" -p "$RABBITMQ_PASS")

echo "Initializing RabbitMQ benchmark queues..."

# Test connection
"${ADMIN[@]}" list vhosts > /dev/null 2>&1 || {
    echo "ERROR: cannot reach RabbitMQ management at $RABBITMQ_HOST:$RABBITMQ_MGMT_PORT"
    exit 1
}

# Purge + redeclare both queues. rabbitmqadmin delete is idempotent-ish
# (fails silently if not present).
for Q in bench_queue_classic bench_queue_quorum; do
    "${ADMIN[@]}" delete queue name="$Q" 2>/dev/null || true
done
sleep 1

# Classic queue — traditional default, mnesia-backed.
"${ADMIN[@]}" declare queue \
    name=bench_queue_classic \
    durable=true

# Quorum queue — Raft-backed. Note: on a single node it operates but
# durability guarantee is degenerate (no replication). Caveat recorded in
# environment.txt per run.
"${ADMIN[@]}" declare queue \
    name=bench_queue_quorum \
    durable=true \
    arguments='{"x-queue-type":"quorum"}'

echo ""
echo "RabbitMQ queues ready:"
"${ADMIN[@]}" list queues name type durable | grep bench_queue
