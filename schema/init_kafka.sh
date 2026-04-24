#!/bin/bash
# Kafka init — wrapper around the Python/librdkafka initializer.
# See schema/init_kafka.py for rationale (JVM cold-start + hang issues
# on single-node KRaft make kafka-topics.sh unreliable for automation).

set -e
exec python3 "$(dirname "$0")/init_kafka.py" "$@"
