#!/bin/bash
# Install Kafka 3.9 in KRaft mode (no ZooKeeper), single broker.
# Matches the single-node fairness principle: fullscale deployment uses
# a cluster; this benchmark intentionally measures raw single-broker
# behavior so the comparison against PG/Valkey is apples-to-apples.

set -e

KAFKA_VERSION="${KAFKA_VERSION:-3.9.0}"
SCALA_VERSION="${SCALA_VERSION:-2.13}"
KAFKA_USER="${KAFKA_USER:-kafka}"
KAFKA_HOME="/opt/kafka"
KAFKA_DATA="/var/lib/kafka"
KAFKA_LOG="/var/log/kafka"

# Auto-size Kafka heap based on available RAM so the benchmark works on
# both beefy dev boxes and constrained VMs (prior hardcoded 4G crashed
# on <6GB VMs). User can override by exporting KAFKA_HEAP_MAX / _MIN.
TOTAL_RAM_MB=$(awk '/MemTotal/ {printf "%d", $2/1024}' /proc/meminfo)
if [ -z "$KAFKA_HEAP_MAX" ]; then
    if   [ "$TOTAL_RAM_MB" -ge 14336 ]; then KAFKA_HEAP_MAX="4G"; KAFKA_HEAP_MIN="4G"
    elif [ "$TOTAL_RAM_MB" -ge  6144 ]; then KAFKA_HEAP_MAX="2G"; KAFKA_HEAP_MIN="1G"
    else                                     KAFKA_HEAP_MAX="1G"; KAFKA_HEAP_MIN="512M"
    fi
fi
KAFKA_HEAP_MIN="${KAFKA_HEAP_MIN:-$KAFKA_HEAP_MAX}"
echo "Sizing Kafka heap: Xmx=${KAFKA_HEAP_MAX} Xms=${KAFKA_HEAP_MIN} (system RAM ${TOTAL_RAM_MB}MB)"

echo "Installing Kafka ${KAFKA_VERSION}..."

# Dependencies
sudo apt-get update -qq
sudo apt-get install -y -qq openjdk-17-jre-headless wget tar

# Fetch
cd /tmp
if [ ! -f "kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz" ]; then
    wget --quiet "https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz"
fi
sudo tar -xzf "kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz" -C /opt/
sudo rm -rf "$KAFKA_HOME"
sudo mv "/opt/kafka_${SCALA_VERSION}-${KAFKA_VERSION}" "$KAFKA_HOME"

# User + dirs
sudo useradd -r -s /bin/false "$KAFKA_USER" 2>/dev/null || true
sudo mkdir -p "$KAFKA_DATA" "$KAFKA_LOG"
sudo chown -R "$KAFKA_USER:$KAFKA_USER" "$KAFKA_HOME" "$KAFKA_DATA" "$KAFKA_LOG"

# KRaft single-broker config. This is the full monolithic config — broker
# and controller in the same process (process.roles=broker,controller),
# single-node quorum (controller.quorum.voters=1@localhost:9093).
sudo tee "$KAFKA_HOME/config/kraft/bench.properties" > /dev/null <<EOF
# Benchmark-specific KRaft config — single broker + controller
process.roles=broker,controller
node.id=1
controller.quorum.voters=1@localhost:9093

listeners=PLAINTEXT://:9092,CONTROLLER://:9093
advertised.listeners=PLAINTEXT://localhost:9092
controller.listener.names=CONTROLLER
inter.broker.listener.name=PLAINTEXT
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT

log.dirs=${KAFKA_DATA}/logs
num.network.threads=8
num.io.threads=8
socket.send.buffer.bytes=102400
socket.receive.buffer.bytes=102400
socket.request.max.bytes=104857600

# Default topic settings (overridden per-topic by init_kafka.sh)
num.partitions=8
default.replication.factor=1
min.insync.replicas=1
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1

# Tunables relevant to the benchmark
log.retention.hours=6
log.segment.bytes=1073741824
log.retention.check.interval.ms=60000

# Durability knobs — runtime CONFIG applies DURABILITY_MODE on top of these.
# These are the 'none' baseline (acks=1 producer-side, default flushing).
# 'matched' and 'strict' get set per-run via ALTER CONFIG or producer opts.
# Note: omitting log.flush.interval.ms lets the default apply (no timed
# flush). Setting it to 'null' is a parse error.
log.flush.interval.messages=9223372036854775807
EOF

# systemd unit
sudo tee /etc/systemd/system/kafka.service > /dev/null <<EOF
[Unit]
Description=Apache Kafka (KRaft, single node, benchmark)
After=network.target

[Service]
Type=simple
User=${KAFKA_USER}
Environment="KAFKA_HEAP_OPTS=-Xmx${KAFKA_HEAP_MAX} -Xms${KAFKA_HEAP_MIN}"
Environment="KAFKA_LOG_DIR=${KAFKA_LOG}"
# Format storage once if not already formatted
ExecStartPre=/bin/bash -c 'if [ ! -f ${KAFKA_DATA}/logs/meta.properties ]; then \
    ${KAFKA_HOME}/bin/kafka-storage.sh format -t \$(${KAFKA_HOME}/bin/kafka-storage.sh random-uuid) \
        -c ${KAFKA_HOME}/config/kraft/bench.properties; fi'
ExecStart=${KAFKA_HOME}/bin/kafka-server-start.sh ${KAFKA_HOME}/config/kraft/bench.properties
Restart=on-failure
LimitNOFILE=65535

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable kafka
sudo systemctl restart kafka

# Wait for Kafka to become steady-state ready. A single connection success
# isn't enough — KRaft can accept connections briefly during startup and
# then die on an internal config check. Require the systemd unit to be
# 'active' AND an API-versions call to succeed twice, 5 seconds apart.
echo "Waiting for Kafka to become steady..."
ready=0
for i in $(seq 1 40); do
    if systemctl is-active --quiet kafka && \
       "$KAFKA_HOME/bin/kafka-broker-api-versions.sh" --bootstrap-server localhost:9092 > /dev/null 2>&1; then
        sleep 5
        if systemctl is-active --quiet kafka && \
           "$KAFKA_HOME/bin/kafka-broker-api-versions.sh" --bootstrap-server localhost:9092 > /dev/null 2>&1; then
            ready=1
            echo "Kafka is ready."
            break
        fi
    fi
    sleep 2
done
if [ "$ready" -ne 1 ]; then
    echo "ERROR: Kafka did not reach steady state. systemctl status:"
    systemctl status kafka --no-pager | head -15
    exit 1
fi

# Expose Kafka admin scripts via /usr/local/bin/ so orchestration can call
# them without hardcoding $KAFKA_HOME. MUST be real wrapper scripts, not
# symlinks — the Kafka shell scripts resolve siblings via `$(dirname $0)`,
# and a symlink from /usr/local/bin/kafka-topics.sh makes dirname resolve
# to /usr/local/bin/ where kafka-run-class.sh doesn't exist.
for cmd in kafka-topics kafka-configs kafka-consumer-groups \
           kafka-broker-api-versions kafka-console-consumer \
           kafka-console-producer kafka-storage; do
    sudo bash -c "cat > /usr/local/bin/${cmd}.sh" <<EOF
#!/bin/bash
exec ${KAFKA_HOME}/bin/${cmd}.sh "\$@"
EOF
    sudo chmod +x "/usr/local/bin/${cmd}.sh"
done

echo "Kafka ${KAFKA_VERSION} installed and running on localhost:9092."
