# Multi-Node Kafka (KRaft) — Install & Failover Guide

> **Purpose:** Deploy a 3-broker Kafka KRaft cluster on VM `10.70.16.242` using Docker Compose.
> **Topology:** 3 combined broker+controller nodes (no ZooKeeper)
> **Replication:** Factor 3, Min ISR 2
> **Last Updated:** 2026-10-19

---

## 1. Overview

A **multi-node Kafka cluster** splits every topic's partitions across multiple brokers and keeps **3 copies** (replicas) of each partition. If one broker fails:

- The **controller quorum** (2 of 3 nodes) elects a new leader for affected partitions
- Producers and consumers **auto-reconnect** to the new leader
- **No data is lost** and **no downtime** occurs (as long as `min.insync.replicas` is respected)

### Single-node vs Multi-node

| Aspect | Single-node | **Multi-node (3)** |
| :--- | :---: | :---: |
| Brokers | 1 | 3 |
| Replication factor | max 1 | 3 |
| Zero-downtime broker failure | No | Yes |
| Data loss on disk failure | yes | no (replicated) |
| RAM | ~512Mi | ~1.5Gi |
| Production-ready | No | Yes |

**This guide is for:** learning HA Kafka, running microservices POC with fault tolerance, and preparing for production-like patterns.

---

## 2. Architecture

```
┌──────────────────────────────────────────────────────────────┐
│               VM x.x.x.242 (host network)                  │
│                                                              │
│   ┌───────────────┐   ┌───────────────┐   ┌───────────────┐  │
│   │   kafka-1     │   │   kafka-2     │   │   kafka-3     │  │
│   │  node.id=1    │   │  node.id=2    │   │  node.id=3    │  │
│   │               │   │               │   │               │  │
│   │ broker +      │   │ broker +      │   │ broker +      │  │
│   │ controller    │   │ controller    │   │ controller    │  │
│   │               │   │               │   │               │  │
│   │ EXTERNAL:9092 │   │ EXTERNAL:9095 │   │ EXTERNAL:9097 │  │
│   │ INTERNAL:19092│   │ INTERNAL:19093│   │ INTERNAL:19094│  │
│   │ CTRL:9093     │   │ CTRL:9094     │   │ CTRL:9096     │  │
│   └───────┬───────┘   └───────┬───────┘   └───────┬───────┘  │
│           │                   │                   │           │
│           └───────────────────┼───────────────────┘           │
│                               │                               │
│              KRaft quorum: 2 of 3 must agree                 │
└───────────────────────────────┬───────────────────────────────┘
                                │
                                ▼
                    Spring Boot services (external)
```

### Port map

| Broker | External | Internal | Controller |
| :--- | :---: | :---: | :---: |
| kafka-1 | 9092 | 19092 | 9093 |
| kafka-2 | 9095 | 19093 | 9094 |
| kafka-3 | 9097 | 19094 | 9096 |

---

## 3. Prerequisites

### On VM `10.70.16.242`

```bash
# Docker
docker --version          # ≥ 20.10
docker compose version    # ≥ 2.0

# Firewall (optional, if UFW enabled)
sudo ufw status
```

### If Docker not installed

```bash
sudo apt install -y docker.io
sudo groupadd --force docker
sudo usermod -aG docker $USER
newgrp docker
docker compose version


If not 
# Check
ARCH=$(uname -m)
echo "Arch: $ARCH"
if x86_64:
bash
sudo curl -SL "https://github.com/docker/compose/releases/latest/download/docker-compose-linux-x86_64" \
  -o /usr/libexec/docker/cli-plugins/docker-compose


sudo chmod +x /usr/libexec/docker/cli-plugins/docker-compose
docker compose version


sudo systemctl enable docker
sudo systemctl start docker
sudo systemctl status docker --no-pager

```

### Open ports (if firewall active)

```bash
sudo ufw allow 9092/tcp
sudo ufw allow 9095/tcp
sudo ufw allow 9097/tcp
sudo ufw allow 9093/tcp
sudo ufw allow 9094/tcp
sudo ufw allow 9096/tcp
sudo ufw allow 19092:19094/tcp
sudo ufw reload
```

---

## 4. Setup

### 4.1 Create project directory

```bash
mkdir -p ~/kafka-cluster && cd ~/kafka-cluster
```

### 4.2 Create data directories for each broker

```bash
mkdir -p kafka-1-data kafka-2-data kafka-3-data
```

### 4.3 Set ownership to UID 1000 (Kafka appuser)

```bash
sudo chown -R 1000:1000 kafka-1-data kafka-2-data kafka-3-data
```

### 4.4 Verify ownership

```bash
ls -ln
```

**Expected:**
```
drwxr-xr-x 2 1000 1000 4096 ... kafka-1-data
drwxr-xr-x 2 1000 1000 4096 ... kafka-2-data
drwxr-xr-x 2 1000 1000 4096 ... kafka-3-data
                ↑    ↑
           Must be 1000 1000
```

> ** Critical:** If ownership is `0 0` (root), Kafka containers will fail with `AccessDeniedException`. Always run `chown` after creating directories.


### 4.5 — Generate CLUSTER_ID

Every KRaft cluster needs a unique 22-character base64 identifier. This ID must be **identical on all 3 nodes**.

### 4.6 Generate the ID

```bash
CLUSTER_ID=$(docker run --rm apache/kafka:3.9.0 \
  /opt/kafka/bin/kafka-storage.sh random-uuid)
echo "CLUSTER_ID: $CLUSTER_ID"
```

**Example output:**
```
CLUSTER_ID: MkU3OEVBNTcwNTJENDM2Qk
```

### 4.7 Save it to a file

```bash
echo "$CLUSTER_ID" > ~/kafka-cluster/.cluster-id
cat ~/kafka-cluster/.cluster-id
```

### 4.8 Verify

```bash
# Should be exactly 22 characters
wc -c ~/kafka-cluster/.cluster-id
# Expected: 23 (22 chars + newline)
```

---
## Create docker-compose.yml

### 4.9 Write the compose file

This writes the file and injects the `CLUSTER_ID` automatically.

```bash
cd ~/kafka-cluster

cat > docker-compose.yml << YAML
services:
  kafka-1:
    image: apache/kafka:3.9.0
    container_name: kafka-1
    hostname: kafka-1
    network_mode: host
    restart: unless-stopped
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: CONTROLLER://:9093,INTERNAL://:19092,EXTERNAL://:9092
      KAFKA_ADVERTISED_LISTENERS: INTERNAL://kafka-1:19092,EXTERNAL://10.70.16.242:9092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka-1:9093,2@kafka-2:9094,3@kafka-3:9096
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_INTER_BROKER_LISTENER_NAME: INTERNAL
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 3
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 3
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 2
      KAFKA_DEFAULT_REPLICATION_FACTOR: 3
      KAFKA_MIN_INSYNC_REPLICAS: 2
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
      KAFKA_LOG_DIRS: /tmp/kraft-combined-logs
      CLUSTER_ID: '${CLUSTER_ID}'
    volumes:
      - ./kafka-1-data:/tmp/kraft-combined-logs
    healthcheck:
      test: ["CMD-SHELL", "/opt/kafka/bin/kafka-cluster.sh cluster-id --bootstrap-server localhost:9092 || exit 1"]
      interval: 10s
      timeout: 10s
      retries: 10
      start_period: 30s

  kafka-2:
    image: apache/kafka:3.9.0
    container_name: kafka-2
    hostname: kafka-2
    network_mode: host
    restart: unless-stopped
    environment:
      KAFKA_NODE_ID: 2
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: CONTROLLER://:9094,INTERNAL://:19093,EXTERNAL://:9095
      KAFKA_ADVERTISED_LISTENERS: INTERNAL://kafka-2:19093,EXTERNAL://10.70.16.242:9095
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka-1:9093,2@kafka-2:9094,3@kafka-3:9096
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_INTER_BROKER_LISTENER_NAME: INTERNAL
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 3
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 3
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 2
      KAFKA_DEFAULT_REPLICATION_FACTOR: 3
      KAFKA_MIN_INSYNC_REPLICAS: 2
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
      KAFKA_LOG_DIRS: /tmp/kraft-combined-logs
      CLUSTER_ID: '${CLUSTER_ID}'
    volumes:
      - ./kafka-2-data:/tmp/kraft-combined-logs
    healthcheck:
      test: ["CMD-SHELL", "/opt/kafka/bin/kafka-cluster.sh cluster-id --bootstrap-server localhost:9095 || exit 1"]
      interval: 10s
      timeout: 10s
      retries: 10
      start_period: 30s

  kafka-3:
    image: apache/kafka:3.9.0
    container_name: kafka-3
    hostname: kafka-3
    network_mode: host
    restart: unless-stopped
    environment:
      KAFKA_NODE_ID: 3
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: CONTROLLER://:9096,INTERNAL://:19094,EXTERNAL://:9097
      KAFKA_ADVERTISED_LISTENERS: INTERNAL://kafka-3:19094,EXTERNAL://10.70.16.242:9097
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka-1:9093,2@kafka-2:9094,3@kafka-3:9096
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_INTER_BROKER_LISTENER_NAME: INTERNAL
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 3
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 3
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 2
      KAFKA_DEFAULT_REPLICATION_FACTOR: 3
      KAFKA_MIN_INSYNC_REPLICAS: 2
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
      KAFKA_LOG_DIRS: /tmp/kraft-combined-logs
      CLUSTER_ID: '${CLUSTER_ID}'
    volumes:
      - ./kafka-3-data:/tmp/kraft-combined-logs
    healthcheck:
      test: ["CMD-SHELL", "/opt/kafka/bin/kafka-cluster.sh cluster-id --bootstrap-server localhost:9097 || exit 1"]
      interval: 10s
      timeout: 10s
      retries: 10
      start_period: 30s
YAML

echo "Done docker-compose.yml written"
```

### 4.10 Inject CLUSTER_ID from the file

```bash
CLUSTER_ID=$(cat ~/kafka-cluster/.cluster-id)
sed -i "s|CLUSTER_ID: '\${CLUSTER_ID}'|CLUSTER_ID: '${CLUSTER_ID}'|g" ~/kafka-cluster/docker-compose.yml
```

### 4.11 Verify CLUSTER_ID is present in all 3 services

```bash
grep CLUSTER_ID ~/kafka-cluster/docker-compose.yml
```

**Expected:**
```
      CLUSTER_ID: 'MkU3OEVBNTcwNTJENDM2Qk'
      CLUSTER_ID: 'MkU3OEVBNTcwNTJENDM2Qk'
      CLUSTER_ID: 'MkU3OEVBNTcwNTJENDM2Qk'
```

> All 3 must be **identical**. If not, re-run step 8.2.

### 4.12 Adjust the advertised IP (if different from 10.70.16.242)

If your VM IP is different, update it in the compose file:

```bash
# Replace with your VM IP
YOUR_IP=10.70.16.242

sed -i "s|10.70.16.242|${YOUR_IP}|g" ~/kafka-cluster/docker-compose.yml

# Verify
grep "EXTERNAL://" ~/kafka-cluster/docker-compose.yml
```

**Expected:**
```
INTERNAL://kafka-1:19092,EXTERNAL://10.70.16.242:9092
INTERNAL://kafka-2:19093,EXTERNAL://10.70.16.242:9095
INTERNAL://kafka-3:19094,EXTERNAL://10.70.16.242:9097
```
---

### 4.13 Key configuration explained

| Variable | Value | Purpose |
| :--- | :--- | :--- |
| `KAFKA_PROCESS_ROLES` | `broker,controller` | Combined mode — each node handles both roles |
| `KAFKA_CONTROLLER_QUORUM_VOTERS` | `1@kafka-1:9093,...` | Cluster membership (all 3 nodes) |
| `KAFKA_LISTENERS` | Three listeners | Separate ports for controller/internal/external |
| `KAFKA_ADVERTISED_LISTENERS` | `EXTERNAL://10.70.16.242:9092` | **Must be the IP external clients reach** |
| `*_REPLICATION_FACTOR` | `3` | Every partition gets 3 copies |
| `KAFKA_MIN_INSYNC_REPLICAS` | `2` | Writes need 2 brokers to ack — prevents data loss |
| `CLUSTER_ID` | 22-char base64 | Must be identical on all 3 nodes |




##  Start the Cluster

### Step-1 Start

```bash
cd ~/kafka-cluster
docker compose up -d
```

**Expected:**
```
✔ Container kafka-1   Started
✔ Container kafka-2   Started
✔ Container kafka-3   Started
```

### Step-2 Wait for healthchecks

```bash
sleep 30
```

### Step-3 Verify all healthy

```bash
docker compose ps
```

**Expected:**
```
NAME      IMAGE                COMMAND                  STATUS
kafka-1   apache/kafka:3.9.0   "/__cacert_entrypoin…"   Up 30 seconds (healthy)
kafka-2   apache/kafka:3.9.0   "/__cacert_entrypoin…"   Up 30 seconds (healthy)
kafka-3   apache/kafka:3.9.0   "/__cacert_entrypoin…"   Up 30 seconds (healthy)
```

> **If any is `(unhealthy)` or `restarting`, see §15 Troubleshooting before continuing.**

### Step-4 Check logs

```bash
docker compose logs kafka-1 --tail=20
```

**Expected (tail):**
```
[KafkaRaftServer nodeId=1] Kafka Server started (kafka.version=3.9.0)
hostname: kafka-1: Try again
```

---

STATUS: Up About a minute (health: starting)   ← Still now fully not ready
### Problem
The /etc/hosts file is a local DNS lookup table on Linux. It maps hostnames to IP addresses before any DNS server is queried. For your Kafka cluster with network_mode: host, this is mandatory because the broker hostnames (kafka-1, kafka-2, kafka-3) don't exist in any DNS server.

### Solution
```
echo "127.0.0.1 kafka-1 kafka-2 kafka-3" | sudo tee -a /etc/hosts
```
### Verify It Worked
### 1. Check the file
```
grep kafka /etc/hosts
```
Expected:

127.0.0.1 kafka-1 kafka-2 kafka-3

## 5. Verification

### 5.1 Cluster is alive

```bash
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --list
```

**Expected:** empty output (no error). This proves the cluster accepts metadata requests.

### 5.2 Create a 3-partition, 3-replica topic

```bash
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --topic my-topic \
  --partitions 3 \
  --replication-factor 3
```

### 5.3 Describe the topic

```bash
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe --topic my-topic
```

**Expected output (example):**

```
Topic: my-topic  TopicId: xxx  PartitionCount: 3  ReplicationFactor: 3  Configs:
  Topic: my-topic  Partition: 0  Leader: 1  Replicas: 1,2,3  Isr: 1,2,3
  Topic: my-topic  Partition: 1  Leader: 2  Replicas: 2,3,1  Isr: 2,3,1
  Topic: my-topic  Partition: 2  Leader: 3  Replicas: 3,1,2  Isr: 3,1,2
```

**Interpretation:**
- `Replicas: 1,2,3` — partition lives on all 3 brokers
- `Isr: 1,2,3` — all 3 are **in-sync** (healthy)
- `Leader: 1` — broker 1 is currently handling writes for partition 0

### 5.4 Produce & consume smoke test

**Terminal 1 — producer:**

```bash
docker exec -it kafka-1 /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic my-topic
```

Type a few messages, press `Ctrl+C`.

**Terminal 2 — consumer:**

```bash
docker exec -it kafka-3 /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic my-topic \
  --from-beginning
```

You should see the messages. This proves cross-broker replication works (produced on kafka-1, consumed from kafka-3).

---

## 6. Failover Tests

These tests **prove** the cluster is truly HA. Run them in order.

### 6.1 Test A — Graceful broker shutdown (planned maintenance)

**Simulates:** taking a broker offline for upgrade / patching.

**Setup:** open two terminals side-by-side.

**Terminal 1 — continuous consumer:**

```bash
docker exec -it kafka-1 /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092,localhost:9095,localhost:9097 \
  --topic my-topic \
  --from-beginning
```

Keep this running.

**Terminal 2 — producer loop:**

```bash
for i in $(seq 1 60); do
  echo "message-$i-$(date +%s)"
  sleep 1
done | docker exec -i kafka-1 /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092,localhost:9095,localhost:9097 \
  --topic my-topic
```

**While both are running, kill one broker:**

```bash
# In a third terminal
docker stop kafka-2
```

**Observe:**
- Producer → brief pause (~2–5 sec), then continues producing
- Consumer → brief pause, then continues consuming
- **No messages are lost** — the producer's next send goes to kafka-1 or kafka-3
- Only `min.insync.replicas=2` condition stays satisfied (2 of 3 brokers alive)

**Verify from a fourth terminal:**

```bash
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe --topic my-topic
```

**Expected:**
```
Partition: 0  Leader: 1  Replicas: 1,2,3  Isr: 1,3        ← kafka-2 removed from ISR
Partition: 1  Leader: 3  Replicas: 2,3,1  Isr: 3,1        ← leader changed from 2 → 3
Partition: 2  Leader: 3  Replicas: 3,1,2  Isr: 3,1        ← still healthy
```

**Restore the broker:**

```bash
docker start kafka-2
sleep 15
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe --topic my-topic
```

`Isr` will now be `1,2,3` again — kafka-2 rejoined and re-synced.

---

### 6.2 Test B — Hard kill (simulated crash)

**Simulates:** OOM kill / node crash — no graceful shutdown.

**Setup** (same two terminals as Test A).

**Kill a broker abruptly:**

```bash
docker kill kafka-3
```

**Observe:**
- Producer keeps sending (2 brokers still up)
- Consumer keeps reading
- Controller quorum (kafka-1 + kafka-2) remains: **2 of 3 → still valid**

**Verify ISR shrink:**

```bash
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe --topic my-topic
```

`Isr` will drop `kafka-3`.

**Restart and re-sync:**

```bash
docker start kafka-3
sleep 20
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe --topic my-topic
```

`Isr: 1,2,3` — kafka-3 is back.

---

### 6.3 Test C — Controller failover

The **controller** coordinates cluster metadata. Killing the active controller triggers a new controller election.

**Find the active controller:**

```bash
docker exec kafka-1 /opt/kafka/bin/kafka-metadata-quorum.sh \
  --bootstrap-server localhost:9092 describe --status \
  | grep -E "LeaderId|CurrentVoters"
```

**Example output:**
```
ClusterId:              MkU3OEVBNTcwNTJENDM2Qk
LeaderId:               1
CurrentVoters:          [{"id":1},{"id":2},{"id":3}]
```

**Kill the leader** (in this case broker 1):

```bash
docker kill kafka-1
```

**Wait ~10 seconds** for re-election:

```bash
sleep 10
docker exec kafka-2 /opt/kafka/bin/kafka-metadata-quorum.sh \
  --bootstrap-server localhost:9095 describe --status \
  | grep -E "LeaderId|CurrentVoters"
```

**Expected:** `LeaderId` is now `2` or `3` — the new controller.

**Bring back the old controller:**

```bash
docker start kafka-1
sleep 15
docker exec kafka-2 /opt/kafka/bin/kafka-metadata-quorum.sh \
  --bootstrap-server localhost:9095 describe --status
```

All 3 voters present again.

---

### 6.4 Test D — Data durability check

**Simulates:** broker crashes mid-write — does data survive?

**1. Produce a batch of numbered messages:**

```bash
seq 1 100 | docker exec -i kafka-1 /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092,localhost:9095,localhost:9097 \
  --topic my-topic
```

**2. Immediately kill one broker (simulating mid-flight crash):**

```bash
docker kill kafka-2
```

**3. Consume all messages:**

```bash
docker exec -it kafka-1 /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic my-topic \
  --from-beginning \
  --property print.value=true \
  | tee /tmp/messages.txt
```

**4. Count:**

```bash
wc -l /tmp/messages.txt
```

**Expected:** `100` — no messages lost, because `min.insync.replicas=2` prevented writes that couldn't be replicated to at least 2 brokers.

**5. Restart kafka-2:**

```bash
docker start kafka-2
sleep 20
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --describe --topic my-topic
```

`Isr` returns to `1,2,3`.

---

### 6.5 Test E — Full cluster restart (recovery)

**Simulates:** planned full outage.

```bash
# Stop all
docker compose down

# Start all
docker compose up -d
sleep 30

# Verify cluster intact
docker compose ps
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe --topic my-topic

# Verify data persisted
docker exec -it kafka-1 /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic my-topic \
  --from-beginning \
  --timeout-ms 5000 \
  | wc -l
```

**Expected:** topics still listed, messages still readable (volumes preserve data).

---

## 7. Spring Boot Integration

### 7.1 `application.yml`

```yaml
spring:
  kafka:
    bootstrap-servers: 10.70.16.242:9092,10.70.16.242:9095,10.70.16.242:9097
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.springframework.kafka.support.serializer.JsonSerializer
      acks: all                       # wait for all in-sync replicas
      retries: 3
      properties:
        enable.idempotence: true
    consumer:
      group-id: inventory-service
      auto-offset-reset: earliest
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.springframework.kafka.support.serializer.JsonDeserializer
      properties:
        spring.json.trusted.packages: "*"
      enable-auto-commit: false       # manual commit for at-least-once
    listener:
      ack-mode: manual_immediate
```

### 7.2 `build.gradle`

```gradle
implementation 'org.springframework.kafka:spring-kafka'
```

### 7.3 Producer example

```java
@Service
public class OrderEventProducer {

    private final KafkaTemplate<String, Object> kafka;

    public OrderEventProducer(KafkaTemplate<String, Object> kafka) {
        this.kafka = kafka;
    }

    public void publishOrderCreated(String orderId, double amount) {
        OrderCreatedEvent event = new OrderCreatedEvent(orderId, amount, Instant.now());
        kafka.send("order.created", orderId, event)
             .whenComplete((meta, ex) -> {
                 if (ex != null) {
                     log.error("Publish failed", ex);
                 } else {
                     log.info("Sent to partition {} offset {}",
                              meta.getRecordMetadata().partition(),
                              meta.getRecordMetadata().offset());
                 }
             });
    }
}
```

### 7.4 Consumer example

```java
@Service
public class InventoryConsumer {

    @KafkaListener(topics = "order.created", groupId = "inventory-service")
    public void onOrderCreated(OrderCreatedEvent event, Acknowledgment ack) {
        try {
            inventoryService.reserve(event.getOrderId(), event.getAmount());
            ack.acknowledge();               // commit offset after success
        } catch (Exception e) {
            log.error("Failed, will retry", e);   // no ack → message re-delivered
        }
    }
}
```

### 7.5 K8s Deployment env

```yaml
env:
  - name: SPRING_KAFKA_BOOTSTRAP_SERVERS
    value: "10.70.16.242:9092,10.70.16.242:9095,10.70.16.242:9097"
```

> **Note:** Kafka's own client-side HA is used here. No Kubernetes Service needed — the `bootstrap-servers` list includes all 3 brokers. Client will discover the cluster topology after the first connection.

---

## 8. Operations

### 8.1 Daily commands

```bash
# Start
docker compose up -d

# Stop (data preserved)
docker compose down

# Stop + delete all data
docker compose down -v

# Logs
docker compose logs -f kafka-1

# Status
docker compose ps

# Shell inside a broker
docker exec -it kafka-1 bash
```

### 8.2 Topic management

```bash
# List topics
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --list

# Create
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --topic <name> --partitions 3 --replication-factor 3

# Describe
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe --topic <name>

# Delete
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --delete --topic <name>
```

### 8.3 Consumer groups

```bash
# List groups
docker exec kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --list

# Describe group (see lag)
docker exec kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe --group inventory-service
```

**Key columns:**
- `CURRENT-OFFSET` — how far the consumer has read
- `LOG-END-OFFSET` — latest message in partition
- `LAG` — difference (should be low)

---

## 9. Troubleshooting

### 9.1 Container unhealthy / restarting

**Check logs:**

```bash
docker compose logs kafka-1 --tail=50
```

**Common causes:**

| Error | Cause | Fix |
| :--- | :--- | :--- |
| `Cluster id mismatch` | Different `CLUSTER_ID` between brokers | Ensure all 3 use the same ID |
| `Node already registered` | Duplicate node.id | Check `KAFKA_NODE_ID` is 1,2,3 unique |
| `Address already in use` | Port conflict with another process | `sudo ss -tlnp \| grep 9092` |
| `Cannot resolve kafka-1` | Hostname mismatch | Container `hostname:` must match `ADVERTISED_LISTENERS` |

### 9.2 External client cannot connect

**Test from VM itself:**

```bash
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server 10.70.16.242:9092 --list
```

**Test from external machine:**

```bash
telnet 10.70.16.242 9092
telnet 10.70.16.242 9095
telnet 10.70.16.242 9097
```

If fail → firewall blocking. Open ports (§3).

**Also verify `EXTERNAL://10.70.16.242:xxxx` uses the correct IP** — not `localhost` or `kafka-1`.

### 9.3 `INCONSISTENT_GROUP_PROTOCOL` or lag explodes

Producer is writing faster than consumer can keep up. Add more partitions + consumer instances, or increase consumer thread count.

### 9.4 After broker restart, ISR stays 2

Replica is syncing. Wait 30–60s and re-check:

```bash
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe --topic <name>
```

If still stuck, check broker disk / logs:

```bash
docker compose logs kafka-2 | grep -i error
```

### 9.5 Cluster refuses to start after `down -v`

Data was deleted. `CLUSTER_ID` was wiped from volumes. Restart with `docker compose up -d` — fresh cluster.

---

## 10. Verification Checklist

| # | Check | Command | Expected |
| :---: | :--- | :--- | :--- |
| 1 | All brokers healthy | `docker compose ps` | 3× `Up (healthy)` |
| 2 | Metadata alive | `kafka-topics.sh --list` | No error |
| 3 | Topic replicated | `--describe --topic my-topic` | `Isr: 1,2,3` |
| 4 | Produce/consume | cross-broker test | Messages flow |
| 5 | Broker shutdown tolerated | `docker stop kafka-2` | Producer/consumer continue |
| 6 | ISR reflects failure | `--describe` after stop | `Isr: 1,3` |
| 7 | Auto re-sync on restart | `docker start kafka-2` | `Isr: 1,2,3` again |
| 8 | Controller failover | `kafka-metadata-quorum.sh` | New `LeaderId` |
| 9 | No data loss | Count messages before/after kill | Equal |
| 10 | Full restart recovery | `compose down` + `up` | Data intact |

---

## 11. Lessons Learned

| Observation | Root cause | Takeaway |
| :--- | :--- | :--- |
| Broker must ack writes | `min.insync.replicas=2` | Never set to 1 in production — data loss risk |
| `Isr` is the source of truth | Replicas lag or die | Monitor `Isr` count; alert if `< replication.factor` |
| Controller is a separate role | KRaft quorum 2/3 | Lose 2 brokers → cluster read-only or dead |
| External IP must be routable | `ADVERTISED_LISTENERS` | If clients can't reach the advertised IP, no connection |
| Host networking is required | Port mapping with KRaft | Bridge networking breaks broker discovery |

---

## 12. Reference & Next Steps

- [Apache Kafka KRaft mode](https://kafka.apache.org/documentation/#kraft)
- [Docker Hub — apache/kafka](https://hub.docker.com/r/apache/kafka)
- [Kafka Producer configs — acks, retries, idempotence](https://kafka.apache.org/documentation/#producerconfigs)

### Next steps for this POC

1. **Wire Spring Boot services** — `inventory-service` publishes `inventory.reserved`; `payment-service` consumes it
2. **Add Prometheus metrics** — enable `metricsConfig: type: strimziMetricsReporter` (if migrating to K8s) or use `kafka-exporter` container alongside
3. **Migrate to Kubernetes** — same topology, deployed via Strimzi Operator (`KafkaNodePool` with `replicas: 3`)
4. **GitOps** — add `kafka-cluster/` folder to ArgoCD applications

---

## 13. Status Summary

| Component | State |
| :--- | :---: |
| kafka-1 (broker+controller) | Done |
| kafka-2 (broker+controller) | Done |
| kafka-3 (broker+controller) | Done |
| KRaft quorum (2/3) | Done |
| Topic `my-topic` with RF=3 | Done |
| Failover (broker stop) | tested |
| Failover (broker kill) | tested |
| Controller failover | tested |
| Data durability (kill mid-write) | tested |
| Spring Boot ready | Done |
