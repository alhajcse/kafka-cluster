# Kafka Monitoring with Grafana Alloy — Complete Guide

> **Stack:** Grafana Alloy (agent) → Prometheus + Loki (existing on `10.70.16.235`)
> **Target:** 3-broker Kafka KRaft cluster on `10.70.16.242`
> **Prerequisites:** Kafka cluster running (see `kafka-multinode-install.md`), Prometheus + Loki + Grafana already deployed
> **Time:** ~30 minutes

---

##  Overview

This guide wires up **full observability** for your 3-broker Kafka cluster using **Grafana Alloy** — a single agent that handles:

- **Metrics** — Kafka broker internals (JMX), broker health, topic stats, consumer lag
- **Logs** — Kafka container logs → Loki

### Why Alloy over separate exporters?

| Aspect | Alloy | Separate exporters |
| :--- | :--- | :--- |
| Agents | **1** (Alloy) | 3 (kafka-exporter + JMX + Promtail) |
| Config | One `config.alloy` | Multiple files (prometheus, docker-compose, promtail) |
| JMX handling | Native | Requires JMX Exporter jar |
| Log collection | Native | Requires Promtail |
| Reload | Hot-reload | Restart required |
| Future-ready | OTel + tracing | Tool-specific |

---

## 1. Architecture

```
┌──────────────────────────────────────────────────────────┐
│        VM 10.70.16.242 (Kafka cluster)                   │
│                                                          │
│   kafka-1 ─┐   (JMX 7071)                               │
│   kafka-2 ─┼── (JMX 7072) ──┐                          │
│   kafka-3 ─┘   (JMX 7073)   │                          │
│                              │                          │
│   kafka-exporter :9308 ──────┤                          │
│   (broker/topic/consumer)    │                          │
│                              │                          │
│   Docker logs ── Alloy ──────┤                          │
│                 (:12345 UI)  │                          │
└──────────────────────────────┼──────────────────────────┘
                               │
              ┌────────────────┴────────────────┐
              │         remote_write            │
              │         loki.write              │
              ▼                                  ▼
┌────────────────────────────────────────────────────────┐
│              VM 10.70.16.235                           │
│                                                         │
│  Prometheus :9090  ◄──── metrics                       │
│  Loki       :3100  ◄──── logs                          │
│  Grafana    :3000  ◄──── visualization                 │
└────────────────────────────────────────────────────────┘
```

---

## 2. Prerequisites Check

Verify the existing stack before starting:

### On `10.70.16.235` (monitoring VM)

```bash
# Prometheus up?
curl -s http://127.0.0.1:9090/-/healthy
# Expected: "Prometheus Server is Healthy."

# Loki up?
curl -s http://127.0.0.1:3100/ready
# Expected: "ready"

# Grafana up?
curl -s http://127.0.0.1:3000/api/health | jq .
# Expected: {"commit":"...","database":"ok","version":"..."}

# Prometheus accepts remote_write?
curl -s -X POST http://127.0.0.1:9090/api/v1/write
# 200 or 400 = accepts writes; 404 = not enabled
```

If Prometheus doesn't accept remote_write, add this flag to its startup args:

```yaml
# In docker-compose.yml on .235
command:
  - '--config.file=/etc/prometheus/prometheus.yml'
  - '--storage.tsdb.path=/prometheus'
  - '--web.enable-lifecycle'
  - '--web.enable-remote-write-receiver'   # ← ADD THIS
```

Then restart:

```bash
cd ~/postgres-ha-cluster/docker-compose/monitoring
docker compose restart prometheus
```

### On `x.x.x.242` (Kafka VM)

```bash
# Kafka brokers healthy?
docker compose ps
# Expected: kafka-1, kafka-2, kafka-3 → Up (healthy)

# Hostnames resolvable?
getent hosts kafka-1 kafka-2 kafka-3
# Expected: 127.0.0.1 kafka-1 (etc.)

# Docker socket accessible?
ls -la /var/run/docker.sock
```

---

## 3. Enable JMX on Kafka Brokers

Kafka exposes broker internals (UnderReplicatedPartitions, ActiveControllerCount, etc.) only via **JMX**. Enable it on all 3 brokers.

### 3.1 Download JMX Exporter JAR

```bash
cd ~/kafka-cluster
mkdir -p jmx-exporter
cd jmx-exporter

wget https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/0.20.0/jmx_prometheus_javaagent-0.20.0.jar \
  -O jmx_prometheus_javaagent.jar

# wget https://raw.githubusercontent.com/prometheus/jmx_exporter/main/#example_configs/kafka-2_0_0.yml \
#  -O kafka.yml

wget https://raw.githubusercontent.com/SimonaCatanoiu/JMX-Exporter-Kafka/main/kafka_kraft_metrics.yml -O kafka.yml

ls -lh
# jmx_prometheus_javaagent.jar  (~1.5 MB)
# kafka.yml                      (~2 KB)
```

### 3.2 Update `docker-compose.yml`

Add **JMX agent** and **JMX remote port** to each broker.

```yaml
services:
  kafka-1:
    # ... existing config ...
    environment:
      # ... existing env vars ...
      KAFKA_JMX_OPTS: >-
        -Dcom.sun.management.jmxremote
        -Dcom.sun.management.jmxremote.authenticate=false
        -Dcom.sun.management.jmxremote.ssl=false
        -Dcom.sun.management.jmxremote.local.only=false
        -Dcom.sun.management.jmxremote.port=9999
        -Dcom.sun.management.jmxremote.rmi.port=9999
        -Djava.rmi.server.hostname=127.0.0.1
      KAFKA_OPTS: "-javaagent:/opt/jmx-exporter/jmx_prometheus_javaagent.jar=7071:/opt/jmx-exporter/kafka.yml"
    volumes:
      - ./kafka-1-data:/tmp/kraft-combined-logs
      - ./jmx-exporter:/opt/jmx-exporter:ro

  kafka-2:
    # ... existing config ...
    environment:
      # ... existing env vars ...
      KAFKA_JMX_OPTS: >-
        -Dcom.sun.management.jmxremote
        -Dcom.sun.management.jmxremote.authenticate=false
        -Dcom.sun.management.jmxremote.ssl=false
        -Dcom.sun.management.jmxremote.local.only=false
        -Dcom.sun.management.jmxremote.port=9998
        -Dcom.sun.management.jmxremote.rmi.port=9998
        -Djava.rmi.server.hostname=127.0.0.1
      KAFKA_OPTS: "-javaagent:/opt/jmx-exporter/jmx_prometheus_javaagent.jar=7072:/opt/jmx-exporter/kafka.yml"
    volumes:
      - ./kafka-2-data:/tmp/kraft-combined-logs
      - ./jmx-exporter:/opt/jmx-exporter:ro

  kafka-3:
    # ... existing config ...
    environment:
      # ... existing env vars ...
      KAFKA_JMX_OPTS: >-
        -Dcom.sun.management.jmxremote
        -Dcom.sun.management.jmxremote.authenticate=false
        -Dcom.sun.management.jmxremote.ssl=false
        -Dcom.sun.management.jmxremote.local.only=false
        -Dcom.sun.management.jmxremote.port=9997
        -Dcom.sun.management.jmxremote.rmi.port=9997
        -Djava.rmi.server.hostname=127.0.0.1
      KAFKA_OPTS: "-javaagent:/opt/jmx-exporter/jmx_prometheus_javaagent.jar=7073:/opt/jmx-exporter/kafka.yml"
    volumes:
      - ./kafka-3-data:/tmp/kraft-combined-logs
      - ./jmx-exporter:/opt/jmx-exporter:ro
```

**Port mapping:**

| Broker | JMX RMI | JMX Prometheus |
| :--- | :---: | :---: |
| kafka-1 | 9999 | **7071** |
| kafka-2 | 9998 | **7072** |
| kafka-3 | 9997 | **7073** |

### 3.3 Restart and verify

```bash
cd ~/kafka-cluster
docker compose down
docker compose up -d
sleep 45
docker compose ps
```

Your log proves it exactly:

```
Caused by: java.net.BindException: Address in use
    at ...HTTPServer$Builder.build
    at ...JavaAgent.premain
FATAL ERROR in native method: processing of -javaagent failed
```
### What's happening:

Main Kafka JVM binds 7071 (JMX agent)

Healthcheck runs kafka-cluster.sh — a second JVM

That JVM inherits KAFKA_OPTS → tries to bind 7071 again

BindException: Address in use → healthcheck exits 1

Docker marks container unhealthy

Kafka is healthy. Only the healthcheck is broken.

No Java, no port conflict, no dependency on Kafka CLI.

Edit docker-compose.yml — replace all three healthcheck blocks:

```
  kafka-1:
    # ... rest of service ...
    healthcheck:
      test: ["CMD-SHELL", "bash -c '</dev/tcp/localhost/9092' || exit 1"]
      interval: 10s
      timeout: 5s
      retries: 6
      start_period: 30s

  kafka-2:
    # ... rest of service ...
    healthcheck:
      test: ["CMD-SHELL", "bash -c '</dev/tcp/localhost/9095' || exit 1"]
      interval: 10s
      timeout: 5s
      retries: 6
      start_period: 30s

  kafka-3:
    # ... rest of service ...
    healthcheck:
      test: ["CMD-SHELL", "bash -c '</dev/tcp/localhost/9097' || exit 1"]
      interval: 10s
      timeout: 5s
      retries: 6
      start_period: 30s

```
Note the port per broker: kafka-1 → 9092, kafka-2 → 9095, kafka-3 → 9097.

Why it works: Bash's /dev/tcp/localhost/9092 opens a TCP socket. Exit 0 = Kafka listener is accepting connections. No JVM, no JMX conflict.

**Verify JMX Prometheus endpoints:**

```bash
for port in 7071 7072 7073; do
  echo "=== :$port ==="
  curl -s http://localhost:$port/metrics | head -3
done
```

**Expected:** Prometheus-format metrics like `# HELP kafka_server_...`.

**Key metrics to check:**

```bash
curl -s http://localhost:7071/metrics | grep -E \
  'kafka_server_replicamanager_underreplicatedpartitions|kafka_controller_kafkacontroller_activecontrollercount'
```

**Expected:**
```
kafka_server_replicamanager_underreplicatedpartitions 0
kafka_controller_kafkacontroller_activecontrollercount 1
```
### If — ActiveControllerCount 0.0
```
kafka_controller_kafkacontroller_activecontrollercount 0.0   ← kafka-1
```
It's normal! Because :

In KRaft Cluster at a time only one broker active controller

Other 2 broker inactive ( count = 0)

at this moment kafka-1  controller is not active → So 0.0

Ensure — Total controller = ১
```
# Three broker-count
for port in 7071 7072 7073; do
  echo -n ":$port → "
  curl -s http://localhost:$port/metrics | \
    grep '^kafka_controller_kafkacontroller_activecontrollercount' | \
    awk '{print $2}'
done
```

Espected Result:

```
:7071 → 0.0    ← kafka-1 inactive
:7072 → 0.0    ← kafka-2 inactive
:7073 → 1.0    ← kafka-3 ACTIVE 
```



> **If `underreplicatedpartitions` is `> 0`** → a broker is lagging. If `activecontrollercount` is `!= 1` → KRaft quorum broken.

### 3.4 Firewall

```bash
sudo ufw allow 7071:7073/tcp
sudo ufw reload
```

---

## 4. Add `kafka-exporter` for Consumer Lag

JMX covers broker internals, but **consumer group lag** requires the dedicated `kafka-exporter`.

### 4.1 Add to `docker-compose.yml`

```yaml
  kafka-exporter:
    image: danielqsj/kafka-exporter:latest
    container_name: kafka-exporter
    restart: unless-stopped
    network_mode: host
    command:
      - '--kafka.server=kafka-1:9092'
      - '--kafka.server=kafka-2:9095'
      - '--kafka.server=kafka-3:9097'
      - '--web.listen-address=:9308'
      - '--topic.filter=.*'
      - '--group.filter=.*'
    depends_on:
      kafka-1: {condition: service_healthy}
      kafka-2: {condition: service_healthy}
      kafka-3: {condition: service_healthy}
```

### 4.2 Start and verify

```bash
cd ~/kafka-cluster
docker compose up -d kafka-exporter
sleep 10

docker compose ps kafka-exporter
curl -s http://localhost:9308/metrics | grep -E '^kafka_brokers|^kafka_topic_partitions' | head
```

**Expected:**
```
kafka_brokers 3
kafka_topic_partitions{topic="smoke-test"} 3
```

### 4.3 Firewall

```bash
sudo ufw allow 9308/tcp
sudo ufw reload
```

---


## .235- Prometheus scrape

```
ssh vm@10.70.16.235
cd ~/postgres-ha-cluster/config/prometheus

# ব্যাকআপ
cp prometheus.yml prometheus.yml.bak.$(date +%Y%m%d_%H%M%S)

# এডিট
nano prometheus.yml
ফাইলের শেষে scrape_configs:-এর ভিতরে যোগ করুন:

yaml
  # ═══════════════════════════════════════════════════════
  # Kafka JMX — 3 brokers on 10.70.16.242
  # ═══════════════════════════════════════════════════════
  - job_name: 'kafka-jmx'
    scrape_interval: 15s
    scrape_timeout: 10s
    static_configs:
      - targets: ['10.70.16.242:7071']
        labels: {instance: 'kafka-1', broker_id: '1', cluster: 'kafka-multi-node'}
      - targets: ['10.70.16.242:7072']
        labels: {instance: 'kafka-2', broker_id: '2', cluster: 'kafka-multi-node'}
      - targets: ['10.70.16.242:7073']
        labels: {instance: 'kafka-3', broker_id: '3', cluster: 'kafka-multi-node'}

  # ═══════════════════════════════════════════════════════
  # Kafka Exporter — consumer lag, topics
  # ═══════════════════════════════════════════════════════
  - job_name: 'kafka-exporter'
    scrape_interval: 30s
    scrape_timeout: 10s
    static_configs:
      - targets: ['10.70.16.242:9308']
        labels: {instance: 'kafka-exporter', cluster: 'kafka-multi-node'}
```
Saved (Ctrl+O, Enter, Ctrl+X)

YAML verify:

```
python3 -c "import yaml; yaml.safe_load(open('prometheus.yml'))" && echo " Valid YAML"
```
Reload + Verify:

```
curl -X POST http://127.0.0.1:9090/-/reload
sleep 20
```

## Targets UP?
```
curl -s http://127.0.0.1:9090/api/v1/targets | \
  jq -r '.data.activeTargets[] | select(.labels.job | test("kafka")) | 
    "\(.labels.job)/\(.labels.instance) → \(.health)"'
```
Expected Output:

```
kafka-jmx/kafka-1 → up
kafka-jmx/kafka-2 → up
kafka-jmx/kafka-3 → up
kafka-exporter/kafka-exporter → up
```

If kafka-exporter/kafka-exporter → down  

JMX exporter is very slow

there is no whitelistObjectNames.
this means?
yaml
### Your config only present "rules"
```
rules:
  - pattern: ...
  - pattern: ...
```

### But there is no limit  MBean scrabing 
Result: JMX Exporter All `MBean` (1০,০০০+) read


Solution — whitelistObjectNames should be added
### Add New kafka.yml (current rules + whitelist)

```
cd ~/kafka-cluster/jmx-exporter

# backup
cp kafka.yml kafka.yml.bak.$(date +%Y%m%d_%H%M%S)

# new config
cat > kafka.yml << 'YAML'
lowercaseOutputName: true
lowercaseOutputLabelNames: true

# ═══════════════════════════════════════════════════════
# CRITICAL: Only scrape essential MBeans (whitelist)
# Without this, JMX exporter reads ALL 10,000+ MBeans → 30s+ response
# With this, only ~500 MBeans → 1-2s response
# ═══════════════════════════════════════════════════════
whitelistObjectNames:
  # Broker internals
  - "kafka.server:type=ReplicaManager,*"
  - "kafka.server:type=BrokerTopicMetrics,*"
  - "kafka.server:type=KafkaRequestHandlerPool,*"
  - "kafka.server:type=KafkaServer,*"
  - "kafka.server:type=SessionExpireListener,*"
  - "kafka.server:type=DelayedOperationPurgatory,*"
  # Network
  - "kafka.network:type=RequestChannel,*"
  - "kafka.network:type=SocketServer,*"
  # Controller
  - "kafka.controller:type=KafkaController,*"
  - "kafka.controller:type=ControllerStats,*"
  # Log
  - "kafka.log:type=LogManager,*"
  - "kafka.log:type=LogFlushStats,*"
  # Coordinator
  - "kafka.coordinator.group:type=GroupMetadataManager,*"
  # KRaft
  - "kafka.server:type=raft-metrics,*"
  - "kafka.server:type=raft-channel-metrics,*"
  - "kafka.server:type=broker-metadata-metrics,*"
  # JVM
  - "java.lang:type=Memory,*"
  - "java.lang:type=GarbageCollector,name=*"
  - "java.lang:type=Threading,*"
  - "java.lang:type=OperatingSystem,*"

# ═══════════════════════════════════════════════════════
# Rules: Keep your existing comprehensive rules
# ═══════════════════════════════════════════════════════
rules:
  # ─── Special cases ───
  - pattern: kafka.server<type=(.+), name=(.+), clientId=(.+), topic=(.+), partition=(.+)><>Value
    name: kafka_server_$1_$2
    type: GAUGE
    labels:
      clientId: "$3"
      topic: "$4"
      partition: "$5"

  - pattern: kafka.server<type=(.+), name=(.+), clientId=(.+), brokerHost=(.+), brokerPort=(.+)><>Value
    name: kafka_server_$1_$2
    type: GAUGE
    labels:
      clientId: "$3"
      broker: "$4:$5"

  - pattern: kafka.coordinator.(\w+)<type=(.+), name=(.+)><>Value
    name: kafka_coordinator_$1_$2_$3
    type: GAUGE

  # ─── Per-second counters ───
  - pattern: kafka.(\w+)<type=(.+), name=(.+)PerSec\w*, (.+)=(.+), (.+)=(.+)><>Count
    name: kafka_$1_$2_$3_total
    type: COUNTER
    labels:
      "$4": "$5"
      "$6": "$7"

  - pattern: kafka.(\w+)<type=(.+), name=(.+)PerSec\w*, (.+)=(.+)><>Count
    name: kafka_$1_$2_$3_total
    type: COUNTER
    labels:
      "$4": "$5"

  - pattern: kafka.(\w+)<type=(.+), name=(.+)PerSec\w*><>Count
    name: kafka_$1_$2_$3_total
    type: COUNTER

  # ─── Generic gauges ───
  - pattern: kafka.(\w+)<type=(.+), name=(.+), (.+)=(.+), (.+)=(.+)><>Value
    name: kafka_$1_$2_$3
    type: GAUGE
    labels:
      "$4": "$5"
      "$6": "$7"

  - pattern: kafka.(\w+)<type=(.+), name=(.+), (.+)=(.+)><>Value
    name: kafka_$1_$2_$3
    type: GAUGE
    labels:
      "$4": "$5"

  - pattern: kafka.(\w+)<type=(.+), name=(.+)><>Value
    name: kafka_$1_$2_$3
    type: GAUGE

  # ─── Counters ───
  - pattern: kafka.(\w+)<type=(.+), name=(.+), (.+)=(.+), (.+)=(.+)><>Count
    name: kafka_$1_$2_$3_count
    type: COUNTER
    labels:
      "$4": "$5"
      "$6": "$7"

  - pattern: kafka.(\w+)<type=(.+), name=(.+), (.+)=(.+)><>Count
    name: kafka_$1_$2_$3_count
    type: COUNTER
    labels:
      "$4": "$5"

  - pattern: kafka.(\w+)<type=(.+), name=(.+)><>Count
    name: kafka_$1_$2_$3_count
    type: COUNTER

  # ─── Percent gauges ───
  - pattern: kafka.(\w+)<type=(.+), name=(.+)Percent\w*><>MeanRate
    name: kafka_$1_$2_$3_percent
    type: GAUGE

  - pattern: kafka.(\w+)<type=(.+), name=(.+)Percent\w*><>Value
    name: kafka_$1_$2_$3_percent
    type: GAUGE

  - pattern: kafka.(\w+)<type=(.+), name=(.+)Percent\w*, (.+)=(.+)><>Value
    name: kafka_$1_$2_$3_percent
    type: GAUGE
    labels:
      "$4": "$5"

  # ─── KRaft state ───
  - pattern: "kafka.server<type=raft-metrics><>current-state: ([a-z]+)"
    name: kafka_server_raft_metrics_current_state_info
    type: GAUGE
    value: 1
    labels:
      "state": "$1"

  # ─── KRaft metrics ───
  - pattern: kafka.server<type=(.+)><>([a-z-]+)-total
    name: kafka_server_$1_$2_total
    type: COUNTER

  - pattern: kafka.server<type=(.+)><>([a-z-]+)
    name: kafka_server_$1_$2
    type: GAUGE

  # ─── JVM metrics ───
  - pattern: "java.lang<type=Memory><HeapMemoryUsage>(used|committed|max)"
    name: jvm_memory_heap_$1_bytes
    type: GAUGE

  - pattern: "java.lang<type=Memory><NonHeapMemoryUsage>(used|committed|max)"
    name: jvm_memory_nonheap_$1_bytes
    type: GAUGE

  - pattern: "java.lang<type=GarbageCollector, name=(.+)><>CollectionCount"
    name: jvm_gc_collection_count_total
    type: COUNTER
    labels:
      gc: "$1"

  - pattern: "java.lang<type=GarbageCollector, name=(.+)><>CollectionTime"
    name: jvm_gc_collection_time_milliseconds_total
    type: COUNTER
    labels:
      gc: "$1"

  - pattern: "java.lang<type=Threading><>(ThreadCount|PeakThreadCount|DaemonThreadCount)"
    name: jvm_threads_$1
    type: GAUGE

  - pattern: "java.lang<type=OperatingSystem><>(OpenFileDescriptorCount|MaxFileDescriptorCount|ProcessCpuLoad|SystemCpuLoad)"
    name: jvm_os_$1
    type: GAUGE
YAML

echo " config created"
wc -l kafka.yml

```

### Broker Restart
```
cd ~/kafka-cluster
docker compose restart kafka-1 kafka-2 kafka-3
sleep 60
docker compose ps | grep kafka
```
Expected:

```
kafka-1   Up X (healthy)
kafka-2   Up X (healthy)
kafka-3   Up X (healthy)

```

### Response Time 
```
# .242-এ
for port in 7071 7072 7073; do
  echo -n "port $port: "
  curl -s -o /dev/null -w "%{time_total}s (%{http_code})\n" --max-time 30 http://10.70.16.242:$port/metrics
done

```

Expected:

```
port 7071: 1.2s (200)   ← 30s from 1.2s 
port 7072: 1.5s (200)   ← 30s from 1.5s 
port 7073: 1.3s (200)   ← 25s from 1.3s
```


### Metrics Verify

```
# ─── Broker count ───
curl -sG http://127.0.0.1:9090/api/v1/query \
  --data-urlencode 'query=kafka_brokers' | jq -r '.data.result[0].value[1]'
# Expected: "3"

# ─── Under-replicated partitions ───
curl -sG http://127.0.0.1:9090/api/v1/query \
  --data-urlencode 'query=sum(kafka_server_replicamanager_underreplicatedpartitions)' \
  | jq -r '.data.result[0].value[1]'
# Expected: "0"

# ─── Active controller ───
curl -sG http://127.0.0.1:9090/api/v1/query \
  --data-urlencode 'query=sum(kafka_controller_kafkacontroller_activecontrollercount)' \
  | jq -r '.data.result[0].value[1]'
# Expected: "1"


```

### Duplicate verify
```
curl -sG http://127.0.0.1:9090/api/v1/query \
  --data-urlencode 'query=count(kafka_brokers)' | jq -r '.data.result[0].value[1]'
```
Expected: "1" 


## 5. Deploy Grafana Alloy on `.242`

### Add Alloy (logs only)


Step 1: .242-Create Alloy
```
ssh vm@10.70.16.242
cd ~/kafka-cluster
```
### Create a directory
```
mkdir -p alloy
```
### config.alloy (Only logs)
```
cat > alloy/config.alloy << 'ALLOY'
logging {
  level  = "info"
  format = "logfmt"
}

// Docker containers discover
discovery.docker "containers" {
  host = "unix:///var/run/docker.sock"
}

// Lebel add
discovery.relabel "docker_logs" {
  targets = discovery.docker.containers.targets

  rule {
    source_labels = ["__meta_docker_container_name"]
    regex         = "/(.*)"
    target_label  = "container"
  }
  rule {
    source_labels = ["__meta_docker_container_log_stream"]
    target_label  = "stream"
  }
  rule {
    source_labels = ["__meta_docker_container_label_com_docker_compose_service"]
    target_label  = "service"
  }
}

// Read log and send to Loki
loki.source.docker "containers" {
  host          = "unix:///var/run/docker.sock"
  targets       = discovery.relabel.docker_logs.output
  forward_to    = [loki.process.kafka_logs.receiver]
  relabel_rules = discovery.relabel.docker_logs.rules
}

loki.process "kafka_logs" {
  stage.static_labels {
    values = {
      cluster = "kafka-multi-node",
      env     = "lab",
    }
  }
  forward_to = [loki.write.monitoring.receiver]
}

loki.write "monitoring" {
  endpoint {
    url = "http://10.70.16.235:3100/loki/api/v1/push"
  }
}
ALLOY

echo "config.alloy created"
```



### 5.2 Add Alloy to `docker-compose.yml`

```

nano docker-compose.yml


  alloy:
    image: grafana/alloy:latest
    container_name: alloy
    restart: unless-stopped
    network_mode: host
    volumes:
      - ./alloy/config.alloy:/etc/alloy/config.alloy:ro
      - /var/run/docker.sock:/var/run/docker.sock
      - /var/lib/docker/containers:/var/lib/docker/containers:ro
      - alloy-data:/var/lib/alloy
    command:
      - run
      - --server.http.listen-addr=0.0.0.0:12345
      - /etc/alloy/config.alloy
    depends_on:
      - kafka-1
      - kafka-2
      - kafka-3
      - kafka-exporter

volumes:
  alloy-data:
```
Save (Ctrl+O, Enter, Ctrl+X)


### 5.3 Start Alloy

```bash
cd ~/kafka-cluster
docker compose up -d alloy
sleep 15

docker compose ps alloy
docker logs alloy --tail=20
```

**Expected logs:**
```
level=info msg="now listening for http traffic" addr=0.0.0.0:12345
level=info msg="started prometheus.scrape" component=kafka_jmx
level=info msg="started loki.source.docker" component=containers
```

### 5.4 Firewall

```bash
sudo ufw allow 12345/tcp
sudo ufw reload
```

---

## 6. Verify Metrics Arriving at Prometheus (on `.235`)

### 6.1 Check remote_write succeeded

From `10.70.16.235`:

```bash
# Query for kafka metrics
curl -sG http://127.0.0.1:9090/api/v1/query \
  --data-urlencode 'query=kafka_brokers' | jq .
```

**Expected:**
```json
{
  "status": "success",
  "data": {
    "resultType": "vector",
    "result": [
      {
        "metric": {"cluster": "kafka-multi-node", "env": "lab", "job": "kafka-exporter"},
        "value": [..., "3"]
      }
    ]
  }
}
```

### 6.2 Check all critical metrics

```bash
# ─── Broker count ───
curl -sG http://127.0.0.1:9090/api/v1/query \
  --data-urlencode 'query=kafka_brokers' | jq -r '.data.result[0].value[1]'
# Expected: "3"

# ─── Under-replicated partitions (should be 0) ───
curl -sG http://127.0.0.1:9090/api/v1/query \
  --data-urlencode 'query=kafka_server_replicamanager_underreplicatedpartitions' \
  | jq -r '.data.result[] | "\(.metric.instance) → \(.value[1])"'
# Expected:
# kafka-1 → 0
# kafka-2 → 0
# kafka-3 → 0

# ─── Active controllers (should be 1) ───
curl -sG http://127.0.0.1:9090/api/v1/query \
  --data-urlencode 'query=sum(kafka_controller_kafkacontroller_activecontrollercount)' \
  | jq -r '.data.result[0].value[1]'
# Expected: "1"

# ─── Consumer lag (if any consumers running) ───
curl -sG http://127.0.0.1:9090/api/v1/query \
  --data-urlencode 'query=sum(kafka_consumergroup_lag) by (consumergroup)' | jq .
```

### 6.3 Check Prometheus targets

```bash
curl -s http://127.0.0.1:9090/api/v1/targets \
  | jq -r '.data.activeTargets[] | select(.labels.job | test("kafka")) | "\(.labels.job) \(.labels.instance) → \(.health)"'
```

**Expected:**
```
kafka-jmx localhost:7071 → up
kafka-jmx localhost:7072 → up
kafka-jmx localhost:7073 → up
kafka-exporter localhost:9308 → up
```

> **Note:** Since Alloy uses `remote_write`, these targets appear in Alloy's `/metrics` — they don't show in Prometheus's `/targets` page. But the **metrics themselves** show up in Prometheus queries.

---

## 7. Verify Logs Arriving at Loki

From `10.70.16.235`:

```bash
# ─── Check available container labels ───
curl -sG http://127.0.0.1:3100/loki/api/v1/label/container/values | jq .
```

**Expected:**
```json
{
  "status": "success",
  "data": [
    "kafka-1",
    "kafka-2",
    "kafka-3",
    "kafka-exporter",
    "alloy"
  ]
}
```

```bash
# ─── Query kafka-1 logs ───
curl -sG http://127.0.0.1:3100/loki/api/v1/query_range \
  --data-urlencode 'query={container="kafka-1"}' \
  --data-urlencode 'limit=5' | jq -r '.data.result[].values[][1]'
```

**Expected:** Kafka startup lines, broker logs, etc.

```bash
# ─── Filter by log level ───
curl -sG http://127.0.0.1:3100/loki/api/v1/query_range \
  --data-urlencode 'query={container=~"kafka-.*"} | level="ERROR"' \
  --data-urlencode 'limit=5' | jq -r '.data.result[].values[][1]'
```

---

## 8. Grafana Dashboards

### 8.1 Add Prometheus data source (if not already)

Grafana → **Connections → Data sources → Prometheus**
- URL: `http://10.70.16.235:9090`
- Save & test

### 8.2 Import Kafka dashboards

Grafana → **Dashboards → Import** → enter the ID → select Prometheus → Import.

| Dashboard | ID | Shows |
| :--- | :---: | :--- |
| **Kafka Exporter Overview** | `7589` | Brokers, topics, consumer lag |
| **Kafka JMX Overview** | `721` | Broker internals (JMX) |
| **Kafka Cluster** | `12345` | Alternative view (search "Kafka" in Grafana) |

### 8.3 Create a custom health panel

Create a new dashboard with these panels:

**Panel 1 — Broker Count (Stat)**

```promql
kafka_brokers
```
- Threshold: red if `< 3`, green if `= 3`

**Panel 2 — Under-Replicated Partitions (Stat)**

```promql
sum(kafka_server_replicamanager_underreplicatedpartitions)
```
- Threshold: green `= 0`, red `> 0`

**Panel 3 — Active Controllers (Stat)**

```promql
sum(kafka_controller_kafkacontroller_activecontrollercount)
```
- Threshold: green `= 1`, red `!= 1`

**Panel 4 — Consumer Lag (Time series)**

```promql
sum(kafka_consumergroup_lag) by (consumergroup)
```

**Panel 5 — Messages In/Out (Time series)**

```promql
rate(kafka_server_brokertopicmetrics_messagesin_total[1m])
rate(kafka_server_brokertopicmetrics_messagesout_total[1m])
```

**Panel 6 — JVM Heap (Time series)**

```promql
jvm_memory_bytes_used{area="heap", job="kafka-jmx"}
```

### 8.4 Loki log panel

Create a **Logs** panel:

```logql
{cluster="kafka-multi-node", container=~"kafka-.*"} | level="ERROR"
```

---

## 9. Alerts (Critical for HA Cluster)

### 9.1 Add alert rules to Prometheus

On `.235`, create `prometheus-rules/kafka.yml`:

```yaml
groups:
  - name: kafka-alerts
    interval: 30s
    rules:

      - alert: KafkaBrokerDown
        expr: kafka_brokers < 3
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "Kafka broker count below 3"
          description: "Only {{ $value }} brokers are up (expected 3)"

      - alert: KafkaUnderReplicatedPartitions
        expr: kafka_server_replicamanager_underreplicatedpartitions > 0
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Under-replicated partitions on {{ $labels.instance }}"
          description: "{{ $value }} partitions have fewer than 3 replicas"

      - alert: KafkaNoActiveController
        expr: sum(kafka_controller_kafkacontroller_activecontrollercount) != 1
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "KRaft quorum broken — no active controller"
          description: "Current active controllers: {{ $value }}"

      - alert: KafkaOfflinePartitions
        expr: kafka_controller_kafkacontroller_offlinepartitionscount > 0
        for: 2m
        labels:
          severity: critical
        annotations:
          summary: "Offline partitions on {{ $labels.instance }}"
          description: "{{ $value }} partitions are offline"

      - alert: KafkaHighConsumerLag
        expr: sum(kafka_consumergroup_lag) by (consumergroup) > 10000
        for: 10m
        labels:
          severity: warning
        annotations:
          summary: "High consumer lag for {{ $labels.consumergroup }}"
          description: "Lag is {{ $value }} messages"

      - alert: KafkaHighDiskUsage
        expr: |
          kafka_log_log_size{job="kafka-jmx"}
          / 1e9 > 40
        for: 15m
        labels:
          severity: warning
        annotations:
          summary: "Kafka log directory > 40 GB on {{ $labels.instance }}"
```

### 9.2 Mount rules in Prometheus

Update `docker-compose.yml` on `.235`:

```yaml
  prometheus:
    volumes:
      - ./prometheus.yml:/etc/prometheus/prometheus.yml
      - ./prometheus-rules:/etc/prometheus/rules:ro   # ← ADD THIS
```

Update `prometheus.yml`:

```yaml
rule_files:
  - /etc/prometheus/rules/*.yml
```

Restart:

```bash
docker compose restart prometheus
sleep 10
curl -sG http://127.0.0.1:9090/api/v1/rules \
  | jq -r '.data.groups[] | .name' | grep kafka
# → kafka-alerts
```

### 9.3 Test an alert (safe)

**Simulate broker down:**

```bash
# On .242
docker stop kafka-2
sleep 90

# On .235
curl -sG http://127.0.0.1:9090/api/v1/alerts \
  | jq -r '.data.alerts[] | "\(.labels.alertname) → \(.state)"'
# → KafkaBrokerDown → firing
```

**Restore:**

```bash
# On .242
docker start kafka-2
```

---

## 10. Troubleshooting

### 10.1 Alloy not sending metrics to Prometheus

```bash
# On .242 — check Alloy logs
docker logs alloy --tail=50 | grep -iE 'error|remote_write|failed'

# Check Prometheus accepts remote_write
curl -s -X POST http://10.70.16.235:9090/api/v1/write
# 400 = accepts (needs body), 404 = NOT enabled
```

**Fix:** add `--web.enable-remote-write-receiver` to Prometheus command args.

### 10.2 No JMX metrics

```bash
# On .242
curl -s http://localhost:7071/metrics | head -5

# If empty → JMX agent didn't load
docker logs kafka-1 2>&1 | grep -i "jmx\|javaagent"

# Check env var is set
docker exec kafka-1 env | grep KAFKA_OPTS
```

**Fix:** ensure `jmx_prometheus_javaagent.jar` and `kafka.yml` exist in `jmx-exporter/`:

```bash
ls -la ~/kafka-cluster/jmx-exporter/
```

### 10.3 No logs in Loki

```bash
# On .242
docker logs alloy --tail=30 | grep -iE 'loki|docker|error'

# Check Docker socket mount
docker exec alloy ls -la /var/run/docker.sock

# Test Loki from .242
curl -s http://10.70.16.235:3100/ready
```

**Fix:** if socket not mounted, check `docker-compose.yml`:
```yaml
volumes:
  - /var/run/docker.sock:/var/run/docker.sock
```

### 10.4 Consumer lag metrics missing

```bash
# Verify kafka-exporter connects
docker logs kafka-exporter --tail=20

# Test directly
curl -s http://localhost:9308/metrics | grep kafka_consumergroup_lag
```

**If empty:** No consumers are active. Create a test consumer:

```bash
docker exec -it kafka-1 /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic smoke-test \
  --group test-group \
  --from-beginning &
sleep 10
curl -s http://localhost:9308/metrics | grep test-group
```

### 10.5 Alloy UI not reachable

```bash
# Port listening?
sudo ss -tlnp | grep 12345

# Firewall?
sudo ufw status | grep 12345
```

Open `http://10.70.16.242:12345` in browser. Alloy UI shows:
- **Component graph** — visual pipeline
- **Component health** — green/red per component
- **Live metrics** — self-monitoring

---

## 11. Verification Checklist

Run these in order — all should pass:

### 11.1 On `.242` (Kafka VM)

```bash
# 1. All containers
docker compose ps
# → kafka-1,2,3 (healthy); kafka-exporter, alloy (running)

# 2. JMX endpoints
for p in 7071 7072 7073; do
  echo -n ":$p → "
  curl -s http://localhost:$p/metrics | head -1
done
# → all show "# HELP ..."

# 3. Exporter
curl -s http://localhost:9308/metrics | grep '^kafka_brokers'
# → kafka_brokers 3

# 4. Alloy UI
curl -s http://localhost:12345/-/ready
# → Alloy is ready

# 5. Alloy logs
docker logs alloy --tail=20 | grep -c "error"
# → 0
```

### 11.2 On `.235` (Monitoring VM)

```bash
# 1. Metrics flow
curl -sG http://127.0.0.1:9090/api/v1/query \
  --data-urlencode 'query=kafka_brokers' | jq -r '.data.result[0].value[1]'
# → "3"

# 2. Under-replicated
curl -sG http://127.0.0.1:9090/api/v1/query \
  --data-urlencode 'query=sum(kafka_server_replicamanager_underreplicatedpartitions)' \
  | jq -r '.data.result[0].value[1]'
# → "0"

# 3. Active controller
curl -sG http://127.0.0.1:9090/api/v1/query \
  --data-urlencode 'query=sum(kafka_controller_kafkacontroller_activecontrollercount)' \
  | jq -r '.data.result[0].value[1]'
# → "1"

# 4. Logs in Loki
curl -sG http://127.0.0.1:3100/loki/api/v1/label/container/values | jq -r '.data[]' | grep kafka
# → kafka-1, kafka-2, kafka-3

# 5. Alert rules loaded
curl -sG http://127.0.0.1:9090/api/v1/rules | jq -r '.data.groups[].name' | grep kafka
# → kafka-alerts
```

### 11.3 In Grafana

| Check | Expected |
| :--- | :--- |
| Prometheus data source → Save & Test |  "Data source is working" |
| Loki data source → Save & Test |  "Data source is working" |
| Dashboard 7589 imported |  Panels populated |
| Dashboard 721 imported |  Panels populated |
| Explore → Loki → `{container="kafka-1"}` | Log lines appear |
| Explore → Prometheus → `kafka_brokers` | Value = 3 |

---

## 12. Complete Copy-Paste Runbook

```bash
# ═══════════════════════════════════════════════════════
# KAFKA MONITORING SETUP — ONE BLOCK
# ═══════════════════════════════════════════════════════

# ─── On .242: Download JMX exporter ───
cd ~/kafka-cluster && mkdir -p jmx-exporter && cd jmx-exporter
wget -q https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/0.20.0/jmx_prometheus_javaagent-0.20.0.jar -O jmx_prometheus_javaagent.jar
wget -q https://raw.githubusercontent.com/prometheus/jmx_exporter/main/example_configs/kafka-2_0_0.yml -O kafka.yml
cd ..

# ─── Update docker-compose.yml: add JMX + kafka-exporter + alloy ───
# (paste the YAML blocks from §3.2, §4.1, §5.2)

# ─── Restart stack ───
docker compose down
docker compose up -d
sleep 60

# ─── Verify ───
docker compose ps
curl -s http://localhost:7071/metrics | head -1
curl -s http://localhost:9308/metrics | grep '^kafka_brokers'
curl -s http://localhost:12345/-/ready

# ─── Firewall ───
sudo ufw allow 7071:7073/tcp
sudo ufw allow 9308/tcp
sudo ufw allow 12345/tcp
sudo ufw reload

# ─── On .235: verify data arriving ───
curl -sG http://127.0.0.1:9090/api/v1/query \
  --data-urlencode 'query=kafka_brokers' | jq -r '.data.result[0].value[1]'
# → 3

curl -sG http://127.0.0.1:3100/loki/api/v1/label/container/values \
  | jq -r '.data[]' | grep kafka
# → kafka-1, kafka-2, kafka-3

# ─── Import Grafana dashboards ───
# Dashboard → Import → 7589 → Prometheus → Import
# Dashboard → Import → 721  → Prometheus → Import
```

---

## 13. What You Get

| Category | Metrics | Source |
| :--- | :--- | :--- |
| **Broker health** | `kafka_brokers`, `kafka_server_replicamanager_underreplicatedpartitions` | kafka-exporter + JMX |
| **KRaft quorum** | `kafka_controller_kafkacontroller_activecontrollercount` | JMX |
| **Topics** | `kafka_topic_partitions`, `kafka_topic_partition_replicas` | kafka-exporter |
| **Consumer lag** | `kafka_consumergroup_lag`, `kafka_consumergroup_current_offset` | kafka-exporter |
| **Throughput** | `kafka_server_brokertopicmetrics_messagesin_total` | JMX |
| **JVM** | `jvm_memory_bytes_used`, `jvm_gc_pause_seconds` | JMX |
| **Logs** | All Kafka container logs | Alloy → Loki |
| **Alerts** | 6 critical rules | Prometheus |

---

## 14. Related Docs

- [Kafka Install Guide](./kafka-multinode-install.md)
- [Kafka Reset Guide](./kafka-reset-guide.md)
- [Kafka Architecture](./kafka-multi-node-architecture.md)
- [Prometheus Setup](./prometheus-setup.md) *(if present in repo)*
