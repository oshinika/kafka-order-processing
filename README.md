# Kafka Order Processing System

> **Big Data Assignment** — Real-time Kafka message pipeline with Avro serialization, retry logic, and Dead Letter Queue (DLQ).

---

## 📐 Architecture

```
┌──────────────┐    Avro/Kafka     ┌───────────────────────┐
│  producer.py │ ──► orders ──────► │    consumer.py        │
│              │                   │  ✔ Avro Deserialization│
│  Random      │                   │  ✔ Real-time Avg Price │
│  Orders      │                   │  ✔ Retry Logic (3×)   │
│  (Avro)      │                   │  ✔ DLQ Routing        │
└──────────────┘                   └──────────┬────────────┘
                                              │ permanent failure
                                              ▼
                                   ┌───────────────────────┐
                                   │   orders-dlq topic    │
                                   │   dlq_consumer.py     │
                                   └───────────────────────┘
```

---

## 🗂 Project Structure

```
.
├── docker-compose.yml   # Kafka + Zookeeper + Schema Registry
├── order.avsc           # Avro schema for Order messages
├── producer.py          # Kafka producer — publishes random orders
├── consumer.py          # Kafka consumer — aggregation, retry, DLQ
├── dlq_consumer.py      # Monitors the Dead Letter Queue
├── requirements.txt     # Python dependencies
└── README.md
```

---

## 📋 Order Message Schema (`order.avsc`)

| Field     | Type   | Description                          |
|-----------|--------|--------------------------------------|
| `orderId` | string | Unique order identifier (e.g. "1001")|
| `product` | string | Product name (e.g. "Laptop")         |
| `price`   | float  | Randomized price (₹5.00 – ₹500.00)  |

---

## ⚙️ Prerequisites

| Tool          | Version  | Download |
|---------------|----------|----------|
| Docker Desktop| ≥ 4.x    | https://www.docker.com/products/docker-desktop |
| Python        | ≥ 3.10   | https://www.python.org/downloads/ |
| pip           | latest   | Bundled with Python |

---

## 🚀 Setup & Run

### Step 1 — Clone the repository

```bash
git clone <your-repo-url>
cd takehome
```

### Step 2 — Start Kafka infrastructure

```bash
docker-compose up -d
```

Wait ~30 seconds for all containers to be healthy. Verify:

```bash
docker-compose ps
```

All three services (`zookeeper`, `kafka`, `schema-registry`) should show **Up**.

### Step 3 — Install Python dependencies

```bash
pip install -r requirements.txt
```

### Step 4 — Run the Consumer (Terminal 1)

Start the consumer **before** the producer so no messages are missed:

```bash
python consumer.py
```

### Step 5 — Run the Producer (Terminal 2)

```bash
python producer.py
```

You will see 20 orders produced (1 per second). Switch back to Terminal 1 to watch real-time processing, retry attempts, and DLQ routing.

### Step 6 — Run the DLQ Monitor (Terminal 3, optional)

```bash
python dlq_consumer.py
```

This shows all messages that permanently failed after 3 retries.

### Step 7 — Teardown

```bash
docker-compose down
```

---

## 🔍 Features Demonstrated

### ✅ Avro Serialization
- Schema defined in `order.avsc` and registered with Confluent Schema Registry.
- Producer serializes messages using `AvroSerializer`.
- Consumer deserializes using `AvroDeserializer`.

### ✅ Real-time Aggregation
- Consumer maintains a **running total** and **count** of all processed prices.
- After every successful message, it prints the updated **running average price**.

### ✅ Retry Logic
- **25% of messages** trigger a simulated transient failure (configurable via `FAILURE_RATE`).
- Consumer retries up to **3 times** with **exponential back-off**: 1s → 2s → 4s.
- All retry attempts are logged to the console.

### ✅ Dead Letter Queue (DLQ)
- Messages that fail all 3 retries are published to the `orders-dlq` Kafka topic.
- DLQ messages carry metadata headers: `error_reason`, `original_topic`, `failed_at`.
- `dlq_consumer.py` reads and displays all dead-lettered messages.

---

## 🖥 Sample Output

**consumer.py:**
```
══════════════════════════════════════════════════════════════════════
  🎧  KAFKA ORDER CONSUMER STARTED
══════════════════════════════════════════════════════════════════════

──────────────────────────────────────────────────────────────────────
  ▶  [11:30:01] RECEIVED   orderId=1001   product=Laptop         price=₹342.50  [partition=0 offset=0]
  ✔  [11:30:01] PROCESSED  orderId=1001   product=Laptop         price=₹342.50
      📊 Running Average: ₹  342.50  (over 1 order)

──────────────────────────────────────────────────────────────────────
  ▶  [11:30:02] RECEIVED   orderId=1002   product=Keyboard       price=₹ 89.99  [partition=0 offset=1]
  ↩  [11:30:02] RETRY 1/3  orderId=1002 — Simulated transient failure  (backoff=1s)
  ↩  [11:30:03] RETRY 2/3  orderId=1002 — Simulated transient failure  (backoff=2s)
  ✔  [11:30:05] PROCESSED  orderId=1002   product=Keyboard       price=₹ 89.99
      📊 Running Average: ₹  216.25  (over 2 orders)

──────────────────────────────────────────────────────────────────────
  ▶  [11:30:06] RECEIVED   orderId=1003   product=Monitor        price=₹210.00
  ↩  RETRY 1/3 ...
  ↩  RETRY 2/3 ...
  ✖  EXHAUSTED retries for orderId=1003
  ☠  [11:30:13] DLQ        orderId=1003 → orders-dlq  reason: Simulated transient failure...
```

---

## 🌿 Environment Variables

| Variable              | Default               | Description            |
|-----------------------|-----------------------|------------------------|
| `KAFKA_BOOTSTRAP`     | `localhost:9092`      | Kafka broker address   |
| `SCHEMA_REGISTRY_URL` | `http://localhost:8081` | Schema Registry URL  |

---

## 📦 Tech Stack

| Component        | Technology                        |
|------------------|-----------------------------------|
| Message Broker   | Apache Kafka (Confluent 7.5)      |
| Schema Registry  | Confluent Schema Registry         |
| Serialization    | Apache Avro                       |
| Language         | Python 3.10+                      |
| Kafka Client     | `confluent-kafka[avro]` 2.3.0     |
| Infrastructure   | Docker Compose                    |
