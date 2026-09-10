"""
consumer.py — Kafka Order Consumer
=====================================
Consumes Avro-serialized Order messages from the 'orders' topic.

Features:
  ✔ Real-time aggregation  — running count & average price updated after each message
  ✔ Retry logic           — up to MAX_RETRIES (3) attempts with exponential back-off
  ✔ Dead Letter Queue     — permanently failed messages published to 'orders-dlq'
"""

import random
import time
import json
import os
import threading
from datetime import datetime

from confluent_kafka import DeserializingConsumer, SerializingProducer, KafkaError, KafkaException
from confluent_kafka.error import ConsumeError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.serialization import StringDeserializer, StringSerializer

# ── Configuration ──────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP     = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
TOPIC               = "orders"
DLQ_TOPIC           = "orders-dlq"
GROUP_ID            = "order-consumer-group"
MAX_RETRIES         = 3
FAILURE_RATE        = 0.25   # 25% chance a message triggers a simulated transient error
POLL_TIMEOUT        = 1.0    # seconds

# ── ANSI colour helpers ─────────────────────────────────────────────────────────
RESET   = "\033[0m"
BOLD    = "\033[1m"
CYAN    = "\033[96m"
GREEN   = "\033[92m"
YELLOW  = "\033[93m"
RED     = "\033[91m"
MAGENTA = "\033[95m"
BLUE    = "\033[94m"
WHITE   = "\033[97m"

def banner(text: str, color: str = CYAN):
    width = 70
    print(f"\n{color}{BOLD}{'═' * width}")
    print(f"  {text}")
    print(f"{'═' * width}{RESET}\n")

def ts() -> str:
    return datetime.now().strftime("%H:%M:%S")

# ── Load Avro schema ────────────────────────────────────────────────────────────
schema_path = os.path.join(os.path.dirname(__file__), "order.avsc")
with open(schema_path, "r") as f:
    schema_str = json.dumps(json.load(f))

# ── Schema Registry client ─────────────────────────────────────────────────────
sr_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})

# ── Consumer configuration ─────────────────────────────────────────────────────
consumer_conf = {
    "bootstrap.servers"  : KAFKA_BOOTSTRAP,
    "group.id"           : GROUP_ID,
    "auto.offset.reset"  : "earliest",
    "key.deserializer"   : StringDeserializer("utf_8"),
    "value.deserializer" : AvroDeserializer(sr_client, schema_str),
    "enable.auto.commit" : False,
}
consumer = DeserializingConsumer(consumer_conf)

# ── DLQ Producer configuration ─────────────────────────────────────────────────
dlq_producer_conf = {
    "bootstrap.servers": KAFKA_BOOTSTRAP,
    "key.serializer"   : StringSerializer("utf_8"),
    "value.serializer" : AvroSerializer(sr_client, schema_str),
}
dlq_producer = SerializingProducer(dlq_producer_conf)

# ── Aggregation state ─────────────────────────────────────────────────────────
agg_lock           = threading.Lock()
total_price: float = 0.0
total_count: int   = 0

def update_aggregation(price: float) -> tuple[float, int]:
    global total_price, total_count
    with agg_lock:
        total_price += price
        total_count += 1
        avg = total_price / total_count
        return avg, total_count

# ── Simulated business logic ───────────────────────────────────────────────────
def process_order(order: dict) -> None:
    """
    Simulates processing an order.
    Raises RuntimeError ~FAILURE_RATE% of the time (transient failure).
    """
    if random.random() < FAILURE_RATE:
        raise RuntimeError(f"Simulated transient failure for orderId={order['orderId']}")
    # Successful processing — just print the order details
    avg, count = update_aggregation(order["price"])
    print(f"{GREEN}  ✔  [{ts()}] PROCESSED  "
          f"orderId={order['orderId']:<6} "
          f"product={order['product']:<14} "
          f"price=₹{order['price']:>7.2f}{RESET}")
    print(f"{BLUE}      📊 Running Average: ₹{avg:>8.2f}  "
          f"(over {count} order{'s' if count > 1 else ''}){RESET}")

# ── DLQ routing ────────────────────────────────────────────────────────────────
def send_to_dlq(order: dict, reason: str) -> None:
    print(f"{RED}  ☠  [{ts()}] DLQ        "
          f"orderId={order['orderId']} → {DLQ_TOPIC}  reason: {reason}{RESET}")
    headers = [
        ("error_reason", reason.encode()),
        ("original_topic", TOPIC.encode()),
        ("failed_at", ts().encode()),
    ]
    dlq_producer.produce(
        topic=DLQ_TOPIC,
        key=order["orderId"],
        value=order,
        headers=headers,
        on_delivery=lambda err, msg: (
            print(f"{RED}      ✔  DLQ delivery confirmed: offset={msg.offset()}{RESET}")
            if not err else
            print(f"{RED}      ✖  DLQ delivery FAILED: {err}{RESET}")
        ),
    )
    dlq_producer.poll(0)

# ── Retry logic ────────────────────────────────────────────────────────────────
def process_with_retry(order: dict) -> None:
    last_error = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            process_order(order)
            return   # success — exit retry loop
        except RuntimeError as e:
            last_error = str(e)
            backoff = 2 ** (attempt - 1)   # 1s, 2s, 4s
            if attempt < MAX_RETRIES:
                print(f"{YELLOW}  ↩  [{ts()}] RETRY {attempt}/{MAX_RETRIES} "
                      f"orderId={order['orderId']} — {e}  "
                      f"(backoff={backoff}s){RESET}")
                time.sleep(backoff)
            else:
                print(f"{RED}  ✖  [{ts()}] EXHAUSTED retries for "
                      f"orderId={order['orderId']}{RESET}")

    send_to_dlq(order, last_error or "Unknown error after retries exhausted")

# ── Main consumer loop ─────────────────────────────────────────────────────────
def main():
    banner("🎧  KAFKA ORDER CONSUMER STARTED", CYAN)
    print(f"  Bootstrap   : {KAFKA_BOOTSTRAP}")
    print(f"  Schema Reg  : {SCHEMA_REGISTRY_URL}")
    print(f"  Topic       : {TOPIC}")
    print(f"  DLQ Topic   : {DLQ_TOPIC}")
    print(f"  Group ID    : {GROUP_ID}")
    print(f"  Max Retries : {MAX_RETRIES}")
    print(f"  Failure Rate: {int(FAILURE_RATE * 100)}% (simulated transient errors)\n")

    consumer.subscribe([TOPIC])
    print(f"{CYAN}Waiting for messages… (Ctrl+C to stop){RESET}\n")

    try:
        while True:
            # DeserializingConsumer.poll() raises ConsumeError on broker/deserialization
            # errors instead of returning a Message with .error() set (unlike the plain
            # Consumer API) — so those errors must be caught here, not checked via msg.error().
            try:
                msg = consumer.poll(timeout=POLL_TIMEOUT)
            except ConsumeError as e:
                if e.code == KafkaError._PARTITION_EOF:
                    print(f"{CYAN}  ⏸  End of partition reached{RESET}")
                elif e.code == KafkaError.UNKNOWN_TOPIC_OR_PART:
                    # Topic not created yet (auto.create.topics.enable is still propagating)
                    print(f"{YELLOW}  ⏳  Topic '{TOPIC}' not available yet — waiting for auto-creation…{RESET}")
                    time.sleep(1)
                else:
                    print(f"{RED}  ✖  Kafka error: {e}{RESET}")
                continue

            if msg is None:
                continue

            order = msg.value()
            print(f"\n{MAGENTA}{'─' * 70}{RESET}")
            print(f"{MAGENTA}  ▶  [{ts()}] RECEIVED   "
                  f"orderId={order['orderId']:<6} "
                  f"product={order['product']:<14} "
                  f"price=₹{order['price']:>7.2f}"
                  f"  [partition={msg.partition()} offset={msg.offset()}]{RESET}")

            process_with_retry(order)
            try:
                consumer.commit(asynchronous=False)  # commit only after processing
            except KafkaException as e:
                # A broker-side hiccup (e.g. a transient network stall) can knock the
                # consumer out of its group before the commit lands, surfacing as
                # UNKNOWN_MEMBER_ID / REBALANCE_IN_PROGRESS. That message was still
                # processed above; don't crash the whole consumer over a lost commit —
                # log it and keep going (the next successful commit will catch up, and
                # a rejoined-but-stale offset only risks a harmless reprocess, not loss).
                print(f"{RED}  ✖  [{ts()}] COMMIT FAILED for orderId={order['orderId']}: {e}{RESET}")

    except KeyboardInterrupt:
        banner("🛑  Consumer interrupted — shutting down", YELLOW)
    finally:
        consumer.close()
        dlq_producer.flush()
        with agg_lock:
            if total_count > 0:
                banner(
                    f"📊 FINAL STATS: {total_count} processed  "
                    f"| Avg Price ₹{total_price / total_count:.2f}",
                    GREEN,
                )

if __name__ == "__main__":
    main()
