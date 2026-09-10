"""
producer.py — Kafka Order Producer
====================================
Produces Avro-serialized Order messages to the 'orders' Kafka topic.
- Reads schema from order.avsc
- Registers schema with Schema Registry
- Sends 20 random orders (one per second)
"""

import json
import random
import time
import os
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer

# ── Configuration ──────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
TOPIC = "orders"
NUM_MESSAGES = 20
SEND_INTERVAL = 1.0  # seconds between messages

# ── ANSI colour helpers ─────────────────────────────────────────────────────────
RESET  = "\033[0m"
BOLD   = "\033[1m"
CYAN   = "\033[96m"
GREEN  = "\033[92m"
YELLOW = "\033[93m"
MAGENTA = "\033[95m"

def banner(text: str, color: str = CYAN):
    width = 60
    print(f"\n{color}{BOLD}{'═' * width}")
    print(f"  {text}")
    print(f"{'═' * width}{RESET}\n")

# ── Load Avro schema ────────────────────────────────────────────────────────────
schema_path = os.path.join(os.path.dirname(__file__), "order.avsc")
with open(schema_path, "r") as f:
    schema_str = json.dumps(json.load(f))

# ── Schema Registry client ─────────────────────────────────────────────────────
schema_registry_conf = {"url": SCHEMA_REGISTRY_URL}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

# ── Avro serializer ────────────────────────────────────────────────────────────
avro_serializer = AvroSerializer(schema_registry_client, schema_str)

# ── Producer configuration ─────────────────────────────────────────────────────
producer_conf = {
    "bootstrap.servers": KAFKA_BOOTSTRAP,
    "key.serializer": StringSerializer("utf_8"),
    "value.serializer": avro_serializer,
}
producer = SerializingProducer(producer_conf)

# ── Data pools ─────────────────────────────────────────────────────────────────
PRODUCTS = ["Laptop", "Headphones", "Keyboard", "Monitor", "Mouse",
            "Webcam", "USB Hub", "SSD Drive", "GPU Card", "RAM Module"]

def generate_order(seq: int) -> dict:
    return {
        "orderId": str(1000 + seq),
        "product": random.choice(PRODUCTS),
        "price": round(random.uniform(5.0, 500.0), 2),
    }

def delivery_report(err, msg):
    if err is not None:
        print(f"{YELLOW}  ⚠  Delivery failed for key {msg.key()}: {err}{RESET}")
    else:
        print(f"{GREEN}  ✔  Delivered → topic={msg.topic()} "
              f"partition={msg.partition()} offset={msg.offset()}{RESET}")

# ── Main ───────────────────────────────────────────────────────────────────────
def main():
    banner("🚀  KAFKA ORDER PRODUCER STARTED", CYAN)
    print(f"  Bootstrap : {KAFKA_BOOTSTRAP}")
    print(f"  Schema Reg: {SCHEMA_REGISTRY_URL}")
    print(f"  Topic     : {TOPIC}")
    print(f"  Messages  : {NUM_MESSAGES}\n")

    for i in range(1, NUM_MESSAGES + 1):
        order = generate_order(i)
        key   = order["orderId"]

        print(f"{MAGENTA}[{i:02d}/{NUM_MESSAGES}]{RESET} Producing  "
              f"orderId={key}  product={order['product']:<14}  "
              f"price=₹{order['price']:>7.2f}")

        producer.produce(topic=TOPIC, key=key, value=order,
                         on_delivery=delivery_report)
        producer.poll(0)          # trigger delivery callbacks
        time.sleep(SEND_INTERVAL)

    producer.flush()
    banner("✅  ALL MESSAGES SENT SUCCESSFULLY", GREEN)

if __name__ == "__main__":
    main()
