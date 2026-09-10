"""
dlq_consumer.py — Dead Letter Queue Monitor
=============================================
Reads and displays all messages that were permanently failed
and routed to the 'orders-dlq' topic.
"""

import json
import os
from datetime import datetime

from confluent_kafka import DeserializingConsumer, KafkaError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer

# ── Configuration ──────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP     = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
DLQ_TOPIC           = "orders-dlq"
GROUP_ID            = "dlq-monitor-group"
POLL_TIMEOUT        = 2.0

# ── ANSI colour helpers ─────────────────────────────────────────────────────────
RESET  = "\033[0m"
BOLD   = "\033[1m"
CYAN   = "\033[96m"
RED    = "\033[91m"
YELLOW = "\033[93m"
WHITE  = "\033[97m"

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

# ── Schema Registry + Consumer ─────────────────────────────────────────────────
sr_client    = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
consumer_conf = {
    "bootstrap.servers"  : KAFKA_BOOTSTRAP,
    "group.id"           : GROUP_ID,
    "auto.offset.reset"  : "earliest",
    "key.deserializer"   : StringDeserializer("utf_8"),
    "value.deserializer" : AvroDeserializer(sr_client, schema_str),
    "enable.auto.commit" : True,
}
consumer = DeserializingConsumer(consumer_conf)

# ── Main ───────────────────────────────────────────────────────────────────────
def main():
    banner("☠  DEAD LETTER QUEUE MONITOR", RED)
    print(f"  Bootstrap  : {KAFKA_BOOTSTRAP}")
    print(f"  DLQ Topic  : {DLQ_TOPIC}")
    print(f"  Group ID   : {GROUP_ID}")
    print(f"\n{CYAN}Listening for dead-lettered messages… (Ctrl+C to stop){RESET}\n")

    consumer.subscribe([DLQ_TOPIC])
    dlq_count = 0

    try:
        while True:
            msg = consumer.poll(timeout=POLL_TIMEOUT)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print(f"{CYAN}  ⏸  End of DLQ partition reached{RESET}")
                else:
                    print(f"{RED}  ✖  Kafka error: {msg.error()}{RESET}")
                continue

            dlq_count += 1
            order = msg.value()

            # Extract headers
            headers = {}
            if msg.headers():
                for k, v in msg.headers():
                    headers[k] = v.decode() if v else ""

            print(f"{RED}{'═' * 70}")
            print(f"  ☠  [{ts()}]  DLQ MESSAGE #{dlq_count}")
            print(f"{'─' * 70}")
            print(f"  orderId       : {order['orderId']}")
            print(f"  product       : {order['product']}")
            print(f"  price         : ₹{order['price']:.2f}")
            print(f"  partition     : {msg.partition()}")
            print(f"  offset        : {msg.offset()}")
            print(f"  error_reason  : {headers.get('error_reason', 'N/A')}")
            print(f"  original_topic: {headers.get('original_topic', 'N/A')}")
            print(f"  failed_at     : {headers.get('failed_at', 'N/A')}")
            print(f"{'═' * 70}{RESET}\n")

    except KeyboardInterrupt:
        banner(f"🛑  DLQ Monitor stopped — {dlq_count} dead-lettered messages seen", YELLOW)
    finally:
        consumer.close()

if __name__ == "__main__":
    main()
