"""
dashboard.py — Live Web Dashboard for the Kafka Order Processing System
==========================================================================
Runs its own Kafka consumer — a separate consumer group from consumer.py, so
it can run alongside it without interfering — implementing the same
retry / DLQ logic, and streams every event to a browser dashboard in real
time over Server-Sent Events (SSE).

Usage:
    python dashboard.py
    then open http://localhost:5000 while producer.py is running.

The dashboard consumer starts from the latest offset (not "earliest"), so a
freshly opened dashboard shows only new activity rather than replaying the
entire topic backlog.
"""

import json
import os
import random
import threading
import time
import queue
from datetime import datetime

from flask import Flask, Response

from confluent_kafka import DeserializingConsumer, SerializingProducer, KafkaError, KafkaException
from confluent_kafka.error import ConsumeError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.serialization import StringDeserializer, StringSerializer

# ── Configuration ──────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP     = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
TOPIC                = "orders"
DLQ_TOPIC            = "orders-dlq"
GROUP_ID              = "order-dashboard-group"  # separate group: doesn't compete with consumer.py
MAX_RETRIES          = 3
FAILURE_RATE         = 0.25
POLL_TIMEOUT         = 1.0
WEB_PORT             = int(os.getenv("DASHBOARD_PORT", "5000"))

# ── Load Avro schema ────────────────────────────────────────────────────────────
schema_path = os.path.join(os.path.dirname(__file__), "order.avsc")
with open(schema_path, "r") as f:
    schema_str = json.dumps(json.load(f))

sr_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})

consumer_conf = {
    "bootstrap.servers"  : KAFKA_BOOTSTRAP,
    "group.id"           : GROUP_ID,
    "auto.offset.reset"  : "latest",   # show only orders produced while the dashboard runs
    "key.deserializer"   : StringDeserializer("utf_8"),
    "value.deserializer" : AvroDeserializer(sr_client, schema_str),
    "enable.auto.commit" : False,
}
consumer = DeserializingConsumer(consumer_conf)

dlq_producer_conf = {
    "bootstrap.servers": KAFKA_BOOTSTRAP,
    "key.serializer"   : StringSerializer("utf_8"),
    "value.serializer" : AvroSerializer(sr_client, schema_str),
}
dlq_producer = SerializingProducer(dlq_producer_conf)

app = Flask(__name__)

# ── Event broadcasting (one subscriber queue per open browser tab) ─────────────
subscribers = []
subscribers_lock = threading.Lock()

def broadcast(event: dict) -> None:
    event["ts"] = datetime.now().strftime("%H:%M:%S")
    with subscribers_lock:
        dead = []
        for q in subscribers:
            try:
                q.put_nowait(event)
            except queue.Full:
                dead.append(q)
        for q in dead:
            subscribers.remove(q)

# ── Aggregation state ───────────────────────────────────────────────────────────
state_lock = threading.Lock()
total_price = 0.0
total_count = 0
retry_count = 0
dlq_count = 0

def snapshot() -> dict:
    with state_lock:
        avg = (total_price / total_count) if total_count else 0.0
        return {
            "total_count": total_count,
            "avg_price": round(avg, 2),
            "retry_count": retry_count,
            "dlq_count": dlq_count,
        }

# ── Business logic (mirrors consumer.py's retry/DLQ behavior) ──────────────────
def process_order(order: dict) -> None:
    global total_price, total_count
    if random.random() < FAILURE_RATE:
        raise RuntimeError(f"Simulated transient failure for orderId={order['orderId']}")
    with state_lock:
        total_price += order["price"]
        total_count += 1
        avg, count = total_price / total_count, total_count
    broadcast({"type": "processed", "order": order, "avg_price": round(avg, 2), "count": count})

def send_to_dlq(order: dict, reason: str) -> None:
    global dlq_count
    headers = [
        ("error_reason", reason.encode()),
        ("original_topic", TOPIC.encode()),
        ("failed_at", datetime.now().strftime("%H:%M:%S").encode()),
    ]
    dlq_producer.produce(topic=DLQ_TOPIC, key=order["orderId"], value=order, headers=headers)
    dlq_producer.poll(0)
    with state_lock:
        dlq_count += 1
        count = dlq_count
    broadcast({"type": "dlq", "order": order, "reason": reason, "count": count})

def process_with_retry(order: dict) -> None:
    global retry_count
    last_error = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            process_order(order)
            return
        except RuntimeError as e:
            last_error = str(e)
            backoff = 2 ** (attempt - 1)
            with state_lock:
                retry_count += 1
                count = retry_count
            if attempt < MAX_RETRIES:
                broadcast({"type": "retry", "order": order, "attempt": attempt,
                           "max": MAX_RETRIES, "backoff": backoff, "count": count})
                time.sleep(backoff)
            else:
                broadcast({"type": "exhausted", "order": order})
    send_to_dlq(order, last_error or "Unknown error after retries exhausted")

# ── Kafka consumer thread ───────────────────────────────────────────────────────
def consume_loop() -> None:
    consumer.subscribe([TOPIC])
    broadcast({"type": "status", "message": f"Subscribed to '{TOPIC}' (group={GROUP_ID})"})
    while True:
        try:
            msg = consumer.poll(timeout=POLL_TIMEOUT)
        except ConsumeError as e:
            if e.code == KafkaError.UNKNOWN_TOPIC_OR_PART:
                time.sleep(1)
            else:
                broadcast({"type": "status", "message": f"Kafka error: {e}"})
            continue
        except Exception as e:  # keep the background thread alive on anything unexpected
            broadcast({"type": "status", "message": f"Unexpected error: {e}"})
            time.sleep(1)
            continue

        if msg is None:
            continue

        order = msg.value()
        broadcast({"type": "received", "order": order, "offset": msg.offset()})
        process_with_retry(order)
        try:
            consumer.commit(asynchronous=False)
        except KafkaException as e:
            broadcast({"type": "status", "message": f"Commit warning: {e}"})

# ── Routes ──────────────────────────────────────────────────────────────────────
@app.route("/events")
def events():
    q = queue.Queue(maxsize=200)
    with subscribers_lock:
        subscribers.append(q)

    def gen():
        try:
            yield f"data: {json.dumps({'type': 'snapshot', **snapshot()})}\n\n"
            while True:
                event = q.get()
                yield f"data: {json.dumps(event)}\n\n"
        finally:
            with subscribers_lock:
                if q in subscribers:
                    subscribers.remove(q)

    return Response(gen(), mimetype="text/event-stream")

@app.route("/")
def index():
    return DASHBOARD_HTML

DASHBOARD_HTML = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Kafka Order Dashboard</title>
<style>
  :root {
    --bg:        #0d0d0d;
    --surface:   #1a1a19;
    --ink:       #ffffff;
    --ink-2:     #c3c2b7;
    --ink-muted: #898781;
    --border:    rgba(255,255,255,0.10);
    --grid:      #2c2c2a;
    --good:      #0ca30c;
    --warning:   #fab219;
    --critical:  #d03b3b;
    --accent:    #3987e5;
    font-family: system-ui, -apple-system, "Segoe UI", sans-serif;
  }
  * { box-sizing: border-box; }
  body { margin: 0; background: var(--bg); color: var(--ink); padding: 24px 20px 48px; }
  .wrap { max-width: 1200px; margin: 0 auto; }

  header { display: flex; align-items: center; justify-content: space-between; margin-bottom: 24px; flex-wrap: wrap; gap: 12px; }
  h1 { font-size: 22px; margin: 0 0 4px; }
  header p { margin: 0; color: var(--ink-muted); font-size: 13px; }
  .status { display: flex; align-items: center; gap: 8px; font-size: 13px; color: var(--ink-2); }
  .dot { width: 9px; height: 9px; border-radius: 50%; background: var(--ink-muted); transition: background .2s; }
  .dot.live { background: var(--good); box-shadow: 0 0 0 3px rgba(12,163,12,0.25); }
  .dot.down { background: var(--critical); box-shadow: 0 0 0 3px rgba(208,59,59,0.25); }

  .stats { display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 12px; margin-bottom: 20px; }
  .tile { background: var(--surface); border: 1px solid var(--border); border-radius: 10px; padding: 16px 18px; border-top: 3px solid var(--accent); }
  .tile.warn { border-top-color: var(--warning); }
  .tile.crit { border-top-color: var(--critical); }
  .tile .label { font-size: 12px; color: var(--ink-muted); text-transform: uppercase; letter-spacing: .04em; margin-bottom: 6px; }
  .tile .value { font-size: 30px; font-weight: 700; font-variant-numeric: tabular-nums; }

  .grid { display: grid; grid-template-columns: 1.6fr 1fr; gap: 16px; align-items: start; }
  @media (max-width: 820px) { .grid { grid-template-columns: 1fr; } }

  .panel { background: var(--surface); border: 1px solid var(--border); border-radius: 10px; overflow: hidden; }
  .panel h2 { font-size: 14px; margin: 0; padding: 14px 16px; border-bottom: 1px solid var(--border); color: var(--ink-2); }
  .panel .body { max-height: 560px; overflow-y: auto; padding: 8px; }
  .panel .empty { color: var(--ink-muted); font-size: 13px; padding: 20px; text-align: center; }

  .item { display: flex; gap: 10px; padding: 9px 10px; border-left: 3px solid var(--ink-muted); border-radius: 6px; margin-bottom: 6px; background: rgba(255,255,255,0.02); font-size: 13px; }
  .item .icon { flex: none; width: 18px; text-align: center; }
  .item .txt { flex: 1; min-width: 0; }
  .item .meta { color: var(--ink-muted); font-size: 11px; margin-top: 2px; }
  .item .time { color: var(--ink-muted); font-size: 11px; flex: none; font-variant-numeric: tabular-nums; }
  .item.received { border-left-color: var(--accent); }
  .item.processed { border-left-color: var(--good); }
  .item.retry { border-left-color: var(--warning); }
  .item.exhausted, .item.dlq { border-left-color: var(--critical); }
  .item.status { border-left-color: var(--grid); color: var(--ink-muted); }

  .dlq-card { border: 1px solid var(--border); border-left: 3px solid var(--critical); border-radius: 8px; padding: 10px 12px; margin-bottom: 8px; font-size: 13px; }
  .dlq-card .row1 { display: flex; justify-content: space-between; font-weight: 600; }
  .dlq-card .reason { color: var(--ink-2); font-size: 12px; margin-top: 4px; }
  .dlq-card .time { color: var(--ink-muted); font-size: 11px; margin-top: 4px; }
</style>
</head>
<body>
<div class="wrap">
  <header>
    <div>
      <h1>📦 Kafka Order Processing — Live Dashboard</h1>
      <p>Avro-serialized orders · real-time aggregation · retry with backoff · dead-letter queue</p>
    </div>
    <div class="status"><span class="dot" id="dot"></span><span id="status-text">Connecting…</span></div>
  </header>

  <div class="stats">
    <div class="tile"><div class="label">Orders Processed</div><div class="value" id="s-count">0</div></div>
    <div class="tile"><div class="label">Running Average</div><div class="value" id="s-avg">₹0.00</div></div>
    <div class="tile warn"><div class="label">Retry Attempts</div><div class="value" id="s-retry">0</div></div>
    <div class="tile crit"><div class="label">Dead-Lettered</div><div class="value" id="s-dlq">0</div></div>
  </div>

  <div class="grid">
    <div class="panel">
      <h2>Live Activity Feed</h2>
      <div class="body" id="feed"><div class="empty">Waiting for orders… start producer.py to see live activity.</div></div>
    </div>
    <div class="panel">
      <h2>Dead Letter Queue</h2>
      <div class="body" id="dlq-panel"><div class="empty">No dead-lettered orders yet.</div></div>
    </div>
  </div>
</div>

<script>
  const feed = document.getElementById('feed');
  const dlqPanel = document.getElementById('dlq-panel');
  const dot = document.getElementById('dot');
  const statusText = document.getElementById('status-text');
  const MAX_FEED_ITEMS = 60;

  function fmtPrice(p) { return '₹' + Number(p).toFixed(2); }

  function addFeedItem(cls, icon, text, meta, time) {
    if (feed.querySelector('.empty')) feed.innerHTML = '';
    const el = document.createElement('div');
    el.className = 'item ' + cls;
    el.innerHTML = `<span class="icon">${icon}</span><span class="txt">${text}${meta ? `<div class="meta">${meta}</div>` : ''}</span><span class="time">${time}</span>`;
    feed.prepend(el);
    while (feed.children.length > MAX_FEED_ITEMS) feed.removeChild(feed.lastChild);
  }

  function addDlqCard(order, reason, time) {
    if (dlqPanel.querySelector('.empty')) dlqPanel.innerHTML = '';
    const el = document.createElement('div');
    el.className = 'dlq-card';
    el.innerHTML = `<div class="row1"><span>#${order.orderId} — ${order.product}</span><span>${fmtPrice(order.price)}</span></div>
                     <div class="reason">${reason}</div><div class="time">failed_at ${time}</div>`;
    dlqPanel.prepend(el);
  }

  function setStats({ total_count, avg_price, retry_count, dlq_count }) {
    if (total_count !== undefined) document.getElementById('s-count').textContent = total_count;
    if (avg_price !== undefined) document.getElementById('s-avg').textContent = fmtPrice(avg_price);
    if (retry_count !== undefined) document.getElementById('s-retry').textContent = retry_count;
    if (dlq_count !== undefined) document.getElementById('s-dlq').textContent = dlq_count;
  }

  function connect() {
    const es = new EventSource('/events');
    es.onopen = () => { dot.className = 'dot live'; statusText.textContent = 'Live'; };
    es.onerror = () => { dot.className = 'dot down'; statusText.textContent = 'Reconnecting…'; };
    es.onmessage = (e) => {
      const ev = JSON.parse(e.data);
      switch (ev.type) {
        case 'snapshot':
          setStats(ev);
          break;
        case 'received':
          addFeedItem('received', '▶', `Received #${ev.order.orderId} — ${ev.order.product} (${fmtPrice(ev.order.price)})`, `offset ${ev.offset}`, ev.ts);
          break;
        case 'processed':
          addFeedItem('processed', '✔', `Processed #${ev.order.orderId} — ${ev.order.product}`, null, ev.ts);
          setStats({ total_count: ev.count, avg_price: ev.avg_price });
          break;
        case 'retry':
          addFeedItem('retry', '↩', `Retry ${ev.attempt}/${ev.max} for #${ev.order.orderId}`, `backoff ${ev.backoff}s`, ev.ts);
          setStats({ retry_count: ev.count });
          break;
        case 'exhausted':
          addFeedItem('exhausted', '✖', `Retries exhausted for #${ev.order.orderId} — routing to DLQ`, null, ev.ts);
          break;
        case 'dlq':
          addFeedItem('dlq', '☠', `#${ev.order.orderId} → orders-dlq`, ev.reason, ev.ts);
          addDlqCard(ev.order, ev.reason, ev.ts);
          setStats({ dlq_count: ev.count });
          break;
        case 'status':
          addFeedItem('status', 'ℹ', ev.message, null, ev.ts);
          break;
      }
    };
  }
  connect();
</script>
</body>
</html>
"""

if __name__ == "__main__":
    threading.Thread(target=consume_loop, daemon=True).start()
    print(f"Dashboard running — open http://localhost:{WEB_PORT}")
    app.run(host="0.0.0.0", port=WEB_PORT, threaded=True, debug=False, use_reloader=False)
