# P1 · Hello Kafka

**Read first:** [Messaging basics](../01-fundamentals/messaging-basics.md) →
[Kafka architecture](../01-fundamentals/kafka-architecture.md) →
[Setup](../01-fundamentals/setup.md)

Your first night with Kafka: a single topic, a producer that makes orders, and a
consumer that reads them — with your own eyes on **partitions and offsets**.

## What you build

```mermaid
flowchart LR
    A["FastAPI producer<br/>POST /orders → produce"] -->|"key=order_id"| K["topic: orders — 3 partitions"]
    K --> C["consumer (console or python)"]
```

| Skill | Learned by |
|-------|-----------|
| Start Kafka via Docker (KRaft) | setup page, docker compose |
| Create topics, inspect partitions/offsets | `kafka-topics.sh`, `kcat -L` |
| Produce with keys | key → partition relationship |
| Consume in a group, see assignment | console consumer, later groups |
| Read lag | `kafka-consumer-groups.sh --describe` |

## Steps

### 1. Stack up

```bash
docker compose up -d          # from the base compose in setup
docker compose ps
```

### 2. Topics

```bash
docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --create \
  --topic orders --partitions 3 --replication-factor 1
```

### 3. Python producer (the skeleton every later project copies)

```python title="produce_orders.py"
from confluent_kafka import Producer

producer = Producer({"bootstrap.servers": "localhost:9092",
                     "acks": "all",                        # see concept: durability
                     "enable.idempotence": True,           # see P10
                     "client.id": "order-producer"})

ORDER_COUNTER = 0

def delivery_cb(err, msg):                    # NEVER skip error callbacks
    if err:
        print(f"DELIVERY FAILED: {err}")
    else:
        print(f"→ {msg.topic()}[{msg.partition()}] @ {msg.offset()}")

def place_order(order_id: str, amount: int):
    payload = f'{{"order_id": "{order_id}", "amount": {amount}}}'
    producer.produce("orders",
                     key=order_id,            # ← the ordering decision
                     value=payload,
                     callback=delivery_cb)
    producer.flush()                          # wait for ack (demo only)

for i in range(20):
    place_order(f"ord-{i:03d}", 100 + i)
```

Run it, watch `delivery_cb` print `partition` and `offset` — that's the ack path.
Then produce 20 messages with `key=None` and see partitions scatter.

### 4. Consume with your eyes

```bash
# console consumer, show key + partition
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic orders --from-beginning \
  --property print.key=true --property print.partition=true

# same, via kcat
kcat -b localhost:9092 -t orders -C -K: -f 'partition=%p offset=%o key=%k value=%s\n'
```

### 5. Consumer group + lag

```python title="consume_orders.py"
from confluent_kafka import Consumer

consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "orders-console-group",
    "auto.offset.reset": "earliest",
    "enable.auto.offset.commit": False,   # discipline from day 1
})
consumer.subscribe(["orders"])

while True:
    msg = consumer.poll(1.0)
    if msg is None:      continue
    if msg.error():      raise Exception(msg.error())
    print(f"group read: {msg.key()} {msg.value()} [p{msg.partition()} o{msg.offset()}]")
    consumer.commit(asynchronous=False)
```

While it runs, check lag from another terminal:

```bash
docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --describe --group orders-console-group
```

Kill the consumer, produce 10 more, restart → it resumes at the committed offset
(never replays, never skips).

## Break it (the real lesson)

1. **Kill the consumer mid-read**, produce 20 orders, restart. Note exactly which
   offsets it starts from. Did you lose/duplicate any? (Answer: late-committed
   records between process and commit may re-read → duplicates possible — write
   this observation down.)
2. **Restart the broker** (`docker compose restart kafka`). Produce + consume again.
   What survives? What didn't (hint: `acks` config, retention)?
3. **Set `acks=0`** and cut the network (or just stop broker). Silent loss or error?
   This is why `acks=all` is the default in this guide.
4. Produce 2 orders with the **same key** and 2 with different keys. Watch their
   partitions and offsets. Re-state the "same key → same partition → ordered" law
   in your own words.

## Checkpoints (write these down)

??? question "1. What exactly does `offset` mean in the delivery callback vs in the consumer?"
    - **Delivery callback**: the *broker-assigned* position of the record **within
      its partition** after acknowledgment (`msg.partition()`, `msg.offset()`).
      The producer learns this only after the ack — the offset is stamped by the
      broker, not by you.
    - **Consumer**: `msg.offset()` is the position in the partition *from which
      the consumer read this record*; the **committed offset** is your group's
      stored position in the broker's `__consumer_offsets` topic — where the
      group will resume after restart/rebalance.
    Same number, two lifetimes: producer-side = "broker accepted it here",
    consumer-side = "we've processed up to here" — only the second is a
    checkpoint.

??? question "2. Why did the same-key records go to one partition and null-key records scatter?"
    Kafka computes partition = `hash(key) % num_partitions` when a key exists —
    deterministically, so the **same key always lands on the same partition**,
    which is what makes per-key ordering possible. With `key=None` the client
    round-robins (or uses sticky partitioning) across all partitions — records
    scatter, so two events of the *same logical entity* can end up on different
    partitions and their relative order is **not preserved** (reading them back is
    a wheel of fortune). That's the P2 ordering law in one paragraph.

??? question "3. `min.insync.replicas` with a single broker: what's possible/not possible here?"
    With **one broker** you can only have `RF=1` and `min.insync.replicas=1` —
    possible only as a *demo*: `acks=all` then means "the single broker acked".
    What's **not possible**: any real ISR-based durability. A single broker is a
    single point of failure — lose it and you lose every topic, every offset, every
    ISR story: there are no other replicas to catch up, no ISR shrinking
    protection (a broker can't stand as its own quorum partner), and "durable" is
    a euphemism.
    The production lesson (P1 → P12): RF ≥ 3, `min.insync.replicas=2`, per-broker
    failure then doesn't burn data — the config you *never* ship to prod is
    exactly this compose's `RF=1`.

??? question "4. Which two configs make your producer reliably durable end-to-end on the broker?"
    - **`acks=all`** — wait for *all* ISR replicas to acknowledge before the
      produce is done → no "leader acked, then lost the data" window.
    - **`enable.idempotence=true`** — sequence numbers make broker-side
      duplicate *impossible* even when retries happen after ambiguous failures
      (the P10 groundwork).
    Together: each produce is *durable* (all replicas) and *duplicate-free*
    (idempotent sequence) — "exactly-once per produce" at the broker.
    (Optional third: `min.insync.replicas=2` at the broker — it converts
    `acks=all` from theory to promise by refusing to lead without a quorum.)

## Done when

- [ ] You can describe what a partition is to someone else without slides
- [ ] You have produced with and without keys and observed the difference
- [ ] You have measured, then explained, consumer lag
- [ ] You killed something and observed exactly one of the failure modes above

Next: **[P2 · Consumer groups & scaling](p02-consumer-groups.md)** — parallelism,
rebalance, and why "5 consumers for 4 partitions" is a trap.