# P1 · Hello Kafka

**Read first:** [Messaging basics](../01-fundamentals/messaging-basics.md) →
[Kafka architecture](../01-fundamentals/kafka-architecture.md) →
[Setup](../01-fundamentals/setup.md)

A single topic, a keyed producer, and a consumer you can watch move records
between partitions and offsets.

## What you build

```mermaid
flowchart LR
    P["producer.py"] -->|"key=order_id"| K["orders: 3 partitions"]
    K --> C["consumer.py"]
```

### How the flow works

1. `producer.py` loops 20 times. Each iteration picks `order_id = ord-000 … ord-019` — a fake business key so you can see key → partition routing.
2. It builds an event with `event_id = order-{order_id}-placed`. The `event_id` is stable (derived from the order, not random) so a retry would carry the same identity. `aggregate_id = order_id` doubles as the Kafka record key.
3. `publish()` sends with `key=aggregate_id`, then `flush()` blocks until the broker acks and assigns partition + offset. That ack is the only proof the write landed.
4. `consumer.py` joins group `orders-console-group`, subscribes to `orders`, then `poll → print → commit`. The print is the stand-in for real work; the sync commit after it is the restart checkpoint.
5. Kill and restart the consumer: it resumes from the last committed offset, not from zero. Records after that commit replay — that replay is at-least-once in miniature.

## Files and setup

Every file below is complete and self-contained — no shared helper module.
Copy `compose.yaml` from [Setup](../01-fundamentals/setup.md). The project files are:

```text
compose.yaml
requirements.txt
producer.py
consumer.py
```

Start the stack and create the topic explicitly:

```bash
docker compose up -d --wait --wait-timeout 180
docker compose ps
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:29092 \
  --create --if-not-exists \
  --topic orders --partitions 3 --replication-factor 1 \
  --config min.insync.replicas=1
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:29092 --describe --topic orders
```

Start the producer from this project root:

```bash
python producer.py
```

## 1. `producer.py`

```python title="producer.py"
import json
import os
from datetime import datetime, timezone
from typing import Any

from confluent_kafka import Producer


def make_event(event_id, event_type, aggregate_type, aggregate_id, data):
    # Build canonical envelope; stable event_id enables later dedup.
    return {
        "event_id": event_id,
        "event_type": event_type,
        "aggregate_type": aggregate_type,
        "aggregate_id": aggregate_id,
        "schema_version": 1,  # envelope version, not business version
        "occurred_at": datetime.now(timezone.utc).isoformat(),  # UTC event time
        "data": data,
    }


def new_producer(client_id: str) -> Producer:
    # Idempotent producer: broker dedups retries, acks=all waits for replicas.
    return Producer(
        {
            "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
            "client.id": client_id,
            "acks": "all",
            "enable.idempotence": True,
            "delivery.timeout.ms": 30000,
        }
    )


def publish(producer: Producer, topic: str, event: dict[str, Any]) -> None:
    # Produce one keyed record and block until the broker acknowledges it.
    errors: list[str] = []

    def delivered(error: object, message: object) -> None:
        # Async delivery report; collect errors to raise after flush.
        if error is not None:
            errors.append(str(error))

    producer.produce(
        topic,
        key=str(event["aggregate_id"]).encode("utf-8"),  # key controls partition
        value=json.dumps(event, sort_keys=True, separators=(",", ":")).encode("utf-8"),
        callback=delivered,
    )
    remaining = producer.flush(15.0)  # wait for ack; produce() alone is async
    if remaining != 0:
        raise TimeoutError(f"{remaining} record(s) undelivered")
    if errors:
        raise RuntimeError(errors[0])
    producer.poll(0)  # serve leftover callbacks


TOPIC = "orders"


def main() -> None:
    # One producer process emits 20 keyed OrderPlaced events.
    producer = new_producer("p01-order-producer")
    for number in range(20):
        order_id = f"ord-{number:03d}"
        event = make_event(
            event_id=f"order-{order_id}-placed",  # stable id: retry-safe
            event_type="OrderPlaced",
            aggregate_type="order",
            aggregate_id=order_id,  # Kafka key: same order -> same partition
            data={"order_id": order_id, "amount": 100 + number},
        )
        publish(producer, TOPIC, event)


if __name__ == "__main__":
    main()
```

Each `produce` waits for its delivery callback to report success. The callback
receives the broker-assigned partition and offset; a timeout or delivery error
raises instead of being silently ignored.

## 2. `consumer.py`

Start it with `python consumer.py` in another terminal. Run this:

```python title="consumer.py"
import os

from confluent_kafka import Consumer

TOPIC = "orders"
GROUP = "orders-console-group"


def new_consumer(group_id: str) -> Consumer:
    # Manual-commit consumer: offset moves only after local processing.
    return Consumer(
        {
            "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
            "group.id": group_id,  # offset + rebalance scope
            "auto.offset.reset": "earliest",  # no commit yet -> read from start
            "enable.auto.offset.commit": False,  # app commits explicitly below
        }
    )


def main() -> None:
    consumer = new_consumer(GROUP)
    consumer.subscribe([TOPIC])  # join group, get partition assignment
    try:
        while True:
            message = consumer.poll(1.0)  # heartbeat + fetch; None on timeout
            if message is None:
                continue
            error = message.error()
            if error is not None:
                raise RuntimeError(str(error))  # broker-level failure
            key = (message.key() or b"").decode("utf-8")
            value = (message.value() or b"").decode("utf-8")
            # Local processing step: print. Commit only after it succeeds.
            print(
                f"p{message.partition()} o{message.offset()} "
                f"key={key} value={value}"
            )
            consumer.commit(message=message, asynchronous=False)  # sync checkpoint
    finally:
        consumer.close()  # leave group cleanly, revoke partitions


if __name__ == "__main__":
    main()
```

The consumer commits only after its local processing step, which here is the
print. A crash between the side effect and the commit can replay a record.

## 3. Consume without the Python loop

Use this when you want to see keys and partitions directly:

```bash
docker compose exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:29092 \
  --topic orders --from-beginning \
  --property print.key=true \
  --property print.partition=true \
  --property print.offset=true
```

While the Python consumer is stopped or running, inspect the group and its lag:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka:29092 \
  --describe --group orders-console-group
```

The command shows the current assignment, committed offset, log end offset, and
lag for each assigned partition. Produce more records, then run the command again
to watch lag rise or fall.

## Ordering lesson

With a fixed topic partition count and partitioner, the same non-null key maps
deterministically to the same partition. Records appended to one partition keep
their partition order, so events for one order are ordered relative to one
another. This is **per-key ordering**, not a total order across the topic: records
with different keys can be interleaved across partitions. Changing partition
count or partitioner can remap a key, and a null key does not provide entity
ordering.

The offset in a delivery callback is the broker position assigned to the record
after acknowledgment. The consumer's offset identifies the record it read; the
group's committed offset is the restart checkpoint and advances only after the
local processing step.

## Break it

1. Stop the consumer after it reads a record but before it commits, produce more
   records, and restart it. Which records replay, and why can a late commit cause
   duplicates?
2. Run `docker compose restart kafka`, then produce and consume again. Which data
   survives in this single-broker lab?
3. Change a copy of the producer to use a null key. Compare partitions and explain
   why two events for one logical entity can now be observed out of order.
4. Create two records with the same key and two with different keys. Record each
   partition and offset, then restate the ordering guarantee without saying that
   Kafka orders the whole topic.

## Checkpoints

??? question "What does the delivery callback prove?"
    A broker acknowledged the record and assigned its partition and offset. It
    does not prove that a consumer committed its local processing.

??? question "What does a group commit do?"
    It stores the next position from which the group can resume. Manual commit
    after processing provides at-least-once behavior: a crash before commit can
    replay, but a successful commit is not an early side-effect checkpoint.

??? question "Does a key provide global ordering?"
    No. It provides deterministic routing and per-partition order for that key
    while the topic mapping is stable. Different partitions have independent logs.

??? question "What is lag?"
    For each assigned partition it is the difference between the log end offset and
    the group's committed position. More consumers help only when idle partitions
    exist and their processing is faster than the producer.

## Done when

- [ ] You produced keyed records and observed partitions and offsets.
- [ ] You inspected a group assignment and measured lag.
- [ ] You can explain the difference between acknowledgment and commit.
- [ ] You can state the per-key ordering rule and its limits.

Next: **[P2 · Consumer groups & scaling](p02-consumer-groups.md)**.
