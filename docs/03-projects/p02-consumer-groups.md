# P2 · Consumer Groups & Scaling

**Read first:** [Scaling consumers & backpressure](../02-concepts/scaling-consumer-groups.md) →
[Event ordering](../02-concepts/ordering.md)

Add consumers, watch partitions get reassigned, and see why extra members do not
create extra parallelism.

## What you build

```mermaid
flowchart LR
    P["producer.py"] -->|"key=customer_id"| K["orders: 6 partitions"]
    K --> G["group: workers"]
    G --> W1["worker-1"]
    G --> W2["worker-2"]
    G --> W3["worker-3"]
    W1 --> M[("Postgres processed")]
    W2 --> M
    W3 --> M
```

### How the flow works

1. `producer.py` emits an endless stream. Each record has two IDs on purpose: `order_id = ord-{customer}-{ms}` is unique per order (the business fact), while `customer_id` (0–99) is the Kafka key. Keying by customer — not order — is what pins all of one customer's orders to one partition while letting different customers parallelize.
2. `event_id = order-{order_id}-placed` stays stable per order so a producer retry does not look like a new order.
3. Each `worker.py` joins the same group `workers`. Kafka gives each member whole partitions (never half a partition). 1 worker owns all 6; 6 workers own 1 each; the 7th+ worker idles — partitions are the parallelism ceiling.
4. The worker's real work is `INSERT INTO processed … ON CONFLICT (partition_id, record_offset) DO NOTHING`, then a sync commit. `processed` is just an observation log, but its unique key is the lesson: `(partition, offset)` is the record's true identity. A crash between insert and commit replays the same record, and the conflict makes the replay harmless.
5. Query one `record_key` (e.g. `'42'`) ordered by partition + offset: that customer's history is ordered. Compare two different keys and the order interleaves — per-key order, not global order.

## Files and topic

Every file below is complete — no shared helper module. Copy `compose.yaml`
from [Setup](../01-fundamentals/setup.md). Add these project files:

```text
compose.yaml
requirements.txt
schema.sql
producer.py
worker.py
```

P2 uses exactly six partitions. If P1's `orders` topic already exists, reset the
lab first because `--if-not-exists` does not change an existing partition count:

```bash
docker compose up -d --wait --wait-timeout 180
docker compose down -v
docker compose up -d --wait --wait-timeout 180
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:29092 \
  --create --if-not-exists \
  --topic orders --partitions 6 --replication-factor 1 \
  --config min.insync.replicas=1
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:29092 --describe --topic orders
```

Apply the complete worker schema:

```bash
docker compose exec -T postgres psql -v ON_ERROR_STOP=1 -U app -d app < schema.sql
```

Start the load generator in another terminal with `python producer.py`.

## 1. `schema.sql`

```sql title="schema.sql"
-- Observation log: one row per consumed record, for studying assignment.
CREATE TABLE IF NOT EXISTS processed (
  id             BIGSERIAL PRIMARY KEY,
  record_key     TEXT NOT NULL,
  partition_id   INTEGER NOT NULL,
  record_offset  BIGINT NOT NULL,
  worker         TEXT NOT NULL,
  payload        JSONB NOT NULL,
  processed_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  -- Same record redelivered -> same (partition, offset) -> no second row.
  UNIQUE (partition_id, record_offset)
);

-- Fast lookup of one customer's history in partition order.
CREATE INDEX IF NOT EXISTS processed_key_order_idx
  ON processed (record_key, partition_id, record_offset);
```

The unique key makes the observation log safe when a worker crashes after its
insert but before its Kafka commit. A retry does not create a second observation.

## 2. `producer.py`

```python title="producer.py"
import json
import os
import random
import time
from datetime import datetime, timezone
from typing import Any

from confluent_kafka import Producer


def make_event(event_id, event_type, aggregate_type, aggregate_id, data):
    # Canonical envelope; stable event_id survives producer retries.
    return {
        "event_id": event_id,
        "event_type": event_type,
        "aggregate_type": aggregate_type,
        "aggregate_id": aggregate_id,
        "schema_version": 1,
        "occurred_at": datetime.now(timezone.utc).isoformat(),
        "data": data,
    }


def new_producer(client_id: str) -> Producer:
    # Idempotent producer: acks=all + idempotence avoids broker dupes.
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
    # Keyed produce + flush: proves broker ack before continuing.
    errors: list[str] = []

    def delivered(error: object, message: object) -> None:
        if error is not None:
            errors.append(str(error))

    producer.produce(
        topic,
        key=str(event["aggregate_id"]).encode("utf-8"),  # key -> partition
        value=json.dumps(event, sort_keys=True, separators=(",", ":")).encode("utf-8"),
        callback=delivered,
    )
    remaining = producer.flush(15.0)
    if remaining != 0:
        raise TimeoutError(f"{remaining} record(s) undelivered")
    if errors:
        raise RuntimeError(errors[0])
    producer.poll(0)


TOPIC = "orders"


def main() -> None:
    # Endless load: random customer key, unique order per iteration.
    producer = new_producer("p02-load-generator")
    while True:
        customer_id = random.randint(0, 99)
        order_id = f"ord-{customer_id}-{int(time.time() * 1000)}"
        event = make_event(
            event_id=f"order-{order_id}-placed",  # stable per order
            event_type="OrderPlaced",
            aggregate_type="customer",
            aggregate_id=str(customer_id),  # key: one customer -> one partition
            data={
                "order_id": order_id,
                "customer_id": customer_id,
                "amount": 100,
            },
        )
        publish(producer, TOPIC, event)
        time.sleep(0.05)  # throttle load so lag is observable
```

The key is the customer ID, so events for one customer stay on one partition
when the topic mapping is stable. Different customers can be processed in
parallel, but the topic has no global order across their partitions.

## 3. `worker.py`

Start one command per terminal, changing only the worker name:

```bash
python worker.py worker-1
python worker.py worker-2
python worker.py worker-3
```

```python title="worker.py"
import argparse
import json
import os
import time

import psycopg
from confluent_kafka import Consumer, Producer  # Producer unused here; Consumer is the worker
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

TOPIC = "orders"
GROUP = "workers"


def connect_db():
    # Postgres with dict rows; each worker opens its own connection.
    return psycopg.connect(os.environ["DATABASE_URL"], row_factory=dict_row)


def new_consumer(group_id: str) -> Consumer:
    # Group consumer: same group.id shares partitions; manual commit.
    return Consumer(
        {
            "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.offset.commit": False,  # commit only after DB insert
            "max.poll.interval.ms": 60000,  # bound slow processing before eviction
        }
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("worker_name")  # label stored in processed.worker
    args = parser.parse_args()
    consumer = new_consumer(GROUP)
    consumer.subscribe([TOPIC])  # join group; broker assigns partitions
    try:
        while True:
            message = consumer.poll(0.5)  # heartbeat + fetch
            if message is None:
                continue
            error = message.error()
            if error is not None:
                raise RuntimeError(str(error))
            record_key = message.key().decode("utf-8")  # customer_id key
            payload = json.loads(message.value())  # the event envelope
            time.sleep(0.01)  # simulate work; keep under max.poll.interval.ms
            # Local processing step: insert observation row (idempotent).
            with connect_db() as connection:
                with connection.transaction():  # atomic insert unit
                    connection.execute(
                        """
                        INSERT INTO processed
                            (record_key, partition_id, record_offset, worker, payload)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (partition_id, record_offset) DO NOTHING
                        """,
                        (
                            record_key,
                            message.partition(),
                            message.offset(),
                            args.worker_name,
                            Jsonb(payload),  # wrap dict for JSONB column
                        ),
                    )
            # Checkpoint only after DB commit; crash before -> replay, no dupe row.
            consumer.commit(message=message, asynchronous=False)
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
```

Each member owns complete partitions. A member can process only its assigned
records, and a rebalance revokes assignments before giving them to another
member. The database insert is the local processing step; the offset commit is
kept after that transaction.

## 4. Watch the group

Keep this command running while workers join and leave:

```bash
watch -n1 "docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:29092 --describe --group workers"
```

| Members | Expected assignment |
|---------|---------------------|
| 1 | One member owns all six partitions. |
| 2 | The six partitions are split between the two members. |
| 6 | One partition per member. |
| 9 | Six members work; three members are idle. |
| Kill one | A rebalance moves its partitions to surviving members. |

The exact split can differ, but the total number of assigned partitions stays
six. A ninth member cannot receive a seventh partition.

Measure lag with the same command. For a selected customer, the observation
query is:

```sql
SELECT record_key, partition_id, record_offset, processed_at
FROM processed
WHERE record_key = '42'
ORDER BY partition_id, record_offset;
```

Offsets for one key should increase in its partition. Rows for different keys
may be processed at different times, so compare only the key being studied.

## Break it

1. Set `max.poll.interval.ms` to 1000 in a copy of the worker and make its local
   processing take 1.5 seconds. Observe eviction, reassignment, and repeated
   rebalances.
2. Stop the broker and restart it. Check whether the group metadata and committed
   offsets survive this single-node lab.
3. Change the producer key to null. Compare the partition history for one
   customer and explain which ordering guarantee disappeared.
4. Kill a worker after its database transaction but before its commit. Restart
   it and use the unique observation key to show that the retry is harmless.

## Checkpoints

??? question "Why do extra consumers cost anything?"
    A join or departure triggers a rebalance, during which assignments pause and
    move. More members than partitions buy no additional records; the idle
    members still join the group and can increase rebalance work.

??? question "What does a six-partition group support?"
    At most six active partition consumers. A seventh worker is idle for this
    topic. Increasing partitions changes capacity, but existing keys can be
    remapped when the partition count changes.

??? question "What does `max.poll.interval.ms` protect?"
    It bounds how long a member may go without polling. A slow local step can
    exceed the interval, causing eviction and redelivery from the last committed
    offset. Manual commit plus a database uniqueness rule makes that overlap
    safe for this observation log.

??? question "Is one event per key globally ordered?"
    No. A key routes to one partition while the mapping is stable, and that
    partition preserves append order. Other partitions can advance independently.

## Done when

- [ ] You observed one, two, six, and nine group members.
- [ ] You matched an assignment table to the group command.
- [ ] You measured lag while changing the number of members.
- [ ] You explained per-key order without claiming topic-wide order.

Next: **[P3 · Idempotent Payment API](p03-idempotency.md)**.
