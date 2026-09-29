# P10 · Exactly-Once & Kafka Transactions

**Read first:** [Exactly-once semantics](../02-concepts/exactly-once.md) →
[Delivery semantics](../02-concepts/delivery-semantics.md)

Kafka transactions make produce plus offset commit atomic. They do not span
Postgres or HTTP. This lab proves both halves: atomic batches plus the
fencing and side-effect limits.

## What you build

```mermaid
flowchart LR
    S["source"] -->|"consume"| T["validator.py<br/>produce validated + offsets"]
    T --> V["validated"]
    T --> D["source.dead"]
    V --> O["observer.py"]
```

### How the flow works

There is no Postgres here — that absence is the point. The transaction is Kafka-only: outputs + offsets commit atomically.

1. **`validator.py` batches the source.** Group `validators` consumes up to 50 records from `source` (`e-valid-1` with `amount > 0`, `e-bad-1` with `amount = -5`). `transactional.id = validator-1` is the fencing identity: one live owner per ID, enforced by broker epochs. `init_transactions()` fences any prior owner before the loop starts.
2. **One transaction = outputs + offsets.** `begin_transaction()` → for each record, `produce` to `validated` (key and bytes preserved) or to `source.dead` with `x-error-type = permanent` (so no input offset is ever skipped without an output) → `send_offsets_to_transaction(next-offset per partition)` → `commit_transaction()`. Outputs and the group's resume position become visible together; a crash before commit leaves both invisible and the batch replays whole.
3. **Why `observer.py` runs twice.** `read_committed` hides aborted batches; `read_uncommitted` can show pre-commit or aborted bytes. After a commit both converge — that convergence is what "exactly-once *in Kafka*" means, and why downstream consumers still need their own claim tables (the broker guarantee stops at the log).
4. **`zombie.py` proves fencing.** `elder` begins a transaction, then `successor` calls `init_transactions()` with the same `transactional.id = validator-1` (epoch bump) and commits. The elder's commit now fails with a fencing error: only the current epoch can commit. That is how a restarted validator cannot double-commit its predecessor's batch.
5. **`external_effect.py` proves the boundary.** `send_email_stub("ext-1")` appends to `/tmp/opencode-p10-side-effect.txt` *inside* the transaction window, then `abort_transaction()` rolls back the Kafka produce. The file still contains `email-sent ext-1` while `read_committed` never shows `ext-1`. Kafka aborts do not unsend emails — external effects need their own idempotency keys (back to P3).

| Skill | Learned by |
|-------|-----------|
| Idempotent produce | `enable.idempotence=true`, `acks=all` |
| Atomic produce plus offsets | `validator.py` transaction loop |
| Isolation | `observer.py` with both levels |
| Zombie fencing | `zombie.py`, one `transactional.id` |
| External limit | `external_effect.py`, abort cannot unsend email |

## Topics

```bash
export DATABASE_URL='postgresql://app:app@localhost:5432/app'
export KAFKA_BOOTSTRAP_SERVERS='localhost:9092'
docker compose up -d --wait --wait-timeout 180
for topic in source validated source.dead; do
  docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server kafka:29092 --create --if-not-exists \
    --topic "$topic" --partitions 3 --replication-factor 1 \
    --config min.insync.replicas=1
done
```

## 1. `validator.py`: transactional produce plus offsets

Only offsets for messages handled in this batch are sent. Invalid events go
to `source.dead` inside the same transaction so no input offset is skipped
without an output.

```python title="validator.py"
import json
import os
import time
from confluent_kafka import Consumer, Producer, TopicPartition
from confluent_kafka import KafkaException

SOURCE = "source"
VALIDATED = "validated"
DEAD = "source.dead"
GROUP = "validators"


def validate(event: dict) -> bool:
    # Pure check: envelope present, amount positive; invalid -> DLQ branch.
    if not isinstance(event, dict):
        return False
    if not event.get("event_id") or not event.get("aggregate_id"):
        return False
    data = event.get("data")
    if not isinstance(data, dict):
        return False
    amount = data.get("amount", 0)
    return isinstance(amount, (int, float)) and amount > 0


def main() -> None:
    # Manual-commit consumer: offsets sent via transaction, not auto-commit.
    consumer = Consumer(
        {
            "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
            "group.id": GROUP,
            "auto.offset.reset": "earliest",
            "enable.auto.offset.commit": False,
        }
    )
    # Transactional producer: id + init required before begin/commit.
    producer = Producer(
        {
            "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
            "transactional.id": "validator-1",  # fencing identity; one owner at a time
            "enable.idempotence": True,
            "acks": "all",
        }
    )
    producer.init_transactions(10.0)  # fence old epochs, ready for transactions
    consumer.subscribe([SOURCE])
    try:
        while True:
            messages = consumer.consume(num_messages=50, timeout=2.0)  # batch input
            if not messages:
                continue
            producer.begin_transaction()  # start Kafka-only atomic unit
            try:
                processed: list = []
                for message in messages:
                    if message.error() is not None:
                        raise RuntimeError(str(message.error()))
                    event = json.loads(message.value())
                    if validate(event):
                        # Valid: route to validated, preserve original key/bytes.
                        producer.produce(VALIDATED, key=message.key(), value=message.value())
                    else:
                        # Invalid: route to DLQ in same tx so offset never skips.
                        producer.produce(
                            DEAD,
                            key=message.key(),
                            value=message.value(),
                            headers=[("x-error-type", b"permanent")],
                        )
                    processed.append(message)
                producer.poll(0)  # serve delivery callbacks before commit
                offsets = [
                    TopicPartition(m.topic(), m.partition(), m.offset() + 1)
                    for m in processed  # next offset = consumed + 1
                ]
                producer.send_offsets_to_transaction(
                    offsets, consumer.consumer_group_metadata()  # atomic: outputs + offsets
                )
                producer.commit_transaction()  # outputs + offsets visible together
            except KafkaException as error:
                print(f"transaction aborted: {error}")
                try:
                    producer.abort_transaction()  # aborted batch stays invisible
                except KafkaException as abort_error:
                    print(f"abort failed: {abort_error}")
                time.sleep(1)
            except Exception as error:
                print(f"batch failed: {error}")
                try:
                    producer.abort_transaction()
                except KafkaException as abort_error:
                    print(f"abort failed: {abort_error}")
                time.sleep(1)
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
```

The transaction boundary is Kafka-only: `validated` writes plus the `source`
group offsets. No Postgres table participates. A crash before commit leaves
both invisible; a retry replays the same input batch.

## 2. `observer.py`: both isolation levels

```python title="observer.py"
import argparse
import os
from confluent_kafka import Consumer

TOPIC = "validated"


def main() -> None:
    # Compare isolation levels: same topic, different visibility of aborts.
    parser = argparse.ArgumentParser()
    parser.add_argument("--isolation", choices=["read_committed", "read_uncommitted"], required=True)
    parser.add_argument("--max-messages", type=int, default=100)
    args = parser.parse_args()
    consumer = Consumer(
        {
            "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
            "group.id": f"observer-{args.isolation}",  # separate offset per mode
            "auto.offset.reset": "earliest",
            "enable.auto.offset.commit": False,  # explicit commit after print
            "isolation.level": args.isolation,  # the variable under test
        }
    )
    consumer.subscribe([TOPIC])
    seen = 0
    try:
        while seen < args.max_messages:
            message = consumer.poll(1.0)
            if message is None:
                break  # timeout with no data
            if message.error() is not None:
                raise RuntimeError(str(message.error()))
            print(f"{args.isolation} p{message.partition()} o{message.offset()}")
            consumer.commit(message=message, asynchronous=False)
            seen += 1
    finally:
        consumer.close()
    print(f"observed={seen}")


if __name__ == "__main__":
    main()
```

```bash
python validator.py
python observer.py --isolation read_committed --max-messages 20
python observer.py --isolation read_uncommitted --max-messages 20
```

Expected results: after an aborted batch, `read_committed` count excludes it
while `read_uncommitted` can show pre-commit or aborted bytes. After a commit,
both observers converge to the same committed count.

## 3. `zombie.py`: fencing one identity

```python title="zombie.py"
import os
from confluent_kafka import KafkaException, Producer

BOOTSTRAP = os.environ["KAFKA_BOOTSTRAP_SERVERS"]


def make_producer() -> Producer:
    # Same transactional.id: second init fences the first (epoch bump).
    producer = Producer(
        {
            "bootstrap.servers": BOOTSTRAP,
            "transactional.id": "validator-1",
            "enable.idempotence": True,
            "acks": "all",
        }
    )
    producer.init_transactions(10.0)  # fencing happens here
    return producer


def main() -> None:
    elder = make_producer()
    elder.begin_transaction()
    elder.produce("validated", key=b"z-1", value=b'{"event_id":"z-1"}')
    successor = make_producer()  # fences elder on init
    successor.begin_transaction()
    successor.produce("validated", key=b"z-2", value=b'{"event_id":"z-2"}')
    successor.commit_transaction()  # current epoch wins
    print("successor committed; elder is now fenced")
    try:
        elder.commit_transaction()  # stale epoch must fail
        print("elder commit unexpectedly succeeded")
    except KafkaException as error:
        print(f"elder fenced as expected: {error}")


if __name__ == "__main__":
    main()
```

```bash
python zombie.py
```

Expected result: `successor committed`, then `elder fenced as expected`
with a fencing or epoch error. Only the current epoch for `validator-1`
can commit.

## 4. `external_effect.py`: abort cannot unsend

```python title="external_effect.py"
import json
import os
from confluent_kafka import Producer

MARKER = "/tmp/opencode-p10-side-effect.txt"


def send_email_stub(order_id: str) -> None:
    # Non-transactional side effect: file append cannot be rolled back.
    with open(MARKER, "a", encoding="utf-8") as handle:
        handle.write(f"email-sent {order_id}\n")


def main() -> None:
    if os.path.exists(MARKER):
        os.remove(MARKER)  # clean slate for the demo
    producer = Producer(
        {
            "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
            "transactional.id": "validator-external-demo",
            "enable.idempotence": True,
            "acks": "all",
        }
    )
    producer.init_transactions(10.0)
    producer.begin_transaction()
    producer.produce("validated", key=b"ext-1", value=b'{"event_id":"ext-1"}')
    send_email_stub("ext-1")  # happens outside Kafka tx
    producer.abort_transaction()  # Kafka output rolled back, file is not
    with open(MARKER, encoding="utf-8") as handle:
        body = handle.read()
    print(f"aborted; marker file still contains: {body.strip()}")
    print("Kafka batch invisible; external email not rolled back")


if __name__ == "__main__":
    main()
```

```bash
rm -f /tmp/opencode-p10-side-effect.txt
python external_effect.py
cat /tmp/opencode-p10-side-effect.txt
python observer.py --isolation read_committed --max-messages 5
```

Expected results: the marker file contains `email-sent ext-1` even though the
transaction aborted, and the `read_committed` observer never shows `ext-1`.
Exactly-once stops at the broker; the email needs its own idempotency key.

## 5. Abort visibility check

```bash
python -c "import json, os; from confluent_kafka import Producer; p=Producer({'bootstrap.servers': os.environ['KAFKA_BOOTSTRAP_SERVERS']}); p.produce('source', key=b'o-1', value=json.dumps({'event_id':'e-valid-1','aggregate_id':'o-1','event_type':'OrderPlaced','data':{'amount':10}}).encode()); p.flush(10.0)"
python -c "import json, os; from confluent_kafka import Producer; p=Producer({'bootstrap.servers': os.environ['KAFKA_BOOTSTRAP_SERVERS']}); p.produce('source', key=b'o-2', value=json.dumps({'event_id':'e-bad-1','aggregate_id':'o-2','event_type':'OrderPlaced','data':{'amount':-5}}).encode()); p.flush(10.0)"
sleep 3
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:29092 --topic validated --from-beginning --max-messages 5
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:29092 --topic source.dead --from-beginning --max-messages 5
```

Expected results: `e-valid-1` appears in `validated`, `e-bad-1` appears in
`source.dead`, and killing `validator.py` mid-batch never leaves a partial
batch visible to `read_committed`.

## Checkpoints

??? question "What does the transaction atomically join?"
    Kafka produces plus the consumer group offsets for the handled batch.
    Postgres writes and HTTP calls are outside; they need outbox or keys.

??? question "Why keep idempotency after transactions?"
    Rebalances and crashes still redeliver to downstream consumers. The
    broker batch is atomic, but each consumer still needs its claim table.

## Done when

- [ ] Aborted batches stay invisible to `read_committed`
- [ ] Zombie commit is fenced on shared `transactional.id`
- [ ] External marker survives abort while Kafka output does not
- [ ] Only handled offsets are sent in `send_offsets_to_transaction`

Next: **[P11 · Event-Sourced Bank](p11-event-sourcing.md)**
