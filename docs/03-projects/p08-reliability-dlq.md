# P8 · Retries, Backoff & the Dead Letter Queue

**Read first:** [Failure handling: retries & DLQ](../02-concepts/failure-handling.md)

This lab uses a durable PostgreSQL `retry_jobs` scheduler instead of Kafka time
buckets. A future `run_at` stays in the database until due, so a restart cannot
silently skip it. The source offset is committed only after a retry or DLQ
handoff is acknowledged.

## Exact topics and environment

```bash
export DATABASE_URL='postgresql://app:app@localhost:5432/app'
export KAFKA_BOOTSTRAP_SERVERS='localhost:9092'
docker compose up -d --wait --wait-timeout 180
for topic in orders.events orders.retry orders.dead; do
  docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server kafka:29092 --create --if-not-exists \
    --topic "$topic" --partitions 3 --replication-factor 1 \
    --config min.insync.replicas=1
done
docker compose exec -T postgres psql -v ON_ERROR_STOP=1 -U app -d app < schema.sql
```

`orders.events` is the source and replay target. `orders.retry` is an
acknowledged wake-up and audit stream; the database row is the scheduler. The
retry worker publishes due rows to `orders.events`. `orders.dead` holds
forensics. The bounded budget is three retries after the first failure, then a
DLQ handoff on the fourth failure.

## Failure classes and headers

Confluent headers are `(name, bytes)` pairs, not a string dictionary. This
code preserves original headers, replaces only its own names, and decodes JSON
explicitly from `message.value()`.

| Failure | Action |
|---|---|
| invalid UTF-8, JSON, or envelope | DLQ immediately |
| business rejection | DLQ immediately |
| transient dependency failure | durable retry, then bounded DLQ |
| unhandled exception | transient with the same budget |

A source record is committed only after `publish_raw` receives its delivery
callback and `flush()` returns. The job row is written first, so a crash before
Kafka acknowledgment leaves repairable work and an uncommitted source offset.

## Complete schema

```sql title="schema.sql"
CREATE TABLE IF NOT EXISTS processed_events (
  consumer_group TEXT NOT NULL, event_id TEXT NOT NULL,
  claimed_at TIMESTAMPTZ NOT NULL DEFAULT now(), PRIMARY KEY (consumer_group, event_id)
);
CREATE TABLE IF NOT EXISTS order_effects (
  event_id TEXT PRIMARY KEY, order_id TEXT NOT NULL, payload JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS retry_jobs (
  job_id BIGSERIAL PRIMARY KEY, event_id TEXT NOT NULL, source_topic TEXT NOT NULL,
  message_key TEXT, raw_value TEXT NOT NULL, headers JSONB NOT NULL,
  attempt INTEGER NOT NULL CHECK (attempt > 0), run_at TIMESTAMPTZ NOT NULL,
  state TEXT NOT NULL CHECK (state IN ('pending', 'published')),
  notified_at TIMESTAMPTZ, published_at TIMESTAMPTZ, last_error TEXT NOT NULL,
  UNIQUE (event_id, attempt)
);
CREATE INDEX IF NOT EXISTS retry_jobs_due_idx ON retry_jobs (run_at, job_id) WHERE state = 'pending';
CREATE TABLE IF NOT EXISTS dead_letters (
  dead_id BIGSERIAL PRIMARY KEY, event_id TEXT NOT NULL, source_topic TEXT NOT NULL,
  source_partition INTEGER NOT NULL, source_offset BIGINT NOT NULL, message_key TEXT,
  raw_value TEXT NOT NULL, payload JSONB, headers JSONB NOT NULL,
  error_class TEXT NOT NULL, error_detail TEXT NOT NULL, attempts INTEGER NOT NULL CHECK (attempts > 0),
  published_at TIMESTAMPTZ, replayed_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (source_topic, source_partition, source_offset, error_class)
);
```

`retry_jobs` is authoritative for scheduling. `dead_letters` is the durable
index for the Kafka DLQ, so a crash between audit insert and DLQ publish is
repairable without a consumer offset.

## Complete reliability worker and CLI

```python title="reliability.py"
import argparse
import json
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import uuid4
from confluent_kafka import Consumer, Producer
import common

SOURCE_TOPIC = "orders.events"
RETRY_TOPIC = "orders.retry"
DEAD_TOPIC = "orders.dead"
GROUP = "orders-reliability"
MAX_ATTEMPTS = 4
BACKOFF_SECONDS = (1, 5, 15)

class TransientFailure(RuntimeError):
    pass

class PermanentFailure(RuntimeError):
    pass

def header_text(message: Any, name: str, default: str | None = None) -> str | None:
    for key, value in message.headers() or []:
        if key == name:
            return (value or b"").decode("utf-8", errors="replace")
    return default

def headers_as_json(message: Any) -> list[list[str]]:
    return [[key, (value or b"").decode("utf-8", errors="replace")] for key, value in message.headers() or []]

def replace_headers(headers: list[list[str]], additions: dict[str, str]) -> list[tuple[str, bytes]]:
    names = set(additions)
    result = [(key, value.encode("utf-8")) for key, value in headers if key not in names]
    result.extend((key, value.encode("utf-8")) for key, value in additions.items())
    return result

def key_bytes(message: Any) -> bytes | None:
    return message.key()

def key_text(message: Any) -> str | None:
    key = message.key()
    return key.decode("utf-8", errors="replace") if key is not None else None

def value_bytes(message: Any) -> bytes:
    return message.value() or b""

def publish_raw(producer: Producer, topic: str, key: bytes | None, value: bytes, headers: list[tuple[str, bytes]]) -> None:
    errors: list[str] = []
    def delivered(error: object, message: object) -> None:
        if error is not None:
            errors.append(str(error))
    producer.produce(topic, key=key, value=value, headers=headers, callback=delivered)
    remaining = producer.flush(15.0)
    if remaining != 0:
        raise TimeoutError(f"{remaining} Kafka record(s) remain undelivered")
    if errors:
        raise RuntimeError(errors[0])
    producer.poll(0)

def decode_event(message: Any) -> dict[str, Any]:
    try:
        event = json.loads(value_bytes(message).decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise PermanentFailure(f"invalid-json: {error}") from error
    if not isinstance(event, dict):
        raise PermanentFailure("event must be a JSON object")
    for field in ("event_id", "aggregate_id", "event_type"):
        if not isinstance(event.get(field), str) or not event[field]:
            raise PermanentFailure(f"{field} is required")
    return event

def process_event(event: dict[str, Any]) -> None:
    with common.connect() as connection:
        with connection.transaction():
            claimed = connection.execute(
                "INSERT INTO processed_events (consumer_group, event_id) VALUES (%s, %s) ON CONFLICT (consumer_group, event_id) DO NOTHING RETURNING event_id",
                (GROUP, event["event_id"]),
            ).fetchone()
            if claimed is None:
                return
            data = event.get("data")
            if not isinstance(data, dict):
                raise PermanentFailure("data must be an object")
            mode = data.get("fail_mode")
            if mode == "transient":
                raise TransientFailure("injected transient failure")
            if mode == "permanent":
                raise PermanentFailure("injected permanent failure")
            connection.execute(
                "INSERT INTO order_effects (event_id, order_id, payload) VALUES (%s, %s, %s)",
                (event["event_id"], event["aggregate_id"], common.jsonb(event)),
            )

def schedule_retry(message: Any, event: dict[str, Any], error: Exception, producer: Producer) -> None:
    attempt = int(header_text(message, "x-attempts", "0") or 0) + 1
    if attempt >= MAX_ATTEMPTS:
        dead_letter(message, event, "exhausted", str(error), attempt, producer)
        return
    run_at = datetime.now(timezone.utc) + timedelta(seconds=BACKOFF_SECONDS[attempt - 1])
    raw = value_bytes(message)
    headers = headers_as_json(message)
    with common.connect() as connection:
        with connection.transaction():
            job = connection.execute(
                """
                INSERT INTO retry_jobs (event_id, source_topic, message_key, raw_value, headers, attempt, run_at, state, last_error)
                VALUES (%s, %s, %s, %s, %s, %s, %s, 'pending', %s)
                ON CONFLICT (event_id, attempt) DO UPDATE SET last_error = EXCLUDED.last_error
                RETURNING job_id, notified_at
                """,
                (event["event_id"], SOURCE_TOPIC, key_text(message), raw.decode("utf-8", errors="replace"), common.jsonb(headers), attempt, run_at, str(error)),
            ).fetchone()
    if job["notified_at"] is not None:
        return
    wake_headers = replace_headers(headers, {"x-job-id": str(job["job_id"]), "x-attempts": str(attempt), "x-run-at": run_at.isoformat(), "x-error-class": "transient"})
    publish_raw(producer, RETRY_TOPIC, key_bytes(message), raw, wake_headers)
    with common.connect() as connection:
        with connection.transaction():
            connection.execute("UPDATE retry_jobs SET notified_at = now() WHERE job_id = %s", (job["job_id"],))

def dead_letter(message: Any, event: dict[str, Any] | None, error_class: str, detail: str, attempts: int, producer: Producer) -> None:
    raw = value_bytes(message)
    partition = message.partition()
    offset = message.offset()
    event_id = event["event_id"] if event is not None else f"invalid-{message.topic()}-{partition}-{offset}"
    headers = replace_headers(headers_as_json(message), {"x-error-type": error_class, "x-attempts": str(attempts), "x-error-detail": detail, "x-source-partition-offset": f"{partition}/{offset}"})
    stored_headers = [[name, value.decode("utf-8", errors="replace")] for name, value in headers]
    with common.connect() as connection:
        with connection.transaction():
            row = connection.execute(
                """
                INSERT INTO dead_letters (event_id, source_topic, source_partition, source_offset, message_key, raw_value, payload, headers, error_class, error_detail, attempts)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (source_topic, source_partition, source_offset, error_class) DO UPDATE SET error_detail = EXCLUDED.error_detail
                RETURNING dead_id, published_at
                """,
                (event_id, SOURCE_TOPIC, partition, offset, key_text(message), raw.decode("utf-8", errors="replace"), common.jsonb(event) if event is not None else None, common.jsonb(stored_headers), error_class, detail, attempts),
            ).fetchone()
    if row["published_at"] is not None:
        return
    publish_raw(producer, DEAD_TOPIC, key_bytes(message), raw, headers)
    with common.connect() as connection:
        with connection.transaction():
            connection.execute("UPDATE dead_letters SET published_at = now() WHERE dead_id = %s", (row["dead_id"],))

def retry_once(producer: Producer) -> bool:
    with common.connect() as connection:
        with connection.transaction():
            job = connection.execute(
                "SELECT job_id, event_id, message_key, raw_value, headers, attempt FROM retry_jobs WHERE state = 'pending' AND notified_at IS NOT NULL AND run_at <= now() ORDER BY run_at, job_id LIMIT 1 FOR UPDATE SKIP LOCKED"
            ).fetchone()
            if job is None:
                return False
            headers = replace_headers([[key, value] for key, value in job["headers"]], {"x-retry-of": job["event_id"], "x-job-id": str(job["job_id"]), "x-attempts": str(job["attempt"])})
            publish_raw(producer, SOURCE_TOPIC, job["message_key"].encode("utf-8") if job["message_key"] is not None else None, job["raw_value"].encode("utf-8"), headers)
            connection.execute("UPDATE retry_jobs SET state = 'published', published_at = now() WHERE job_id = %s", (job["job_id"],))
            return True

def retry_worker(once: bool) -> None:
    producer = common.new_producer("p08-retry-worker")
    while True:
        try:
            changed = retry_once(producer)
        except Exception as error:
            print(f"retry worker failed: {error}")
            if once:
                raise
            time.sleep(1)
        else:
            if once:
                return
            time.sleep(0.5 if not changed else 0)

def consume() -> None:
    producer = common.new_producer("p08-router")
    consumer = Consumer({"bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"], "group.id": GROUP, "auto.offset.reset": "earliest", "enable.auto.offset.commit": False})
    consumer.subscribe([SOURCE_TOPIC])
    try:
        while True:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error() is not None:
                raise RuntimeError(str(message.error()))
            event: dict[str, Any] | None = None
            try:
                event = decode_event(message)
                process_event(event)
            except PermanentFailure as error:
                dead_letter(message, event, "invalid" if event is None else "permanent", str(error), 1, producer)
            except TransientFailure as error:
                schedule_retry(message, event, error, producer)
            except Exception as error:
                schedule_retry(message, event, TransientFailure(str(error)), producer)
            consumer.commit(message=message, asynchronous=False)
    finally:
        consumer.close()

def produce(failure_mode: str, event_id: str) -> None:
    producer = common.new_producer("p08-producer")
    event = common.make_event(event_id, "OrderReceived", "order", event_id, {"order_id": event_id, "fail_mode": failure_mode})
    if failure_mode == "malformed":
        publish_raw(producer, SOURCE_TOPIC, event_id.encode("utf-8"), b'{"event_id":', [])
    else:
        common.publish(producer, SOURCE_TOPIC, event)
    print(event_id)

def replay(event_id: str | None, clear_failure: bool) -> None:
    producer = common.new_producer("p08-replay")
    with common.connect() as connection:
        with connection.transaction():
            rows = connection.execute(
                "SELECT dead_id, event_id, message_key, raw_value, headers FROM dead_letters WHERE published_at IS NOT NULL AND replayed_at IS NULL AND (%s::text IS NULL OR event_id = %s) ORDER BY dead_id LIMIT 100 FOR UPDATE SKIP LOCKED",
                (event_id, event_id),
            ).fetchall()
            for row in rows:
                value = row["raw_value"].encode("utf-8")
                if clear_failure:
                    event = json.loads(value)
                    event["data"]["fail_mode"] = "none"
                    value = json.dumps(event, separators=(",", ":")).encode("utf-8")
                headers = replace_headers([[key, header] for key, header in row["headers"]], {"x-replay": "true", "x-original-event-id": row["event_id"]})
                publish_raw(producer, SOURCE_TOPIC, row["message_key"].encode("utf-8") if row["message_key"] is not None else None, value, headers)
                connection.execute("UPDATE dead_letters SET replayed_at = now() WHERE dead_id = %s", (row["dead_id"],))
    print(f"replayed {len(rows)} record(s)")

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=["produce", "consume", "retry", "replay"])
    parser.add_argument("--event-id")
    parser.add_argument("--failure-mode", default="none")
    parser.add_argument("--clear-failure", action="store_true")
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()
    event_id = args.event_id or str(uuid4())
    if args.command == "produce":
        produce(args.failure_mode, event_id)
    elif args.command == "consume":
        consume()
    elif args.command == "retry":
        retry_worker(args.once)
    else:
        replay(args.event_id, args.clear_failure)

if __name__ == "__main__":
    main()
```

The retry worker is a PostgreSQL scheduler, not a future-bucket consumer. It
publishes only due rows; a future row is never committed away from Kafka. The
`orders.retry` record is a durable notification, while PostgreSQL remains the
source of due-time state.

## Failure injection and SQL assertions

Start the consumer and retry worker in separate terminals:

```bash
python reliability.py consume
python reliability.py retry
```

A valid event demonstrates the idempotent effect:

```bash
python reliability.py produce --event-id e-ok --failure-mode none
sleep 1
docker compose exec -T postgres psql -U app -d app -c \
  "SELECT event_id, count(*) FROM order_effects WHERE event_id = 'e-ok' GROUP BY event_id;"
```

A transient failure creates bounded durable jobs. The injected mode remains
active so the retries demonstrate exhaustion:

```bash
python reliability.py produce --event-id e-transient --failure-mode transient
sleep 25
docker compose exec -T postgres psql -U app -d app -c \
  "SELECT event_id, attempt, state, run_at, notified_at, published_at, last_error
   FROM retry_jobs WHERE event_id = 'e-transient' ORDER BY attempt;"
docker compose exec -T postgres psql -U app -d app -c \
  "SELECT event_id, error_class, attempts, published_at
   FROM dead_letters WHERE event_id = 'e-transient';"
```

The first query has attempts 1, 2, and 3, all `published`; the second has one
`exhausted` row with `attempts = 4` and a non-null `published_at`. Malformed
input skips the retry budget:

```bash
python reliability.py produce --event-id e-malformed --failure-mode malformed
sleep 1
docker compose exec -T postgres psql -U app -d app -c \
  "SELECT event_id, error_class, attempts, error_detail
   FROM dead_letters WHERE event_id LIKE 'invalid-%' ORDER BY dead_id DESC LIMIT 1;"
```

Replay is database-backed. It republishes the original value and marks
`replayed_at` only after Kafka acknowledgment. `--clear-failure` is a lab-only
repair of the injected mode:

```bash
python reliability.py produce --event-id e-permanent --failure-mode permanent
sleep 1
python reliability.py replay --event-id e-permanent --clear-failure
sleep 1
docker compose exec -T postgres psql -U app -d app -c \
  "SELECT event_id, error_class, replayed_at FROM dead_letters WHERE event_id = 'e-permanent';"
docker compose exec -T postgres psql -U app -d app -c \
  "SELECT event_id, count(*) FROM order_effects WHERE event_id = 'e-permanent' GROUP BY event_id;"
```

## Remaining dual-write gap

The job insert and Kafka wake-up remain two systems, not one distributed
transaction. PostgreSQL is durable before Kafka, the source offset waits for
acknowledgment, and a crash in the gap is repaired by the scheduler or Kafka
redelivery. A crash after acknowledgment but before marking the row can
publish a duplicate; `processed_events` and the effect key absorb it. There is
no exactly-once transaction across PostgreSQL and Kafka.

Next: **[P9 · Schema Registry & Evolution](p09-schema-registry.md)**.
