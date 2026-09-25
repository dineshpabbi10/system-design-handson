# P9 · Schema Registry & Evolution

**Read first:** [Schema evolution & registry](../02-concepts/schema-evolution.md)

One topic, one contract: `OrderPlaced` on `orders.events`. Avro plus the
registry makes the payload a checked contract. You register v1, produce and
consume through serde, add one compatible field, reject one breaking change,
and route corrupt bytes to a DLQ.

## What you build

```mermaid
flowchart LR
    P["producer.py (Avro)"] -->|"register + serialize"| R["Registry :8081"]
    P -->|"Avro bytes"| K["orders.events"]
    C["consumer.py (Avro)"] -->|"fetch id"| R
    K --> C
    C -->|"invalid bytes"| D["orders.dead"]
```

| Skill | Learned by |
|-------|-----------|
| Avro value + optional key schema | `order-value.avsc`, `order-key.avsc` |
| Registry governance | `curl` register, config, compatibility |
| Compatible evolution | v2 optional field, `is_compatible: true` |
| Incompatible rejection | v3 breaking change, `is_compatible: false` |
| Contract breach handling | DLQ with `x-error-type=invalid` |

## Shared files and setup

Copy `compose.yaml` and `common.py` unchanged from
[Setup](../01-fundamentals/setup.md). Add:

```text
compose.yaml
compose.registry.yaml
common.py
requirements.txt
schema.sql
order-value.avsc
order-key.avsc
producer.py
consumer.py
```

Save this merge file as `compose.registry.yaml`. It adds only the registry; the
base services stay untouched in `compose.yaml`.

Replace `requirements.txt` with these complete project dependencies:

```text title="requirements.txt"
confluent-kafka[schema-registry,avro]
psycopg[binary]
```

```bash
python -m pip install -r requirements.txt
```

```yaml title="compose.registry.yaml"
services:
  schema-registry:
    image: confluentinc/cp-schema-registry:7.7.0
    depends_on:
      - kafka
    environment:
      SCHEMA_REGISTRY_HOST_NAME: schema-registry
      SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS: kafka:29092
      SCHEMA_REGISTRY_KAFKASTORE_TOPIC: _schemas
    ports:
      - "127.0.0.1:8081:8081"
```

```bash
export DATABASE_URL='postgresql://app:app@localhost:5432/app'
export KAFKA_BOOTSTRAP_SERVERS='localhost:9092'
docker compose -f compose.yaml -f compose.registry.yaml up -d --wait --wait-timeout 180
for topic in orders.events orders.dead; do
  docker compose -f compose.yaml -f compose.registry.yaml exec kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server kafka:29092 --create --if-not-exists \
    --topic "$topic" --partitions 3 --replication-factor 1 \
    --config min.insync.replicas=1
done
docker compose -f compose.yaml -f compose.registry.yaml exec -T postgres psql -v ON_ERROR_STOP=1 -U app -d app < schema.sql
```

## 1. `order-value.avsc` and `order-key.avsc`

The value carries the event identity. The key schema is optional and kept for
governance; this lab keys by the UTF-8 `aggregate_id` string.

```json title="order-value.avsc"
{
  "type": "record",
  "name": "OrderPlaced",
  "namespace": "com.shop",
  "fields": [
    {"name": "event_id", "type": "string"},
    {"name": "aggregate_id", "type": "string"},
    {"name": "customer_id", "type": "string"},
    {"name": "sku", "type": "string"},
    {"name": "quantity", "type": "int"},
    {"name": "amount_cents", "type": "long"},
    {"name": "currency", "type": "string", "default": "USD"},
    {"name": "occurred_at", "type": "string"}
  ]
}
```

```json title="order-key.avsc"
{
  "type": "record",
  "name": "OrderKey",
  "namespace": "com.shop",
  "fields": [
    {"name": "order_id", "type": "string"}
  ]
}
```

```sql title="schema.sql"
CREATE TABLE IF NOT EXISTS processed_events (
  consumer_group TEXT NOT NULL,
  event_id TEXT NOT NULL,
  claimed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (consumer_group, event_id)
);
```

## 2. Register and govern

```bash
curl -s -X POST http://localhost:8081/subjects/orders.events-value/versions \
  -H 'Content-Type: application/vnd.schemaregistry.v1+json' \
  -d '{"schema":"{\"type\":\"record\",\"name\":\"OrderPlaced\",\"namespace\":\"com.shop\",\"fields\":[{\"name\":\"event_id\",\"type\":\"string\"},{\"name\":\"aggregate_id\",\"type\":\"string\"},{\"name\":\"customer_id\",\"type\":\"string\"},{\"name\":\"sku\",\"type\":\"string\"},{\"name\":\"quantity\",\"type\":\"int\"},{\"name\":\"amount_cents\",\"type\":\"long\"},{\"name\":\"currency\",\"type\":\"string\",\"default\":\"USD\"},{\"name\":\"occurred_at\",\"type\":\"string\"}]}"}'
curl -s http://localhost:8081/subjects/orders.events-value/versions/1
curl -s -X PUT http://localhost:8081/config/orders.events-value \
  -H 'Content-Type: application/vnd.schemaregistry.v1+json' \
  -d '{"compatibility":"BACKWARD"}'
curl -s http://localhost:8081/config/orders.events-value
```

Expected results: the first POST returns `{"id":1}`, the version GET returns
the v1 schema, and the config GET returns `{"compatibility":"BACKWARD"}`.

## 3. `producer.py`

```python title="producer.py"
import uuid
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext
import common

TOPIC = "orders.events"


def event_to_dict(event: dict, ctx: object) -> dict:
    return event


def main() -> None:
    with open("order-value.avsc", encoding="utf-8") as handle:
        value_schema = handle.read()
    registry = SchemaRegistryClient({"url": "http://localhost:8081"})
    serializer = AvroSerializer(registry, value_schema, event_to_dict)
    producer = common.new_producer("p09-orders-producer")
    order_id = str(uuid.uuid4())
    event = {
        "event_id": f"order-{order_id}-placed",
        "aggregate_id": order_id,
        "customer_id": "c-1",
        "sku": "sku-1",
        "quantity": 2,
        "amount_cents": 2500,
        "currency": "USD",
        "occurred_at": "2026-01-01T00:00:00+00:00",
    }
    context = SerializationContext(TOPIC, MessageField.VALUE)
    value = serializer(event, context)
    errors: list[str] = []

    def delivered(error: object, message: object) -> None:
        if error is not None:
            errors.append(str(error))

    producer.produce(
        TOPIC,
        key=order_id.encode("utf-8"),
        value=value,
        callback=delivered,
    )
    remaining = producer.flush(15.0)
    if remaining != 0:
        raise TimeoutError(f"{remaining} Kafka record(s) remain undelivered")
    if errors:
        raise RuntimeError(errors[0])
    producer.poll(0)
    print(order_id)


if __name__ == "__main__":
    main()
```

The serializer writes the wire header. The key stays the `aggregate_id`
string so per-order ordering holds. `event_id` is stable for dedup.

## 4. `consumer.py` with invalid-byte DLQ

```python title="consumer.py"
import os
from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext
import common

TOPIC = "orders.events"
DEAD_TOPIC = "orders.dead"
GROUP = "orders-avro-inventory"


def dict_from_avro(obj: dict | None, ctx: object) -> dict | None:
    return obj


def send_dead(producer: object, key: bytes | None, value: bytes) -> None:
    errors: list[str] = []

    def delivered(error: object, message: object) -> None:
        if error is not None:
            errors.append(str(error))

    typed_producer = producer
    typed_producer.produce(
        DEAD_TOPIC,
        key=key,
        value=value,
        headers=[("x-error-type", b"invalid"), ("x-error-detail", b"avro-decode-failed")],
        callback=delivered,
    )
    remaining = typed_producer.flush(15.0)
    if remaining != 0:
        raise TimeoutError(f"{remaining} Kafka record(s) remain undelivered")
    if errors:
        raise RuntimeError(errors[0])
    typed_producer.poll(0)


def main() -> None:
    with open("order-value.avsc", encoding="utf-8") as handle:
        value_schema = handle.read()
    registry = SchemaRegistryClient({"url": "http://localhost:8081"})
    deserializer = AvroDeserializer(registry, value_schema, dict_from_avro)
    consumer = Consumer(
        {
            "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
            "group.id": GROUP,
            "auto.offset.reset": "earliest",
            "enable.auto.offset.commit": False,
        }
    )
    producer = common.new_producer("p09-dlq-producer")
    consumer.subscribe([TOPIC])
    try:
        while True:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error() is not None:
                raise RuntimeError(str(message.error()))
            try:
                context = SerializationContext(message.topic(), MessageField.VALUE)
                event = deserializer(message.value(), context)
            except Exception:
                raw = message.value() or b""
                send_dead(producer, message.key(), raw)
                consumer.commit(message=message, asynchronous=False)
                continue
            if event is None:
                consumer.commit(message=message, asynchronous=False)
                continue
            if not event.get("event_id") or not event.get("aggregate_id"):
                raw = message.value() or b""
                send_dead(producer, message.key(), raw)
                consumer.commit(message=message, asynchronous=False)
                continue
            with common.connect() as connection:
                with connection.transaction():
                    claimed = connection.execute(
                        "INSERT INTO processed_events (consumer_group, event_id) "
                        "VALUES (%s, %s) ON CONFLICT (consumer_group, event_id) "
                        "DO NOTHING RETURNING event_id",
                        (GROUP, event["event_id"]),
                    ).fetchone()
                    if claimed is None:
                        consumer.commit(message=message, asynchronous=False)
                        continue
            consumer.commit(message=message, asynchronous=False)
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
```

Decode failure is a contract breach, not a retry: DLQ immediately, then commit
the source offset so the partition keeps moving.

## 5. One compatible addition, one rejection

Compatible v2 adds an optional field with a default:

```bash
curl -s -X POST http://localhost:8081/compatibility/subjects/orders.events-value/versions/latest \
  -H 'Content-Type: application/vnd.schemaregistry.v1+json' \
  -d '{"schema":"{\"type\":\"record\",\"name\":\"OrderPlaced\",\"namespace\":\"com.shop\",\"fields\":[{\"name\":\"event_id\",\"type\":\"string\"},{\"name\":\"aggregate_id\",\"type\":\"string\"},{\"name\":\"customer_id\",\"type\":\"string\"},{\"name\":\"sku\",\"type\":\"string\"},{\"name\":\"quantity\",\"type\":\"int\"},{\"name\":\"amount_cents\",\"type\":\"long\"},{\"name\":\"currency\",\"type\":\"string\",\"default\":\"USD\"},{\"name\":\"occurred_at\",\"type\":\"string\"},{\"name\":\"discount_cents\",\"type\":[\"null\",\"long\"],\"default\":null}]}"}'
```

Expected result: `{"is_compatible":true}`. Old consumers read new data; the
new field defaults to null.

Incompatible v3 removes `aggregate_id` and adds a required `coupon` field:

```bash
curl -s -X POST http://localhost:8081/compatibility/subjects/orders.events-value/versions/latest \
  -H 'Content-Type: application/vnd.schemaregistry.v1+json' \
  -d '{"schema":"{\"type\":\"record\",\"name\":\"OrderPlaced\",\"namespace\":\"com.shop\",\"fields\":[{\"name\":\"event_id\",\"type\":\"string\"},{\"name\":\"customer_id\",\"type\":\"string\"},{\"name\":\"sku\",\"type\":\"string\"},{\"name\":\"quantity\",\"type\":\"int\"},{\"name\":\"amount_cents\",\"type\":\"long\"},{\"name\":\"currency\",\"type\":\"string\",\"default\":\"USD\"},{\"name\":\"occurred_at\",\"type\":\"string\"},{\"name\":\"coupon\",\"type\":\"string\"}]}"}'
```

Expected result: `{"is_compatible":false}`. Do not register v3. Old readers
lose the routing key and new readers fail on old records missing `coupon`.

## 6. Run and verify

```bash
python producer.py
python consumer.py
printf '{"event_id":' | \
  python -c "import sys; from confluent_kafka import Producer; import os; p=Producer({'bootstrap.servers': os.environ['KAFKA_BOOTSTRAP_SERVERS']}); p.produce('orders.events', key=b'bad-1', value=b'{\"event_id\":'); p.flush(10.0)"
docker compose -f compose.yaml -f compose.registry.yaml exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:29092 --topic orders.dead --from-beginning \
  --property print.key=true --property print.headers=true --max-messages 5
docker compose -f compose.yaml -f compose.registry.yaml exec -T postgres psql -U app -d app \
  -c "SELECT consumer_group, event_id FROM processed_events ORDER BY claimed_at DESC LIMIT 5;"
```

Expected results: one claimed `order-<uuid>-placed` row, one `orders.dead`
record with `x-error-type=invalid`, and the source group has no lag on the
bad record because its offset was committed after the DLQ handoff.

## Checkpoints

??? question "Why BACKWARD lets you ship the producer first?"
    Old consumers can read new data when the new field has a default. The
    registry proves that before deploy; without the default the check fails.

??? question "Why do corrupt bytes skip retries?"
    Serde failure is deterministic: the same bytes fail forever. Retry would
    stall the partition; DLQ plus commit preserves progress and forensics.

## Done when

- [ ] Avro produce and consume round-trips with registry id 1
- [ ] v2 addition returns compatible; v3 removal returns incompatible
- [ ] Corrupt bytes land in `orders.dead` with `x-error-type=invalid`
- [ ] Redelivered events claim once via `processed_events`

## Learn more

- **Docs** — [Schema Registry serdes](https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/index.html)
- **Read** — [Multiple Event Types in the Same Kafka Topic](https://www.confluent.io/blog/multiple-event-types-in-the-same-kafka-topic/)

Next: **[P10 · Exactly-Once & Kafka Transactions](p10-exactly-once.md)**
