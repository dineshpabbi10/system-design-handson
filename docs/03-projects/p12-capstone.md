# P12 · Capstone: E-Commerce Platform

This is a bounded integration, not a new platform. You reuse the complete
services from P4, P7, P8, and P9 unchanged, then add only tracing plus one
analytics consumer. No new order, saga, or payment logic is defined here.

### How the flow works (what runs where, and what the two new pieces prove)

1. **P4 stays the order path.** `POST /orders` → `orders + orders_outbox` (one transaction, `order_id = uuid4()`, `event_id = order-{id}-placed`) → `relay.py` → `orders.events` → `inventory_worker.py` (claim → decrement → adjustment → commit). Ten posted orders must leave `0 pending` outbox rows — no loss lives here.
2. **P7 stays the saga path.** `POST /checkout` (`saga_id + order_id`, `Idempotency-Key`) → `command_outbox` → `commands.*` → `services.py` → `saga.events` → orchestrator `TRANSITIONS` → compensation or `OrderConfirmed`. The watchdog (`watchdog_count`, republish same `event_id`) is the chaos drill: stop `events`, run `watchdog --once`, restart `events` — the saga advances with no duplicate effect.
3. **P8 stays the poison path.** `reliability.py produce --failure-mode malformed` → `invalid` row in `dead_letters` + record on `orders.dead`; `produce --failure-mode permanent` → `replay --clear-failure` → exactly one `order_effects` row. Same IDs, same claim logic as P8 — the capstone only asserts the wiring still holds under one Compose stack.
4. **P9 stays the contract.** `order-value.avsc` governs `orders.events`; the Avro producer/consumer are the serde reference. The capstone does not re-evolve the schema — it relies on the registry to keep every reused producer honest.
5. **New piece 1: `analytics.py` is the convergence check.** Group `capstone-analytics` claims `(group, event_id)` from `orders.events` and bumps `analytics_counts(event_type, total)` with `INSERT … ON CONFLICT DO UPDATE total + 1`. Ten P4 orders must raise `OrderPlaced` by exactly 10 — duplicates would show here first.
6. **New piece 2: `tracing.py` is ingress-only honesty.** `setup_tracing("capstone-orders", app)` instruments the P4 HTTP span and exports OTLP to Jaeger (`localhost:4318` → UI `:16686`). `inject/extract` helpers exist for header propagation, but the relay does not propagate context yet — the guaranteed span is the order-ingress POST, not a full pipeline waterfall. That limitation is stated so you do not mistake an instrumented edge for end-to-end tracing.

## Exactly what is reused

| Prior file | Used unchanged for |
|------------|-------------------|
| Explicit init pattern from Setup | `connect_db`, `new_producer`, `publish_event`, `Jsonb` (repeated in each file) |
| `compose.yaml` from Setup | `kafka`, `postgres` base services |
| P4 `api.py` | `POST /orders`, order plus outbox transaction |
| P4 `relay.py` | outbox relay with `SKIP LOCKED`, ack then mark |
| P4 `inventory_worker.py` | stock claim plus adjustment, manual commit |
| P7 `orchestrator.py` | `POST /checkout`, `relay`, `events`, `watchdog` modes |
| P8 `reliability.py` | `produce`, `consume`, `retry`, `replay` commands |
| P9 `order-value.avsc` | `OrderPlaced` contract for `orders.events` |
| P9 `producer.py`, `consumer.py` | Avro serde reference, DLQ on invalid bytes |

New in this capstone, and only this:

```text
tracing.py
traced_api.py
analytics.py
capstone_schema.sql
```

Install the four tracing packages once:

```bash
python -m pip install opentelemetry-api opentelemetry-sdk \
  opentelemetry-exporter-otlp-proto-http \
  opentelemetry-instrumentation-fastapi
```

## 0. Stack addition

```yaml
services:
  jaeger:
    image: jaegertracing/all-in-one:1.62
    ports:
      - "127.0.0.1:16686:16686"
      - "127.0.0.1:4318:4318"
```

```sql title="capstone_schema.sql"
-- Analytics counter: one row per group + event type, bumped idempotently.
CREATE TABLE IF NOT EXISTS analytics_counts (
  consumer_group TEXT NOT NULL,
  event_type TEXT NOT NULL,
  total BIGINT NOT NULL DEFAULT 0,
  PRIMARY KEY (consumer_group, event_type)
);
```

```bash
export DATABASE_URL='postgresql://app:app@localhost:5432/app'
export KAFKA_BOOTSTRAP_SERVERS='localhost:9092'
docker compose up -d --wait --wait-timeout 180
docker compose exec -T postgres psql -v ON_ERROR_STOP=1 -U app -d app < capstone_schema.sql
```

P4, P7, and P8 schemas are applied from their own project roots first. This
file only adds the analytics counter.

## 1. `tracing.py`: fixed OTel setup

```python title="tracing.py"
from typing import Any
from fastapi import FastAPI
from opentelemetry import trace
from opentelemetry.propagate import extract, inject
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

_configured = False  # guard: set the global provider only once


def setup_tracing(service_name: str, app: Any = None):
    # Init OTel once, optionally instrument the FastAPI app for HTTP spans.
    global _configured
    if not _configured:
        provider = TracerProvider()
        exporter = OTLPSpanExporter(endpoint="http://localhost:4318/v1/traces")  # Jaeger OTLP
        provider.add_span_processor(BatchSpanProcessor(exporter))  # async batch export
        trace.set_tracer_provider(provider)
        _configured = True
    if app is not None:
        FastAPIInstrumentor.instrument_app(app)  # auto-span each HTTP request
    return trace.get_tracer(service_name)


def inject_context(carrier: dict[str, str]) -> dict[str, str]:
    # Write current trace ids into a dict for propagation (e.g. Kafka headers).
    inject(carrier)
    return carrier


def extract_context(carrier: dict[str, str]) -> Any:
    # Read trace ids back out on the receiving side.
    return extract(carrier)
```

Save this alongside the reused P4 API and serve it instead of the untraced API:

```python title="traced_api.py"
import tracing  # local OTel setup above
from api import app  # P4 self-contained API with explicit DB + event helpers

# Instrument HTTP spans; DB transaction in api.py is unchanged.
tracing.setup_tracing("capstone-orders", app)
```

This instruments the order API's HTTP spans without changing its transaction.
It does not automatically propagate trace context into relay publishing, so the
bounded capstone guarantee is an instrumented order-ingress span, not a complete
pipeline waterfall.

## 2. `analytics.py`: the one new consumer

```python title="analytics.py"
import json
import os

import psycopg
from confluent_kafka import Consumer
from psycopg.rows import dict_row

TOPIC = "orders.events"
GROUP = "capstone-analytics"


def connect_db():
    # Postgres with dict rows for the claim + counter transaction.
    return psycopg.connect(os.environ["DATABASE_URL"], row_factory=dict_row)


def new_consumer(group_id: str) -> Consumer:
    # Manual-commit consumer; offset follows the analytics transaction.
    return Consumer(
        {
            "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.offset.commit": False,
        }
    )


def main() -> None:
    # Counting consumer: claim event, bump per-type total, then commit offset.
    consumer = new_consumer(GROUP)
    consumer.subscribe([TOPIC])
    try:
        while True:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error() is not None:
                raise RuntimeError(str(message.error()))
            event = json.loads(message.value())  # envelope from P4 relay
            event_id = event["event_id"]  # dedup key
            event_type = event["event_type"]  # counting dimension
            with connect_db() as connection:
                with connection.transaction():  # claim + count = atomic
                    claimed = connection.execute(
                        "INSERT INTO processed_events (consumer_group, event_id) "
                        "VALUES (%s, %s) ON CONFLICT (consumer_group, event_id) "
                        "DO NOTHING RETURNING event_id",
                        (GROUP, event_id),
                    ).fetchone()
                    if claimed is not None:  # first delivery: increment
                        connection.execute(
                            "INSERT INTO analytics_counts (consumer_group, event_type, total) "
                            "VALUES (%s, %s, 1) "
                            "ON CONFLICT (consumer_group, event_type) "
                            "DO UPDATE SET total = analytics_counts.total + 1",
                            (GROUP, event_type),
                        )
                    # Duplicate: claim lost race -> no double count.
            consumer.commit(message=message, asynchronous=False)  # checkpoint
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
```

Run the reused processes plus the two new ones:

```bash
python -m uvicorn traced_api:app --host 127.0.0.1 --port 8000
python relay.py
python inventory_worker.py
python -m uvicorn orchestrator:app --host 127.0.0.1 --port 8001
python orchestrator.py events
python orchestrator.py relay --once
python orchestrator.py watchdog
python services.py inventory
python services.py payments
python services.py orders
python reliability.py consume
python analytics.py
```

## 3. Load, chaos, poison, replay assertions

Load ten orders through the reused P4 API, then assert one effect per event:

```bash
for i in $(seq 1 10); do
  curl -s -X POST http://127.0.0.1:8000/orders \
    -H 'Content-Type: application/json' \
    -d '{"customer_id":"c-1","sku":"sku-1","quantity":1,"amount":"10.00"}' > /dev/null
done
sleep 3
docker compose exec -T postgres psql -U app -d app \
  -c "SELECT count(*) AS orders FROM orders;"
docker compose exec -T postgres psql -U app -d app \
  -c "SELECT count(*) AS pending FROM orders_outbox WHERE published = FALSE;"
docker compose exec -T postgres psql -U app -d app \
  -c "SELECT event_type, total FROM analytics_counts WHERE consumer_group = 'capstone-analytics';"
```

Expected results: 10 orders, 0 pending after the relay pass, and the
analytics total for `OrderPlaced` increased by 10.

Chaos is the P7 watchdog path with the event consumer stopped:

```bash
KEY="checkout-$(date +%s)"
curl -s -X POST http://127.0.0.1:8001/checkout \
  -H "Idempotency-Key: $KEY" -H 'Content-Type: application/json' \
  -d '{"customer_id":"c-1","sku":"sku-1","quantity":1,"amount":"10.00"}'
python orchestrator.py watchdog --once
docker compose exec -T postgres psql -U app -d app \
  -c "SELECT status, current_step, watchdog_count FROM saga_instances WHERE idempotency_key = '$KEY';"
```

Expected result: the saga stays `running` with `watchdog_count` incremented
by one and its command row reset to unpublished; restarting
`python orchestrator.py events` lets it advance without a duplicate command
effect.

Poison uses the P8 producer path and dead-letter index:

```bash
python reliability.py produce --event-id poison-capstone-1 --failure-mode malformed
sleep 2
docker compose exec -T postgres psql -U app -d app \
  -c "SELECT event_id, error_class, attempts FROM dead_letters WHERE event_id LIKE 'invalid-%' ORDER BY dead_id DESC LIMIT 1;"
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:29092 --topic orders.dead --from-beginning --max-messages 2 \
  --property print.headers=true
```

Expected results: one `invalid` dead-letter row with `attempts = 1` and one
`orders.dead` record carrying `x-error-type=invalid`.

Replay repairs through the reused P8 command:

```bash
python reliability.py produce --event-id replay-capstone-1 --failure-mode permanent
sleep 2
python reliability.py replay --event-id replay-capstone-1 --clear-failure
sleep 2
docker compose exec -T postgres psql -U app -d app \
  -c "SELECT event_id, replayed_at FROM dead_letters WHERE event_id = 'replay-capstone-1';"
docker compose exec -T postgres psql -U app -d app \
  -c "SELECT event_id, count(*) FROM order_effects WHERE event_id = 'replay-capstone-1' GROUP BY event_id;"
```

Expected results: `replayed_at` is set once and `order_effects` has exactly
one row for the event; a second replay without new failure stays idempotent
via the `processed_events` claim.

## Checkpoints

??? question "Where does each guarantee live?"
    No loss in the P4 outbox relay; no oversell in inventory row locks; no
    double charge in the P3 idempotency key; stuck sagas in the P7 watchdog;
    poison in the P8 DLQ; contracts in the P9 registry; truth in traces.

??? question "What did the capstone actually add?"
    Only tracing correlation plus the analytics counter. Everything else is
    a reused file run as its own process; scaling and repair stay per-team.

## Done when

- [ ] Ten posted orders produce ten analytics counts with zero pending outbox
- [ ] Watchdog re-emit advances after restart with no duplicate effect
- [ ] Poison lands in `orders.dead` as `invalid`; replay yields one effect
- [ ] Jaeger shows the instrumented P4 POST span for one order

Next: **[Glossary](../04-resources/glossary.md)**.
