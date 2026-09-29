# P7 · Saga by Orchestration
**Read first:** [The Saga Pattern](../02-concepts/saga.md)
The orchestrator commits state and its command-outbox row in one PostgreSQL transaction; a relay publishes the command.
Step services below consume `commands.inventory`, `commands.payments`, and `commands.orders` and reply on `saga.events`.
Both `orchestrator.py` and `services.py` below are self-contained — each shows
its own explicit Postgres connect, Producer, Consumer, and event-envelope setup.

### How the flow works

Unlike P6 (every service watches everything), one brain — the orchestrator — issues commands and tracks the saga. Commands go out on `commands.*`; replies come back on `saga.events`.

1. **Three IDs, three jobs.** `order_id = uuid4()` is the business entity (what the customer bought). `saga_id = uuid4()` is the workflow run — retrying the same HTTP intent must not spawn two runs, so the client's `Idempotency-Key` header maps 1:1 to one `saga_id` in `checkout_requests` (with `request_hash` rejecting same-key-different-body). `command event_id = {saga_id}:{Command}` (e.g. `abc:ReserveStock`) is stable per step, so the watchdog can republish the identical command without looking like a new instruction.
2. **`POST /checkout` writes state + first command atomically.** One transaction inserts `checkout_requests` (the idempotency claim), `saga_instances(status = running, current_step = reserve_stock)`, and `command_outbox(ReserveStock → commands.inventory)`. No Kafka publish in the request path.
3. **The relay delivers commands.** `SELECT … WHERE published = FALSE … FOR UPDATE SKIP LOCKED` → `produce` to the per-service topic → mark published. `COMMAND_TOPICS` is the routing table: `ReserveStock/ReleaseStock → commands.inventory`, `ChargePayment/RefundPayment → commands.payments`, `ConfirmOrder/CancelOrder → commands.orders`.
4. **Step services are claim-then-apply.** Each (`saga-inventory-service`, `saga-payments-service`, `saga-orders-service`) claims `(group, command event_id)`, runs its local transaction (e.g. `UPDATE inventory_stock`, `INSERT INTO payment_ledger`, `INSERT INTO order_registry`), buffers a stable reply (`StockReserved`, `PaymentCaptured`, … with `aggregate_id = order_id`), publishes the reply to `saga.events`, and only then commits the command offset. A redelivered command loses the claim and re-emits nothing new.
5. **The orchestrator folds replies through `TRANSITIONS`.** Group `saga-orchestrator` claims each reply, locks the saga row (`SELECT … FOR UPDATE`), and looks up `(current_step, event_type)`: `(reserve_stock, StockReserved) → (charge_payment, ChargePayment)`, `(charge_payment, PaymentCaptured) → (confirm, ConfirmOrder)`, `(confirm, OrderConfirmed) → done`. Failures (`StockFailed`, `PaymentFailed`, `OrderFailed`) call `begin_compensation()`: flip to `compensating`, set `stock_release_required / refund_required` from what actually completed, and enqueue `ReleaseStock + RefundPayment + CancelOrder`. While `compensating`, only `StockReleased / PaymentRefunded / OrderCancelled` advance the row; when every required compensation lands, status becomes `aborted`. A terminal (`succeeded / aborted`) saga ignores late events — no transition, no-op.
6. **The watchdog heals silence.** If `updated_at < now() - 30s` and still `running`, it republishes the same command `event_id` (reset `published = FALSE`) and bumps `watchdog_count`; after 2 silent windows it compensates. Same ID means the republish is dedup-safe, not a duplicate charge.
## Topics and state
Create these topics before starting the relay:
```bash
export DATABASE_URL='postgresql://app:app@localhost:5432/app'
export KAFKA_BOOTSTRAP_SERVERS='localhost:9092'
docker compose up -d --wait --wait-timeout 180
for topic in saga.events commands.inventory commands.payments commands.orders; do
  docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server kafka:29092 --create --if-not-exists \
    --topic "$topic" --partitions 3 --replication-factor 1 \
    --config min.insync.replicas=1
done
docker compose exec -T postgres psql -v ON_ERROR_STOP=1 -U app -d app < schema.sql
```
`ReserveStock` waits for `StockReserved` or `StockFailed`; `PaymentCaptured` leads to `ConfirmOrder`.
The `TRANSITIONS` table is the only allowed normal path; failure enters compensation and a terminal saga ignores late events.
## Complete schema
```sql title="schema.sql"
CREATE TABLE IF NOT EXISTS saga_instances (
  saga_id TEXT PRIMARY KEY, order_id TEXT NOT NULL UNIQUE, idempotency_key TEXT NOT NULL UNIQUE, request_hash TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('running', 'compensating', 'succeeded', 'aborted')),
  current_step TEXT NOT NULL CHECK (current_step IN ('reserve_stock', 'charge_payment', 'confirm', 'abort', 'done')),
  payload JSONB NOT NULL,
  stock_done INTEGER NOT NULL DEFAULT 0 CHECK (stock_done IN (0, 1)), payment_done INTEGER NOT NULL DEFAULT 0 CHECK (payment_done IN (0, 1)),
  stock_release_required INTEGER NOT NULL DEFAULT 0 CHECK (stock_release_required IN (0, 1)), refund_required INTEGER NOT NULL DEFAULT 0 CHECK (refund_required IN (0, 1)),
  order_cancel_required INTEGER NOT NULL DEFAULT 1 CHECK (order_cancel_required IN (0, 1)),
  stock_compensated INTEGER NOT NULL DEFAULT 0 CHECK (stock_compensated IN (0, 1)), payment_compensated INTEGER NOT NULL DEFAULT 0 CHECK (payment_compensated IN (0, 1)),
  order_cancelled INTEGER NOT NULL DEFAULT 0 CHECK (order_cancelled IN (0, 1)), watchdog_count INTEGER NOT NULL DEFAULT 0 CHECK (watchdog_count >= 0),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(), updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS checkout_requests (
  idempotency_key TEXT PRIMARY KEY, request_hash TEXT NOT NULL, saga_id TEXT NOT NULL, response JSONB NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS command_outbox (
  command_id BIGSERIAL PRIMARY KEY, saga_id TEXT NOT NULL REFERENCES saga_instances(saga_id), event_id TEXT NOT NULL UNIQUE,
  command_type TEXT NOT NULL, topic TEXT NOT NULL, payload JSONB NOT NULL, published BOOLEAN NOT NULL DEFAULT FALSE, created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS command_outbox_pending_idx ON command_outbox (published, command_id) WHERE published = FALSE;
CREATE TABLE IF NOT EXISTS processed_events (
  consumer_group TEXT NOT NULL, event_id TEXT NOT NULL, claimed_at TIMESTAMPTZ NOT NULL DEFAULT now(), PRIMARY KEY (consumer_group, event_id)
);
CREATE TABLE IF NOT EXISTS inventory_stock (sku TEXT PRIMARY KEY, available INTEGER NOT NULL CHECK (available >= 0));
CREATE TABLE IF NOT EXISTS payment_ledger (order_id TEXT PRIMARY KEY, amount NUMERIC(12,2) NOT NULL, captured BOOLEAN NOT NULL DEFAULT FALSE, refunded BOOLEAN NOT NULL DEFAULT FALSE);
CREATE TABLE IF NOT EXISTS order_registry (order_id TEXT PRIMARY KEY, status TEXT NOT NULL);
INSERT INTO inventory_stock (sku, available) VALUES ('sku-1', 100) ON CONFLICT (sku) DO NOTHING;
```
The `checkout_requests` insert is the idempotency claim; outbox and state share one transaction, so Kafka can only duplicate a command.
## Complete orchestrator
```python title="orchestrator.py"
import argparse
import hashlib
import json
import os
import time
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Callable
from uuid import uuid4

import psycopg
from confluent_kafka import Consumer, Producer
from fastapi import FastAPI, Header, HTTPException
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from pydantic import BaseModel, Field

# --- Explicit init helpers (no shared module; repeated to teach init) ---
def connect_db():
    # Postgres with dict rows; one connection per request/handler.
    return psycopg.connect(os.environ["DATABASE_URL"], row_factory=dict_row)


def make_event(event_id, event_type, aggregate_type, aggregate_id, data):
    # Canonical envelope; stable event_id makes command retries dedup-safe.
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
    # Idempotent producer: relay restarts can safely republish commands.
    return Producer(
        {
            "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
            "client.id": client_id,
            "acks": "all",
            "enable.idempotence": True,
            "delivery.timeout.ms": 30000,
        }
    )


def publish_event(producer: Producer, topic: str, event: dict[str, Any]) -> None:
    # Keyed produce + flush; raises if broker rejects the write.
    errors: list[str] = []

    def delivered(error: object, message: object) -> None:
        if error is not None:
            errors.append(str(error))

    producer.produce(
        topic,
        key=str(event["aggregate_id"]).encode("utf-8"),
        value=json.dumps(event, sort_keys=True, separators=(",", ":")).encode("utf-8"),
        callback=delivered,
    )
    remaining = producer.flush(15.0)
    if remaining != 0:
        raise TimeoutError(f"{remaining} record(s) undelivered")
    if errors:
        raise RuntimeError(errors[0])
    producer.poll(0)


app = FastAPI()
EVENT_TOPIC = "saga.events"
GROUP = "saga-orchestrator"
COMMAND_TOPICS = {
    "ReserveStock": "commands.inventory",
    "ChargePayment": "commands.payments",
    "ConfirmOrder": "commands.orders",
    "ReleaseStock": "commands.inventory",
    "RefundPayment": "commands.payments",
    "CancelOrder": "commands.orders",
}
TRANSITIONS = {
    ("reserve_stock", "StockReserved"): ("charge_payment", "ChargePayment"),
    ("reserve_stock", "StockFailed"): ("abort", None),
    ("charge_payment", "PaymentCaptured"): ("confirm", "ConfirmOrder"),
    ("charge_payment", "PaymentFailed"): ("abort", None),
    ("confirm", "PaymentFailed"): ("abort", None),
    ("confirm", "StockFailed"): ("abort", None),
    ("confirm", "OrderConfirmed"): ("done", None),
    ("confirm", "OrderFailed"): ("abort", None),
}
STEP_COMMANDS = {"reserve_stock": "ReserveStock", "charge_payment": "ChargePayment", "confirm": "ConfirmOrder"}
COMPENSATION_EVENTS = {"StockReleased", "PaymentRefunded", "OrderCancelled"}
class CheckoutRequest(BaseModel):
    customer_id: str
    sku: str
    quantity: int = Field(gt=0)
    amount: Decimal = Field(gt=0)
def request_hash(request: CheckoutRequest) -> str:
    body = json.dumps(
        {
            "customer_id": request.customer_id,
            "sku": request.sku,
            "quantity": request.quantity,
            "amount": str(request.amount),
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(body.encode("utf-8")).hexdigest()
def command_event(saga: dict[str, Any], command: str) -> dict[str, Any]:
    # Stable command id per saga+command: watchdog replays publish same id.
    return make_event(
        event_id=f"{saga['saga_id']}:{command}",
        event_type=command,
        aggregate_type="saga",
        aggregate_id=saga["order_id"],
        data={**saga["payload"], "saga_id": saga["saga_id"], "command": command},
    )
def enqueue(connection: Any, saga: dict[str, Any], command: str) -> None:
    event = command_event(saga, command)
    connection.execute(
        """
        INSERT INTO command_outbox
            (saga_id, event_id, command_type, topic, payload)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (event_id) DO NOTHING
        """,
        (
            saga["saga_id"],
            event["event_id"],
            command,
            COMMAND_TOPICS[command],
            Jsonb(event),
        ),
    )
def begin_compensation(connection: Any, saga: dict[str, Any]) -> None:
    connection.execute(
        """
        UPDATE saga_instances
        SET status = 'compensating', current_step = 'abort',
            stock_release_required = %s, refund_required = %s,
            order_cancel_required = 1, updated_at = now()
        WHERE saga_id = %s AND status = 'running'
        """,
        (saga["stock_done"], saga["payment_done"], saga["saga_id"]),
    )
    if saga["stock_done"] == 1:
        enqueue(connection, saga, "ReleaseStock")
    if saga["payment_done"] == 1:
        enqueue(connection, saga, "RefundPayment")
    enqueue(connection, saga, "CancelOrder")
def apply_event(event: dict[str, Any]) -> None:
    # Orchestrator step: claim event, lock saga row, advance TRANSITIONS table.
    event_id = event["event_id"]
    event_type = event["event_type"]
    order_id = event["aggregate_id"]
    with connect_db() as connection:
        with connection.transaction():
            claimed = connection.execute(
                """
                INSERT INTO processed_events (consumer_group, event_id)
                VALUES (%s, %s)
                ON CONFLICT (consumer_group, event_id) DO NOTHING
                RETURNING event_id
                """,
                (GROUP, event_id),
            ).fetchone()
            if claimed is None:
                return
            saga = connection.execute(
                "SELECT * FROM saga_instances WHERE order_id = %s FOR UPDATE",
                (order_id,),
            ).fetchone()
            if saga is None:
                raise RuntimeError(f"unknown saga order {order_id}")
            if saga["status"] == "compensating":
                if event_type not in COMPENSATION_EVENTS:
                    return
                if event_type == "StockReleased":
                    connection.execute(
                        "UPDATE saga_instances SET stock_compensated = 1 WHERE saga_id = %s AND stock_release_required = 1",
                        (saga["saga_id"],),
                    )
                elif event_type == "PaymentRefunded":
                    connection.execute(
                        "UPDATE saga_instances SET payment_compensated = 1 WHERE saga_id = %s AND refund_required = 1",
                        (saga["saga_id"],),
                    )
                else:
                    connection.execute(
                        "UPDATE saga_instances SET order_cancelled = 1, updated_at = now() WHERE saga_id = %s",
                        (saga["saga_id"],),
                    )
                state = connection.execute(
                    """
                    SELECT stock_release_required, refund_required,
                           stock_compensated, payment_compensated
                    FROM saga_instances WHERE saga_id = %s
                    """,
                    (saga["saga_id"],),
                ).fetchone()
                if state is not None and (
                    (state["stock_release_required"] == 0 or state["stock_compensated"] == 1)
                    and (state["refund_required"] == 0 or state["payment_compensated"] == 1)
                ):
                    connection.execute(
                        "UPDATE saga_instances SET status = 'aborted', current_step = 'done', updated_at = now() WHERE saga_id = %s",
                        (saga["saga_id"],),
                    )
                return
            if saga["status"] != "running":
                return
            transition = TRANSITIONS.get((saga["current_step"], event_type))
            if transition is None:
                return
            next_step, next_command = transition
            if event_type in {"StockFailed", "PaymentFailed", "OrderFailed"}:
                begin_compensation(connection, saga)
                return
            if event_type == "StockReserved":
                connection.execute(
                    "UPDATE saga_instances SET stock_done = 1, current_step = %s, updated_at = now() WHERE saga_id = %s",
                    (next_step, saga["saga_id"]),
                )
            elif event_type == "PaymentCaptured":
                connection.execute(
                    "UPDATE saga_instances SET payment_done = 1, current_step = %s, updated_at = now() WHERE saga_id = %s",
                    (next_step, saga["saga_id"]),
                )
            else:
                connection.execute(
                    "UPDATE saga_instances SET status = 'succeeded', current_step = 'done', updated_at = now() WHERE saga_id = %s",
                    (saga["saga_id"],),
                )
            if next_command is not None:
                current = connection.execute(
                    "SELECT * FROM saga_instances WHERE saga_id = %s",
                    (saga["saga_id"],),
                ).fetchone()
                if current is not None:
                    enqueue(connection, current, next_command)
def event_consumer() -> None:
    consumer = Consumer(
        {
            "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
            "group.id": GROUP,
            "auto.offset.reset": "earliest",
            "enable.auto.offset.commit": False,
        }
    )
    consumer.subscribe([EVENT_TOPIC])
    try:
        while True:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error() is not None:
                raise RuntimeError(str(message.error()))
            apply_event(json.loads(message.value()))
            consumer.commit(message=message, asynchronous=False)
    finally:
        consumer.close()
def relay_once(connection: Any, producer: Any) -> int:
    with connection.transaction():
        rows = connection.execute(
            """
            SELECT command_id, topic, payload
            FROM command_outbox
            WHERE published = FALSE
            ORDER BY command_id
            LIMIT 100
            FOR UPDATE SKIP LOCKED
            """
        ).fetchall()
        for row in rows:
            publish_event(producer, row["topic"], row["payload"])
        if rows:
            connection.execute(
                "UPDATE command_outbox SET published = TRUE WHERE command_id = ANY(%s)",
                ([row["command_id"] for row in rows],),
            )
    return len(rows)
def relay(once: bool) -> None:
    producer = new_producer("p07-command-relay")
    with connect_db() as connection:
        while True:
            try:
                count = relay_once(connection, producer)
            except Exception as error:
                print(f"relay failed: {error}")
                if once:
                    raise
                time.sleep(1)
            else:
                if once:
                    return
                if count == 0:
                    time.sleep(0.1)
def watchdog_once(connection: Any) -> bool:
    with connection.transaction():
        saga = connection.execute(
            """
            SELECT * FROM saga_instances
            WHERE status = 'running'
              AND updated_at < now() - interval '30 seconds'
            ORDER BY updated_at
            LIMIT 1
            FOR UPDATE SKIP LOCKED
            """
        ).fetchone()
        if saga is None:
            return False
        if saga["watchdog_count"] >= 2:
            begin_compensation(connection, saga)
            return True
        command = connection.execute(
            "SELECT event_id FROM command_outbox WHERE saga_id = %s AND event_id = %s",
            (saga["saga_id"], f"{saga['saga_id']}:{STEP_COMMANDS[saga['current_step']]}"),
        ).fetchone()
        if command is None:
            raise RuntimeError(f"missing command for saga {saga['saga_id']}")
        connection.execute(
            "UPDATE saga_instances SET watchdog_count = watchdog_count + 1, updated_at = now() WHERE saga_id = %s",
            (saga["saga_id"],),
        )
        connection.execute(
            "UPDATE command_outbox SET published = FALSE WHERE event_id = %s",
            (command["event_id"],),
        )
        return True
def watchdog(once: bool) -> None:
    with connect_db() as connection:
        while True:
            try:
                changed = watchdog_once(connection)
            except Exception as error:
                print(f"watchdog failed: {error}")
                if once:
                    raise
                time.sleep(1)
            else:
                if once:
                    return
                if not changed:
                    time.sleep(1)
@app.post("/checkout")
def checkout(
    request: CheckoutRequest,
    idempotency_key: str = Header(),
) -> dict[str, str]:
    body_hash = request_hash(request)
    saga_id = str(uuid4())
    order_id = str(uuid4())
    payload = {
        "order_id": order_id,
        "customer_id": request.customer_id,
        "sku": request.sku,
        "quantity": request.quantity,
        "amount": str(request.amount),
    }
    response = {"saga_id": saga_id, "order_id": order_id, "status": "running"}
    with connect_db() as connection:
        with connection.transaction():
            claimed = connection.execute(
                """
                INSERT INTO checkout_requests
                    (idempotency_key, request_hash, saga_id, response)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (idempotency_key) DO NOTHING
                RETURNING idempotency_key
                """,
                (idempotency_key, body_hash, saga_id, Jsonb(response)),
            ).fetchone()
            if claimed is None:
                existing = connection.execute(
                    "SELECT request_hash, response FROM checkout_requests WHERE idempotency_key = %s",
                    (idempotency_key,),
                ).fetchone()
                if existing is None or existing["request_hash"] != body_hash:
                    raise HTTPException(status_code=409, detail="idempotency key conflict")
                response = existing["response"]
            else:
                connection.execute(
                    """
                    INSERT INTO saga_instances
                        (saga_id, order_id, idempotency_key, request_hash,
                         status, current_step, payload)
                    VALUES (%s, %s, %s, %s, 'running', 'reserve_stock', %s)
                    """,
                    (
                        saga_id,
                        order_id,
                        idempotency_key,
                        body_hash,
                        Jsonb(payload),
                    ),
                )
                enqueue(
                    connection,
                    {"saga_id": saga_id, "order_id": order_id, "payload": payload},
                    "ReserveStock",
                )
    return response
def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=["relay", "events", "watchdog"])
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()
    if args.mode == "relay":
        relay(args.once)
    elif args.mode == "events":
        event_consumer()
    else:
        watchdog(args.once)
if __name__ == "__main__":
    main()
```
`apply_event` claims the event, locks the saga row, and checks the exact `(current_step, event_type)` pair; duplicates lose the claim and late events have no transition.
Compensation events run only while `compensating`; `aborted` requires every required compensation. The watchdog republishes the same command event ID after 30 seconds and compensates after two observations.
## Complete command services
`services.py` runs three groups in separate processes. Each handler claims its command event ID in `processed_events`, updates its local table in one psycopg transaction, and buffers its stable reply. `consume` publishes those replies and commits only after every local transaction and publish succeeds, so redelivery remains idempotent.
```python title="services.py"
import argparse
import json
import os
from datetime import datetime, timezone
from typing import Any

import psycopg
from confluent_kafka import Consumer, Producer
from psycopg.rows import dict_row

# --- Explicit init helpers (same pattern as orchestrator; no shared module) ---
def connect_db():
    # Postgres with dict rows for the claim + local update transaction.
    return psycopg.connect(os.environ["DATABASE_URL"], row_factory=dict_row)


def make_event(event_id, event_type, aggregate_type, aggregate_id, data):
    # Canonical envelope; stable ids keep command/reply retries dedup-safe.
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
    # Idempotent producer for reply events on saga.events.
    return Producer(
        {
            "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
            "client.id": client_id,
            "acks": "all",
            "enable.idempotence": True,
            "delivery.timeout.ms": 30000,
        }
    )


def publish_event(producer: Producer, topic: str, event: dict[str, Any]) -> None:
    # Keyed produce + flush; raises on broker error before offset commit.
    errors: list[str] = []

    def delivered(error: object, message: object) -> None:
        if error is not None:
            errors.append(str(error))

    producer.produce(
        topic,
        key=str(event["aggregate_id"]).encode("utf-8"),
        value=json.dumps(event, sort_keys=True, separators=(",", ":")).encode("utf-8"),
        callback=delivered,
    )
    remaining = producer.flush(15.0)
    if remaining != 0:
        raise TimeoutError(f"{remaining} record(s) undelivered")
    if errors:
        raise RuntimeError(errors[0])
    producer.poll(0)


EVENT_TOPIC = "saga.events"
TOPICS = {"inventory": "commands.inventory", "payments": "commands.payments", "orders": "commands.orders"}
GROUPS = {"inventory": "saga-inventory-service", "payments": "saga-payments-service", "orders": "saga-orders-service"}
def claim(connection: Any, group: str, event_id: str) -> bool:
    return connection.execute("INSERT INTO processed_events (consumer_group, event_id) VALUES (%s, %s) ON CONFLICT (consumer_group, event_id) DO NOTHING RETURNING event_id", (group, event_id)).fetchone() is not None
def emit(outbox: list, event_id: str, event_type: str, order_id: str, data: dict[str, Any]) -> None:
    # Buffer reply in memory; consume() publishes after local DB commit.
    outbox.append(make_event(event_id, event_type, "saga", order_id, data))
def inventory_handler(command: dict[str, Any], outbox: list) -> None:
    event_type = command["event_type"]
    if event_type not in {"ReserveStock", "ReleaseStock"}:
        return
    order_id = command["aggregate_id"]
    data = command["data"]
    saga_id = data["saga_id"]
    with connect_db() as connection:
        with connection.transaction():
            if not claim(connection, GROUPS["inventory"], command["event_id"]):
                return
            if event_type == "ReserveStock":
                row = connection.execute("SELECT available FROM inventory_stock WHERE sku = %s FOR UPDATE", (data["sku"],)).fetchone()
                if data["sku"] == "fail-stock" or row is None or row["available"] < data["quantity"]:
                    emit(outbox, f"stock-{order_id}-failed", "StockFailed", order_id, {"order_id": order_id, "saga_id": saga_id, "reason": "out-of-stock"})
                else:
                    connection.execute("UPDATE inventory_stock SET available = available - %s WHERE sku = %s", (data["quantity"], data["sku"]))
                    emit(outbox, f"stock-{order_id}-reserved", "StockReserved", order_id, {"order_id": order_id, "saga_id": saga_id, "sku": data["sku"], "quantity": data["quantity"]})
            else:
                connection.execute("UPDATE inventory_stock SET available = available + %s WHERE sku = %s", (data["quantity"], data["sku"]))
                emit(outbox, f"stock-{order_id}-released", "StockReleased", order_id, {"order_id": order_id, "saga_id": saga_id, "sku": data["sku"], "quantity": data["quantity"]})
def payments_handler(command: dict[str, Any], outbox: list) -> None:
    event_type = command["event_type"]
    if event_type not in {"ChargePayment", "RefundPayment"}:
        return
    order_id = command["aggregate_id"]
    data = command["data"]
    saga_id = data["saga_id"]
    with connect_db() as connection:
        with connection.transaction():
            if not claim(connection, GROUPS["payments"], command["event_id"]):
                return
            if event_type == "ChargePayment":
                if data["customer_id"] == "fail-payment":
                    emit(outbox, f"payment-{order_id}-failed", "PaymentFailed", order_id, {"order_id": order_id, "saga_id": saga_id, "reason": "card-declined"})
                else:
                    connection.execute("INSERT INTO payment_ledger (order_id, amount, captured) VALUES (%s, %s, TRUE) ON CONFLICT (order_id) DO NOTHING", (order_id, data["amount"]))
                    emit(outbox, f"payment-{order_id}-captured", "PaymentCaptured", order_id, {"order_id": order_id, "saga_id": saga_id, "amount": data["amount"]})
            else:
                connection.execute("UPDATE payment_ledger SET refunded = TRUE WHERE order_id = %s", (order_id,))
                emit(outbox, f"payment-{order_id}-refunded", "PaymentRefunded", order_id, {"order_id": order_id, "saga_id": saga_id})
def orders_handler(command: dict[str, Any], outbox: list) -> None:
    event_type = command["event_type"]
    if event_type not in {"ConfirmOrder", "CancelOrder"}:
        return
    order_id = command["aggregate_id"]
    data = command["data"]
    saga_id = data["saga_id"]
    with connect_db() as connection:
        with connection.transaction():
            if not claim(connection, GROUPS["orders"], command["event_id"]):
                return
            if event_type == "ConfirmOrder":
                if data["sku"] == "fail-order":
                    emit(outbox, f"order-{order_id}-failed", "OrderFailed", order_id, {"order_id": order_id, "saga_id": saga_id, "reason": "confirm-rejected"})
                else:
                    connection.execute("INSERT INTO order_registry (order_id, status) VALUES (%s, %s) ON CONFLICT (order_id) DO UPDATE SET status = %s", (order_id, "confirmed", "confirmed"))
                    emit(outbox, f"order-{order_id}-confirmed", "OrderConfirmed", order_id, {"order_id": order_id, "saga_id": saga_id})
            else:
                connection.execute("INSERT INTO order_registry (order_id, status) VALUES (%s, %s) ON CONFLICT (order_id) DO UPDATE SET status = %s", (order_id, "cancelled", "cancelled"))
                emit(outbox, f"order-{order_id}-cancelled", "OrderCancelled", order_id, {"order_id": order_id, "saga_id": saga_id})
def consume(service: str) -> None:
    producer = new_producer(f"p07-{service}-service")
    consumer = Consumer({"bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"], "group.id": GROUPS[service], "auto.offset.reset": "earliest", "enable.auto.offset.commit": False})
    consumer.subscribe([TOPICS[service]])
    try:
        while True:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error() is not None:
                raise RuntimeError(str(message.error()))
            outbox: list = []
            {"inventory": inventory_handler, "payments": payments_handler, "orders": orders_handler}[service](json.loads(message.value()), outbox)
            for event in outbox:
                publish_event(producer, EVENT_TOPIC, event)
            consumer.commit(message=message, asynchronous=False)
    finally:
        consumer.close()
def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=["inventory", "payments", "orders"])
    args = parser.parse_args()
    consume(args.mode)
if __name__ == "__main__":
    main()
```
## Run and verify
Start the API, orchestrator workers, and three command services in separate terminals:
```bash
python -m uvicorn orchestrator:app --host 127.0.0.1 --port 8000
python orchestrator.py events
python orchestrator.py relay
python orchestrator.py watchdog
python services.py inventory
python services.py payments
python services.py orders
```
Create one checkout, repeat the key, then inspect state and claims:
```bash
KEY="checkout-$(date +%s)"
curl -sS -X POST http://127.0.0.1:8000/checkout -H "Idempotency-Key: $KEY" -H 'Content-Type: application/json' -d '{"customer_id":"c-1","sku":"sku-1","quantity":2,"amount":"25.00"}'
curl -sS -X POST http://127.0.0.1:8000/checkout -H "Idempotency-Key: $KEY" -H 'Content-Type: application/json' -d '{"customer_id":"c-1","sku":"sku-1","quantity":2,"amount":"25.00"}'
docker compose exec -T postgres psql -U app -d app -c "SELECT status, current_step, watchdog_count FROM saga_instances WHERE idempotency_key = '$KEY';"
docker compose exec -T postgres psql -U app -d app -c "SELECT command_type, topic, published FROM command_outbox ORDER BY command_id;"
docker compose exec -T postgres psql -U app -d app -c "SELECT consumer_group, event_id FROM processed_events ORDER BY claimed_at;"
```
The repeated key creates one saga. After `succeeded`, a duplicate `StockReserved` leaves the terminal row unchanged; with the event consumer stopped, `python orchestrator.py watchdog --once` bumps `watchdog_count` and the relay republishes the same command ID.
Next: **[P8 · Retries, Backoff & the Dead Letter Queue](p08-reliability-dlq.md)**.
