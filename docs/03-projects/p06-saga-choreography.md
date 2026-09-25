# P6 · Saga by Choreography

**Read first:** [The Saga Pattern](../02-concepts/saga.md)

This bounded lab places four small services in one educational process. Each
service has its own consumer group, local tables, and local outbox. Production
would run these groups as separate processes with separate databases.

## Ownership and compensation

| Owner | Events emitted | Compensation direction |
|---|---|---|
| orders | `OrderPlaced`, `OrderCancelled` | either failure cancels the order |
| payments | `PaymentCaptured`, `PaymentFailed`, `PaymentRefunded` | later failure refunds a capture |
| inventory | `StockReserved`, `StockFailed`, `StockReleased` | payment failure releases a reservation |
| shipping | `OrderConfirmed` | only schedules after both success flags |

There is one producer-owner for each event type. `OrderPlaced` is the only
checkout-start event. The explicit directions are `PaymentFailed` to
`StockReleased`, `StockFailed` to `PaymentRefunded`, and either failure to
`OrderCancelled`. This bounded demo injects failures before shipping, so it
does not model cancelling an already dispatched shipment.

## Files and setup

Copy shared `common.py` and `compose.yaml` unchanged. Create `schema.sql` and
one clearly labeled educational `workflow.py` containing the API, relay, and
all four handlers.

```text
compose.yaml
common.py
schema.sql
workflow.py
```

```bash
export DATABASE_URL='postgresql://app:app@localhost:5432/app'
export KAFKA_BOOTSTRAP_SERVERS='localhost:9092'
docker compose up -d --wait --wait-timeout 180
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:29092 --create --if-not-exists \
  --topic saga.events --partitions 3 --replication-factor 1 \
  --config min.insync.replicas=1
docker compose exec -T postgres psql -v ON_ERROR_STOP=1 -U app -d app < schema.sql
```

## Complete SQL schema

```sql title="schema.sql"
CREATE TABLE IF NOT EXISTS orders (
  order_id TEXT PRIMARY KEY, customer_id TEXT NOT NULL, sku TEXT NOT NULL,
  quantity INTEGER NOT NULL CHECK (quantity > 0), amount NUMERIC(12,2) NOT NULL CHECK (amount > 0),
  fail_payment BOOLEAN NOT NULL DEFAULT FALSE, fail_stock BOOLEAN NOT NULL DEFAULT FALSE,
  status TEXT NOT NULL DEFAULT 'new', created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS order_pipeline (
  order_id TEXT PRIMARY KEY REFERENCES orders(order_id),
  payment_ok INTEGER NOT NULL DEFAULT 0 CHECK (payment_ok IN (0,1)),
  stock_ok INTEGER NOT NULL DEFAULT 0 CHECK (stock_ok IN (0,1)),
  payment_failed INTEGER NOT NULL DEFAULT 0 CHECK (payment_failed IN (0,1)),
  stock_failed INTEGER NOT NULL DEFAULT 0 CHECK (stock_failed IN (0,1)),
  payment_refunded INTEGER NOT NULL DEFAULT 0 CHECK (payment_refunded IN (0,1)),
  stock_released INTEGER NOT NULL DEFAULT 0 CHECK (stock_released IN (0,1)),
  shipment_scheduled INTEGER NOT NULL DEFAULT 0 CHECK (shipment_scheduled IN (0,1)),
  cancelled INTEGER NOT NULL DEFAULT 0 CHECK (cancelled IN (0,1))
);
CREATE TABLE IF NOT EXISTS payments (
  order_id TEXT PRIMARY KEY REFERENCES orders(order_id), amount NUMERIC(12,2) NOT NULL,
  captured BOOLEAN NOT NULL DEFAULT FALSE, refunded BOOLEAN NOT NULL DEFAULT FALSE
);
CREATE TABLE IF NOT EXISTS inventory (
  sku TEXT PRIMARY KEY, available INTEGER NOT NULL CHECK (available >= 0),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS reservations (
  order_id TEXT PRIMARY KEY REFERENCES orders(order_id), sku TEXT NOT NULL REFERENCES inventory(sku),
  quantity INTEGER NOT NULL CHECK (quantity > 0), released BOOLEAN NOT NULL DEFAULT FALSE
);
CREATE TABLE IF NOT EXISTS saga_outbox (
  id BIGSERIAL PRIMARY KEY, aggregate_id TEXT NOT NULL, event_type TEXT NOT NULL,
  event_id TEXT NOT NULL UNIQUE, payload JSONB NOT NULL,
  published BOOLEAN NOT NULL DEFAULT FALSE, created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS saga_outbox_pending_idx ON saga_outbox (published, id) WHERE published = FALSE;
CREATE TABLE IF NOT EXISTS processed_events (
  consumer_group TEXT NOT NULL, event_id TEXT NOT NULL,
  claimed_at TIMESTAMPTZ NOT NULL DEFAULT now(), PRIMARY KEY (consumer_group, event_id)
);
INSERT INTO inventory (sku, available) VALUES ('sku-1', 100) ON CONFLICT (sku) DO NOTHING;
```

The integer columns are correlation flags, not a text step. A handler locks
`order_pipeline`, records its fact, and evaluates the whole flag set, so the
payment and stock facts may arrive in either order.

## Complete educational `workflow.py`

```python title="workflow.py"
import argparse
import json
import os
import time
from decimal import Decimal
from typing import Any, Callable
from uuid import uuid4
from confluent_kafka import Consumer
from fastapi import FastAPI
from pydantic import BaseModel, Field
import common

app = FastAPI()
TOPIC = "saga.events"
GROUPS = {"payment": "saga-payments", "inventory": "saga-inventory", "orders": "saga-orders", "shipping": "saga-shipping"}

class CheckoutRequest(BaseModel):
    customer_id: str
    sku: str
    quantity: int = Field(gt=0)
    amount: Decimal = Field(gt=0)
    fail_payment: bool = False
    fail_stock: bool = False

def enqueue(connection: Any, event: dict[str, Any]) -> None:
    connection.execute(
        "INSERT INTO saga_outbox (aggregate_id, event_type, event_id, payload) VALUES (%s, %s, %s, %s) ON CONFLICT (event_id) DO NOTHING",
        (event["aggregate_id"], event["event_type"], event["event_id"], common.jsonb(event)),
    )

def emit(connection: Any, event_id: str, event_type: str, order_id: str, data: dict[str, Any]) -> None:
    enqueue(connection, common.make_event(event_id, event_type, "checkout", order_id, data))

@app.post("/checkout")
def checkout(request: CheckoutRequest) -> dict[str, str | int]:
    order_id = str(uuid4())
    event_id = f"order-{order_id}-placed"
    with common.connect() as connection:
        with connection.transaction():
            connection.execute(
                "INSERT INTO orders (order_id, customer_id, sku, quantity, amount, fail_payment, fail_stock) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                (order_id, request.customer_id, request.sku, request.quantity, request.amount, request.fail_payment, request.fail_stock),
            )
            connection.execute("INSERT INTO order_pipeline (order_id) VALUES (%s)", (order_id,))
            emit(connection, event_id, "OrderPlaced", order_id, {"order_id": order_id, "customer_id": request.customer_id, "sku": request.sku, "quantity": request.quantity, "amount": str(request.amount), "fail_payment": request.fail_payment, "fail_stock": request.fail_stock})
    return {"order_id": order_id, "status": "new"}

def claim(connection: Any, group: str, event_id: str) -> bool:
    return connection.execute(
        "INSERT INTO processed_events (consumer_group, event_id) VALUES (%s, %s) ON CONFLICT (consumer_group, event_id) DO NOTHING RETURNING event_id",
        (group, event_id),
    ).fetchone() is not None

def refund(connection: Any, order_id: str) -> None:
    row = connection.execute("SELECT amount, captured, refunded FROM payments WHERE order_id = %s FOR UPDATE", (order_id,)).fetchone()
    if row is None or not row["captured"] or row["refunded"]:
        return
    connection.execute("UPDATE payments SET refunded = TRUE WHERE order_id = %s", (order_id,))
    connection.execute("UPDATE order_pipeline SET payment_refunded = 1 WHERE order_id = %s", (order_id,))
    emit(connection, f"payment-{order_id}-refunded", "PaymentRefunded", order_id, {"order_id": order_id, "amount": str(row["amount"])})

def release(connection: Any, order_id: str) -> None:
    row = connection.execute("SELECT sku, quantity, released FROM reservations WHERE order_id = %s FOR UPDATE", (order_id,)).fetchone()
    if row is None or row["released"]:
        return
    connection.execute("UPDATE inventory SET available = available + %s, updated_at = now() WHERE sku = %s", (row["quantity"], row["sku"]))
    connection.execute("UPDATE reservations SET released = TRUE WHERE order_id = %s", (order_id,))
    connection.execute("UPDATE order_pipeline SET stock_released = 1 WHERE order_id = %s", (order_id,))
    emit(connection, f"stock-{order_id}-released", "StockReleased", order_id, {"order_id": order_id, "sku": row["sku"], "quantity": row["quantity"]})

def payment_handler(event: dict[str, Any]) -> None:
    event_type = event["event_type"]
    if event_type not in {"OrderPlaced", "PaymentFailed", "StockFailed"}:
        return
    order_id = event["aggregate_id"]
    with common.connect() as connection:
        with connection.transaction():
            if not claim(connection, GROUPS["payment"], event["event_id"]):
                return
            order = connection.execute("SELECT amount, fail_payment FROM orders WHERE order_id = %s FOR UPDATE", (order_id,)).fetchone()
            state = connection.execute("SELECT stock_failed FROM order_pipeline WHERE order_id = %s FOR UPDATE", (order_id,)).fetchone()
            if order is None or state is None:
                raise RuntimeError(f"missing order {order_id}")
            if event_type == "OrderPlaced":
                if order["fail_payment"]:
                    connection.execute("UPDATE order_pipeline SET payment_failed = 1 WHERE order_id = %s", (order_id,))
                    emit(connection, f"payment-{order_id}-failed", "PaymentFailed", order_id, {"order_id": order_id, "reason": "injected-payment-failure"})
                else:
                    connection.execute("INSERT INTO payments (order_id, amount, captured) VALUES (%s, %s, TRUE) ON CONFLICT (order_id) DO NOTHING", (order_id, order["amount"]))
                    connection.execute("UPDATE order_pipeline SET payment_ok = 1 WHERE order_id = %s", (order_id,))
                    if state["stock_failed"] == 1:
                        refund(connection, order_id)
                    else:
                        emit(connection, f"payment-{order_id}-captured", "PaymentCaptured", order_id, {"order_id": order_id, "amount": str(order["amount"])})
            else:
                if event_type == "PaymentFailed":
                    connection.execute("UPDATE order_pipeline SET payment_failed = 1 WHERE order_id = %s", (order_id,))
                else:
                    connection.execute("UPDATE order_pipeline SET stock_failed = 1 WHERE order_id = %s", (order_id,))
                refund(connection, order_id)

def inventory_handler(event: dict[str, Any]) -> None:
    event_type = event["event_type"]
    if event_type not in {"OrderPlaced", "PaymentFailed", "StockFailed"}:
        return
    order_id = event["aggregate_id"]
    with common.connect() as connection:
        with connection.transaction():
            if not claim(connection, GROUPS["inventory"], event["event_id"]):
                return
            order = connection.execute("SELECT sku, quantity, fail_stock FROM orders WHERE order_id = %s FOR UPDATE", (order_id,)).fetchone()
            state = connection.execute("SELECT payment_failed FROM order_pipeline WHERE order_id = %s FOR UPDATE", (order_id,)).fetchone()
            if order is None or state is None:
                raise RuntimeError(f"missing order {order_id}")
            if event_type == "OrderPlaced":
                updated = None
                if not order["fail_stock"]:
                    updated = connection.execute("UPDATE inventory SET available = available - %s, updated_at = now() WHERE sku = %s AND available >= %s RETURNING sku", (order["quantity"], order["sku"], order["quantity"])).fetchone()
                if updated is None:
                    connection.execute("UPDATE order_pipeline SET stock_failed = 1 WHERE order_id = %s", (order_id,))
                    emit(connection, f"stock-{order_id}-failed", "StockFailed", order_id, {"order_id": order_id, "reason": "injected-stock-failure"})
                else:
                    connection.execute("INSERT INTO reservations (order_id, sku, quantity) VALUES (%s, %s, %s) ON CONFLICT (order_id) DO NOTHING", (order_id, order["sku"], order["quantity"]))
                    connection.execute("UPDATE order_pipeline SET stock_ok = 1 WHERE order_id = %s", (order_id,))
                    if state["payment_failed"] == 1:
                        release(connection, order_id)
                    else:
                        emit(connection, f"stock-{order_id}-reserved", "StockReserved", order_id, {"order_id": order_id, "sku": order["sku"], "quantity": order["quantity"]})
            else:
                if event_type == "PaymentFailed":
                    connection.execute("UPDATE order_pipeline SET payment_failed = 1 WHERE order_id = %s", (order_id,))
                else:
                    connection.execute("UPDATE order_pipeline SET stock_failed = 1 WHERE order_id = %s", (order_id,))
                release(connection, order_id)

def orders_handler(event: dict[str, Any]) -> None:
    if event["event_type"] not in {"PaymentFailed", "StockFailed"}:
        return
    order_id = event["aggregate_id"]
    with common.connect() as connection:
        with connection.transaction():
            if not claim(connection, GROUPS["orders"], event["event_id"]):
                return
            state = connection.execute("SELECT cancelled FROM order_pipeline WHERE order_id = %s FOR UPDATE", (order_id,)).fetchone()
            if state is None:
                raise RuntimeError(f"missing order {order_id}")
            if state["cancelled"] == 0:
                connection.execute("UPDATE order_pipeline SET cancelled = 1 WHERE order_id = %s", (order_id,))
                connection.execute("UPDATE orders SET status = 'cancelled' WHERE order_id = %s", (order_id,))
                emit(connection, f"order-{order_id}-cancelled", "OrderCancelled", order_id, {"order_id": order_id, "reason": event["event_type"]})

def shipping_handler(event: dict[str, Any]) -> None:
    event_type = event["event_type"]
    if event_type not in {"PaymentCaptured", "StockReserved", "PaymentFailed", "StockFailed"}:
        return
    order_id = event["aggregate_id"]
    with common.connect() as connection:
        with connection.transaction():
            if not claim(connection, GROUPS["shipping"], event["event_id"]):
                return
            if event_type == "PaymentCaptured":
                connection.execute("UPDATE order_pipeline SET payment_ok = 1 WHERE order_id = %s", (order_id,))
            elif event_type == "StockReserved":
                connection.execute("UPDATE order_pipeline SET stock_ok = 1 WHERE order_id = %s", (order_id,))
            elif event_type == "PaymentFailed":
                connection.execute("UPDATE order_pipeline SET payment_failed = 1 WHERE order_id = %s", (order_id,))
            else:
                connection.execute("UPDATE order_pipeline SET stock_failed = 1 WHERE order_id = %s", (order_id,))
            state = connection.execute("SELECT payment_ok, stock_ok, payment_failed, stock_failed, shipment_scheduled FROM order_pipeline WHERE order_id = %s FOR UPDATE", (order_id,)).fetchone()
            if state is None or state["payment_failed"] == 1 or state["stock_failed"] == 1:
                return
            if state["payment_ok"] == 1 and state["stock_ok"] == 1 and state["shipment_scheduled"] == 0:
                connection.execute("UPDATE order_pipeline SET shipment_scheduled = 1 WHERE order_id = %s", (order_id,))
                connection.execute("UPDATE orders SET status = 'confirmed' WHERE order_id = %s", (order_id,))
                emit(connection, f"order-{order_id}-confirmed", "OrderConfirmed", order_id, {"order_id": order_id})

def consume(group: str, handler: Callable[[dict[str, Any]], None]) -> None:
    consumer = Consumer({"bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"], "group.id": group, "auto.offset.reset": "earliest", "enable.auto.offset.commit": False})
    consumer.subscribe([TOPIC])
    try:
        while True:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error() is not None:
                raise RuntimeError(str(message.error()))
            handler(json.loads(message.value()))
            consumer.commit(message=message, asynchronous=False)
    finally:
        consumer.close()

def relay_once(connection: Any, producer: Any) -> int:
    with connection.transaction():
        rows = connection.execute("SELECT id, payload FROM saga_outbox WHERE published = FALSE ORDER BY id LIMIT 100 FOR UPDATE SKIP LOCKED").fetchall()
        for row in rows:
            common.publish(producer, TOPIC, row["payload"])
        if rows:
            connection.execute("UPDATE saga_outbox SET published = TRUE WHERE id = ANY(%s)", ([row["id"] for row in rows],))
    return len(rows)

def relay(once: bool) -> None:
    producer = common.new_producer("p06-saga-relay")
    with common.connect() as connection:
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
                time.sleep(0.1 if count == 0 else 0)

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=["relay", "payment", "inventory", "orders", "shipping"])
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()
    if args.mode == "relay":
        relay(args.once)
    else:
        consume(GROUPS[args.mode], {"payment": payment_handler, "inventory": inventory_handler, "orders": orders_handler, "shipping": shipping_handler}[args.mode])

if __name__ == "__main__":
    main()
```

The four `consume` commands are independent groups even though they share a
process. In production they are separate processes and databases. A crash
after a local commit causes redelivery; the group-scoped claim makes the side
effect once-only. The outbox relay publishes only after `common.publish`
acknowledges each row.

## Run and verify

Start the API, relay, and four groups in separate terminals:

```bash
python -m uvicorn workflow:app --host 127.0.0.1 --port 8000
python workflow.py relay
python workflow.py payment
python workflow.py inventory
python workflow.py orders
python workflow.py shipping
```

Happy path:

```bash
curl -sS -X POST http://127.0.0.1:8000/checkout \
  -H 'Content-Type: application/json' \
  -d '{"customer_id":"c-1","sku":"sku-1","quantity":2,"amount":"25.00"}'
sleep 2
docker compose exec -T postgres psql -U app -d app -c \
  "SELECT o.status, p.payment_ok, p.stock_ok, p.shipment_scheduled, p.cancelled
   FROM orders o JOIN order_pipeline p USING (order_id)
   WHERE o.customer_id = 'c-1' ORDER BY o.created_at DESC LIMIT 1;"
```

Deterministic payment failure reserves stock first or later; either arrival
order releases exactly the reservation:

```bash
curl -sS -X POST http://127.0.0.1:8000/checkout \
  -H 'Content-Type: application/json' \
  -d '{"customer_id":"fail-pay","sku":"sku-1","quantity":3,"amount":"30.00","fail_payment":true}'
sleep 2
docker compose exec -T postgres psql -U app -d app -c \
  "SELECT o.status, p.payment_ok, p.stock_ok, p.stock_released, p.cancelled, i.available
   FROM orders o JOIN order_pipeline p USING (order_id)
   JOIN reservations r USING (order_id) JOIN inventory i ON i.sku = r.sku
   WHERE o.customer_id = 'fail-pay';"
```

Deterministic stock failure captures and refunds payment if the handlers run in
either order:

```bash
curl -sS -X POST http://127.0.0.1:8000/checkout \
  -H 'Content-Type: application/json' \
  -d '{"customer_id":"fail-stock","sku":"sku-1","quantity":4,"amount":"40.00","fail_stock":true}'
sleep 2
docker compose exec -T postgres psql -U app -d app -c \
  "SELECT o.status, p.payment_ok, p.payment_refunded, p.stock_failed,
          pay.captured, pay.refunded
   FROM orders o JOIN order_pipeline p USING (order_id)
   JOIN payments pay USING (order_id)
   WHERE o.customer_id = 'fail-stock';"
```

For every failure, assert `cancelled = 1`, released units equal reserved units,
and refunded payment equals captured payment. A stopped owner leaves its event
in Kafka; restart that owner rather than adding a coordinator.

Next: **[P7 · Saga by Orchestration](p07-saga-orchestration.md)**.
