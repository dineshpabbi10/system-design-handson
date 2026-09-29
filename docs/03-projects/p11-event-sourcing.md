# P11 · Event-Sourced Bank

**Read first:** [Event sourcing & CQRS](../02-concepts/event-sourcing-cqrs.md) →
[Idempotency](../02-concepts/idempotency.md)

The log is the truth; `balance` is a fold. Commands append versioned events
with optimistic concurrency, one transaction also claims the command and
queues the outbox row. Projections rebuild from the log.

## What you build

```mermaid
flowchart LR
    API["bank.py deposit/withdraw"] -->|"claim + append + outbox"| E[("account_events")]
    E -->|"project"| B[("balance_view")]
    E -->|"relay"| K["account_events topic"]
    K --> L["ledger_consumer.py"]
```

### How the flow works

The log is the truth; every balance is a fold over it. Three IDs keep the three concerns separate.

1. **`command_id` is the caller's retry identity.** `cmd-acct-1-deposit-1` comes from the caller and is stable across retries. `INSERT INTO processed_commands … ON CONFLICT DO NOTHING` claims it: the first attempt executes, a retried `command_id` returns the stored `response` with no new event. This is P3's idempotency key, renamed for commands.
2. **`event_version` is the account's sequence.** `next_version = MAX(version) + 1` proposes version N, then `INSERT INTO account_events (account_id, event_version, …)` with `UNIQUE (account_id, event_version)` decides the race. Two concurrent deposits both propose v3; one wins, the loser gets `UniqueViolation` and retries as v4. No `SELECT`-then-trust — the constraint is the concurrency control.
3. **Why `event_id = {account_id}-v{version}`.** It is derived from the winner's version, so a version retry gets a new deterministic identity (`acct-1-v2`, not a reused UUID) while a command retry reuses nothing new at all. `aggregate_id = account_id` is the Kafka key (one account → one partition) and the fold scope.
4. **One transaction writes three things.** Claim command + append event + `INSERT INTO account_outbox` commit together. `Decimal` math is strict: deposits add `abs(delta)`, withdrawals subtract it; amounts must be positive and the sign comes from the command type, never from a client-supplied negative.
5. **Two projections, same fold.** `projector.py --account acct-1` folds `account_events` into `balance_view` (incremental: from stored version; `--full` deletes and refolds everything — both must agree at `70.00`). `relay.py` publishes the outbox to the `account_events` topic and `ledger_consumer.py` (group `ledger-projector`) folds the same events into `ledger_projection(balance_after)` with its own `(group, event_id)` claim. `balance_view` is the query cache; `ledger_projection` proves the Kafka path converges; `account_events` stays authoritative when they disagree.

| Skill | Learned by |
|-------|-----------|
| Versioned event store | `UNIQUE (account_id, event_version)` |
| Idempotent commands | stable `command_id`, `ON CONFLICT` claim |
| Optimistic retry | version conflict, refold, retry |
| Correct money math | `Decimal`, deposit positive, withdraw negative |
| Replay | full plus incremental projector |

## Setup

```bash
export DATABASE_URL='postgresql://app:app@localhost:5432/app'
export KAFKA_BOOTSTRAP_SERVERS='localhost:9092'
docker compose up -d --wait --wait-timeout 180
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:29092 --create --if-not-exists \
  --topic account_events --partitions 3 --replication-factor 1 \
  --config min.insync.replicas=1
docker compose exec -T postgres psql -v ON_ERROR_STOP=1 -U app -d app < schema.sql
```

## 1. `schema.sql`

```sql title="schema.sql"
-- Event store: append-only log; version guard is the optimistic lock.
CREATE TABLE IF NOT EXISTS account_events (id BIGSERIAL PRIMARY KEY, account_id TEXT NOT NULL, event_version INTEGER NOT NULL CHECK (event_version > 0), event_id TEXT NOT NULL UNIQUE, event_type TEXT NOT NULL CHECK (event_type IN ('AccountOpened', 'Deposited', 'Withdrawn')), payload JSONB NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT now(), UNIQUE (account_id, event_version));
-- Cached projection: rebuilt by folding the log; version tracks progress.
CREATE TABLE IF NOT EXISTS balance_view (account_id TEXT PRIMARY KEY, balance NUMERIC(12,2) NOT NULL, event_version INTEGER NOT NULL, updated_at TIMESTAMPTZ NOT NULL DEFAULT now());
-- Command dedup: stable command_id -> stored response for replays.
CREATE TABLE IF NOT EXISTS processed_commands (command_id TEXT PRIMARY KEY, response JSONB NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT now());
-- Outbox for Kafka publishing; relay marks published after ack.
CREATE TABLE IF NOT EXISTS account_outbox (id BIGSERIAL PRIMARY KEY, aggregate_id TEXT NOT NULL, event_id TEXT NOT NULL UNIQUE, payload JSONB NOT NULL, published BOOLEAN NOT NULL DEFAULT FALSE, created_at TIMESTAMPTZ NOT NULL DEFAULT now());
CREATE INDEX IF NOT EXISTS account_outbox_pending_idx ON account_outbox (published, id) WHERE published = FALSE;
-- Consumer dedup for the Kafka projection below.
CREATE TABLE IF NOT EXISTS processed_events (consumer_group TEXT NOT NULL, event_id TEXT NOT NULL, claimed_at TIMESTAMPTZ NOT NULL DEFAULT now(), PRIMARY KEY (consumer_group, event_id));
-- Kafka-side fold: running balance per version.
CREATE TABLE IF NOT EXISTS ledger_projection (account_id TEXT NOT NULL, event_version INTEGER NOT NULL, event_id TEXT NOT NULL, delta NUMERIC(12,2) NOT NULL, balance_after NUMERIC(12,2) NOT NULL, PRIMARY KEY (account_id, event_version));
```

## 2. `bank.py`: claim, append, outbox in one transaction

`command_id` comes from the caller and is stable across retries. `event_id`
is `account_id` plus the attempted version, so a version retry gets a new
deterministic event id without reusing the caller's key.

```python title="bank.py"
import argparse
import json
import os
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

import psycopg
import psycopg.errors
from confluent_kafka import Producer
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

GROUP = "bank-api"


# --- Explicit init helpers (no shared module; repeated to teach init) ---
def connect_db():
    # Postgres with dict rows; one connection per command attempt.
    return psycopg.connect(os.environ["DATABASE_URL"], row_factory=dict_row)


def new_producer(client_id: str) -> Producer:
    # Idempotent producer used by the relay below.
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


def next_version(connection: Any, account_id: str) -> int:
    # Read max version; UNIQUE guard below decides the race, not this read.
    row = connection.execute(
        "SELECT COALESCE(MAX(event_version), 0) AS max_version "
        "FROM account_events WHERE account_id = %s",
        (account_id,),
    ).fetchone()
    return int(row["max_version"]) + 1


def apply_command(account_id: str, command_type: str, amount: Decimal, command_id: str) -> dict[str, Any]:
    # Idempotent append: claim command, compute version, insert event + outbox.
    if command_type not in ("Deposited", "Withdrawn"):
        raise ValueError(f"unknown command {command_type}")
    if amount <= Decimal("0"):
        raise ValueError("amount must be positive")
    for attempt in range(5):  # optimistic retry loop on version conflict
        try:
            with connect_db() as connection:
                with connection.transaction():
                    claimed = connection.execute(
                        "INSERT INTO processed_commands (command_id, response) "
                        "VALUES (%s, %s) ON CONFLICT (command_id) DO NOTHING "
                        "RETURNING command_id",
                        (command_id, Jsonb({"status": "in_progress"})),
                    ).fetchone()
                    if claimed is None:
                        existing = connection.execute(
                            "SELECT response FROM processed_commands WHERE command_id = %s",
                            (command_id,),
                        ).fetchone()
                        return dict(existing["response"])
                    version = next_version(connection, account_id)
                    event_id = f"{account_id}-v{version}"
                    if command_type == "Deposited":
                        delta = amount
                    else:
                        delta = -amount
                    payload = {
                        "event_id": event_id,
                        "event_type": command_type,
                        "aggregate_type": "account",
                        "aggregate_id": account_id,
                        "data": {
                            "account_id": account_id,
                            "amount": str(amount),
                            "delta": str(delta),
                            "event_version": version,
                        },
                    }
                    connection.execute(
                        "INSERT INTO account_events "
                        "(account_id, event_version, event_id, event_type, payload) "
                        "VALUES (%s, %s, %s, %s, %s)",
                        (account_id, version, event_id, command_type, Jsonb(payload)),
                    )
                    connection.execute(
                        "INSERT INTO account_outbox (aggregate_id, event_id, payload) "
                        "VALUES (%s, %s, %s) ON CONFLICT (event_id) DO NOTHING",
                        (account_id, event_id, Jsonb(payload)),
                    )
                    response = {"status": "applied", "event_id": event_id, "event_version": version}
                    connection.execute(
                        "UPDATE processed_commands SET response = %s WHERE command_id = %s",
                        (Jsonb(response), command_id),
                    )
                    return response
        except psycopg.errors.UniqueViolation:
            if attempt == 4:
                raise RuntimeError("concurrent write conflict after retries")
            continue
    raise RuntimeError("unreachable")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=["deposit", "withdraw"])
    parser.add_argument("--account", required=True)
    parser.add_argument("--amount", required=True)
    parser.add_argument("--command-id", required=True)
    args = parser.parse_args()
    command_type = "Deposited" if args.command == "deposit" else "Withdrawn"
    result = apply_command(args.account, command_type, Decimal(args.amount), args.command_id)
    print(result)


if __name__ == "__main__":
    main()
```

The `UNIQUE (account_id, event_version)` row is the optimistic guard. Two
concurrent writers read the same max, one insert wins, the loser gets
`UniqueViolation` and retries with a fresh version. No `SELECT`-then-insert
guard is used; the constraint decides the race.

Run the relay in another terminal with `python relay.py`:

```python title="relay.py"
import json
import os
import time
from typing import Any

import psycopg
from confluent_kafka import Producer
from psycopg.rows import dict_row

TOPIC = "account_events"


def connect_db():
    # Long-lived connection for the polling loop.
    return psycopg.connect(os.environ["DATABASE_URL"], row_factory=dict_row)


def new_producer(client_id: str) -> Producer:
    # Idempotent producer: relay restarts can safely republish pending rows.
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
    # Keyed produce + flush; raises on broker error so row stays pending.
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


def main() -> None:
    # Poll outbox, publish each pending row, mark only after ack.
    producer = new_producer("p11-account-relay")
    with connect_db() as connection:
        while True:
            with connection.transaction():
                # SKIP LOCKED lets a second relay take different rows.
                rows = connection.execute("SELECT id, aggregate_id, payload FROM account_outbox WHERE published = FALSE ORDER BY id LIMIT 100 FOR UPDATE SKIP LOCKED").fetchall()
                for row in rows:
                    publish_event(producer, TOPIC, row["payload"])
                if rows:
                    connection.execute("UPDATE account_outbox SET published = TRUE WHERE id = ANY(%s)", ([row["id"] for row in rows],))
            time.sleep(0.1 if not rows else 0)


if __name__ == "__main__":
    main()
```

## 3. `projector.py`: incremental and full

```python title="projector.py"
import argparse
import os
from decimal import Decimal
from typing import Any

import psycopg
from psycopg.rows import dict_row


def connect_db():
    # Postgres with dict rows for folding the event log.
    return psycopg.connect(os.environ["DATABASE_URL"], row_factory=dict_row)


def fold_account(connection: Any, account_id: str, from_version: int) -> tuple[Decimal, int]:
    rows = connection.execute(
        "SELECT event_type, payload, event_version FROM account_events "
        "WHERE account_id = %s AND event_version > %s ORDER BY event_version",
        (account_id, from_version),
    ).fetchall()
    balance = Decimal("0")
    version = from_version
    base = connection.execute(
        "SELECT balance, event_version FROM balance_view WHERE account_id = %s",
        (account_id,),
    ).fetchone()
    if base is not None and from_version == 0:
        balance = Decimal(str(base["balance"]))
        version = int(base["event_version"])
    elif base is not None:
        balance = Decimal(str(base["balance"]))
        version = int(base["event_version"])
    for row in rows:
        payload = row["payload"]
        data = payload["data"] if isinstance(payload, dict) and "data" in payload else {}
        delta = Decimal(str(data.get("delta", "0")))
        if row["event_type"] == "Deposited":
            balance += abs(delta)
        elif row["event_type"] == "Withdrawn":
            balance -= abs(delta)
        version = int(row["event_version"])
    return balance, version


def project_account(account_id: str) -> None:
    with connect_db() as connection:
        with connection.transaction():
            current = connection.execute(
                "SELECT event_version FROM balance_view WHERE account_id = %s",
                (account_id,),
            ).fetchone()
            stored = int(current["event_version"]) if current is not None else 0
            balance, version = fold_account(connection, account_id, stored)
            if version == stored and current is not None:
                return
            connection.execute(
                "INSERT INTO balance_view (account_id, balance, event_version) "
                "VALUES (%s, %s, %s) ON CONFLICT (account_id) DO UPDATE SET "
                "balance = EXCLUDED.balance, event_version = EXCLUDED.event_version, "
                "updated_at = now()",
                (account_id, balance, version),
            )


def rebuild_full() -> None:
    with connect_db() as connection:
        accounts = connection.execute(
            "SELECT DISTINCT account_id FROM account_events ORDER BY account_id"
        ).fetchall()
    for row in accounts:
        with connect_db() as connection:
            with connection.transaction():
                connection.execute("DELETE FROM balance_view WHERE account_id = %s", (row["account_id"],))
        with connect_db() as connection:
            with connection.transaction():
                rows = connection.execute(
                    "SELECT event_type, payload, event_version FROM account_events "
                    "WHERE account_id = %s ORDER BY event_version",
                    (row["account_id"],),
                ).fetchall()
                balance = Decimal("0")
                version = 0
                for item in rows:
                    payload = item["payload"]
                    data = payload["data"] if isinstance(payload, dict) and "data" in payload else {}
                    delta = Decimal(str(data.get("delta", "0")))
                    if item["event_type"] == "Deposited":
                        balance += abs(delta)
                    elif item["event_type"] == "Withdrawn":
                        balance -= abs(delta)
                    version = int(item["event_version"])
                connection.execute(
                    "INSERT INTO balance_view (account_id, balance, event_version) "
                    "VALUES (%s, %s, %s) ON CONFLICT (account_id) DO UPDATE SET "
                    "balance = EXCLUDED.balance, event_version = EXCLUDED.event_version, "
                    "updated_at = now()",
                    (row["account_id"], balance, version),
                )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--account")
    parser.add_argument("--full", action="store_true")
    args = parser.parse_args()
    if args.full:
        rebuild_full()
        print("full rebuild done")
    elif args.account:
        project_account(args.account)
        print(f"projected {args.account}")
    else:
        raise SystemExit("pass --account ID or --full")


if __name__ == "__main__":
    main()
```

Deposits add `abs(delta)`; withdrawals subtract `abs(delta)`. Both paths
start `balance` at `Decimal("0")`, never an undefined variable.

## 4. `ledger_consumer.py`: Kafka projection

```python title="ledger_consumer.py"
import json
import os
from decimal import Decimal

import psycopg
from confluent_kafka import Consumer
from psycopg.rows import dict_row

TOPIC = "account_events"
GROUP = "ledger-projector"


def connect_db():
    # Per-message connection for the claim + projection transaction.
    return psycopg.connect(os.environ["DATABASE_URL"], row_factory=dict_row)


def new_consumer(group_id: str) -> Consumer:
    # Manual-commit consumer; offset follows the projection transaction.
    return Consumer(
        {
            "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.offset.commit": False,
        }
    )


def main() -> None:
    # Fold Kafka events into ledger_projection; claim makes replay safe.
    consumer = new_consumer(GROUP)
    consumer.subscribe([TOPIC])
    try:
        while True:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error() is not None:
                raise RuntimeError(str(message.error()))
            event = json.loads(message.value())  # outbox envelope from relay
            event_id = event["event_id"]  # dedup key
            data = event["data"]  # delta + event_version live here
            account_id = event["aggregate_id"]  # Kafka key + fold scope
            version = int(data["event_version"])
            delta = Decimal(str(data["delta"]))
            with connect_db() as connection:
                with connection.transaction():  # claim + fold = atomic
                    claimed = connection.execute(
                        "INSERT INTO processed_events (consumer_group, event_id) "
                        "VALUES (%s, %s) ON CONFLICT (consumer_group, event_id) "
                        "DO NOTHING RETURNING event_id",
                        (GROUP, event_id),
                    ).fetchone()
                    if claimed is None:  # duplicate delivery
                        consumer.commit(message=message, asynchronous=False)
                        continue
                    previous = connection.execute(
                        "SELECT balance_after FROM ledger_projection "
                        "WHERE account_id = %s ORDER BY event_version DESC LIMIT 1",
                        (account_id,),
                    ).fetchone()
                    running = Decimal(str(previous["balance_after"])) if previous else Decimal("0")
                    running += delta  # fold: apply signed delta
                    connection.execute(
                        "INSERT INTO ledger_projection "
                        "(account_id, event_version, event_id, delta, balance_after) "
                        "VALUES (%s, %s, %s, %s, %s) "
                        "ON CONFLICT (account_id, event_version) DO NOTHING",
                        (account_id, version, event_id, delta, running),
                    )
            consumer.commit(message=message, asynchronous=False)  # checkpoint
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
```

## 5. Run, replay, assert

Start `python relay.py` and `python ledger_consumer.py` in separate terminals,
then run the bank and projector commands below.

```bash
python bank.py deposit --account acct-1 --amount 100.00 --command-id cmd-acct-1-deposit-1
python bank.py withdraw --account acct-1 --amount 30.00 --command-id cmd-acct-1-withdraw-1
python bank.py deposit --account acct-1 --amount 100.00 --command-id cmd-acct-1-deposit-1
python projector.py --account acct-1
docker compose exec -T postgres psql -U app -d app \
  -c "SELECT account_id, balance, event_version FROM balance_view WHERE account_id = 'acct-1';"
docker compose exec -T postgres psql -U app -d app \
  -c "SELECT account_id, event_version, event_id FROM account_events WHERE account_id = 'acct-1' ORDER BY event_version;"
python projector.py --full
docker compose exec -T postgres psql -U app -d app \
  -c "SELECT account_id, balance, event_version FROM balance_view WHERE account_id = 'acct-1';"
docker compose exec -T postgres psql -U app -d app \
  -c "SELECT count(*) AS outbox_pending FROM account_outbox WHERE published = FALSE;"
```

Expected results: the repeated `command-id` returns the first stored
response with no new version; `balance_view` shows `70.00` at version 2
before and after `--full`; the event list has versions 1 and 2 with distinct
`acct-1-v1` and `acct-1-v2` ids; relaying the outbox then projecting in the
ledger consumer converges to the same `70.00`.

## Checkpoints

??? question "Why is the constraint the concurrency control?"
    Both writers race to insert the next version; the loser gets a conflict
    and retries. A pre-check would have a check-to-insert race.

??? question "What does replay prove?"
    Full rebuild from `account_events` must equal the incremental view. If it
    does not, the projector has a bug; the log stays authoritative.

## Done when

- [ ] Duplicate `command_id` appends nothing and returns the stored response
- [ ] Concurrent writers resolve by version retry with correct `Decimal` math
- [ ] Incremental and full projections agree at `70.00`
- [ ] Ledger consumer converges once the outbox relay publishes

Next: **[P12 · Capstone: E-Commerce Platform](p12-capstone.md)**
