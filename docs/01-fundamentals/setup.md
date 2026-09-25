# Local Environment Setup

Every project uses the same stack and code shape. The project pages provide complete
files for the pattern being taught; they do not depend on a hidden `code/` directory.

## Stack contract

| Concern | Choice |
|---------|--------|
| Python | 3.10+ |
| HTTP | FastAPI with synchronous `def` handlers |
| Kafka client | `confluent-kafka` |
| PostgreSQL client | psycopg 3 DBAPI |
| Database access | Raw SQL with `%s` parameters |
| Rows and JSONB | `dict_row` and `psycopg.types.json.Jsonb` |
| ORM | None: no SQLAlchemy or SQLModel |
| Broker | Local single-node Kafka in KRaft mode |
| Consumer offsets | Manual commits after the local processing step |

Synchronous FastAPI handlers avoid mixing blocking psycopg calls with `async def`.
Workers are ordinary Python processes.

## 1. Create the environment

```bash
mkdir kafka-postgres-lab
cd kafka-postgres-lab
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install fastapi "uvicorn[standard]" confluent-kafka \
  "psycopg[binary]" httpx pytest
```

The matching project dependency is:

```text title="requirements.txt"
fastapi
uvicorn[standard]
confluent-kafka
psycopg[binary]
httpx
pytest
```

Export the same connection settings in every terminal used for a project:

```bash
export DATABASE_URL='postgresql://app:app@localhost:5432/app'
export KAFKA_BOOTSTRAP_SERVERS='localhost:9092'
```

## 2. Create `compose.yaml`

Host applications connect through `localhost:9092`. Docker-network services in P5,
P9, and P12 connect through `kafka:29092`.

```yaml title="compose.yaml"
name: kafka-postgres-lab

services:
  kafka:
    image: apache/kafka:3.8.1
    environment:
      CLUSTER_ID: MkU3OEVBNTcwNTJENDM2Qk
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT
      KAFKA_LISTENERS: INTERNAL://0.0.0.0:29092,EXTERNAL://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093
      KAFKA_ADVERTISED_LISTENERS: INTERNAL://kafka:29092,EXTERNAL://localhost:9092
      KAFKA_INTER_BROKER_LISTENER_NAME: INTERNAL
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_DEFAULT_REPLICATION_FACTOR: 1
      KAFKA_MIN_INSYNC_REPLICAS: 1
      KAFKA_NUM_PARTITIONS: 3
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"
      KAFKA_LOG_DIRS: /var/lib/kafka/data
    ports:
      - "127.0.0.1:9092:9092"
    volumes:
      - kafka-data:/var/lib/kafka/data
    healthcheck:
      test: ["CMD-SHELL", "/opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --list >/dev/null 2>&1"]
      interval: 5s
      timeout: 10s
      retries: 24

  postgres:
    image: postgres:16
    environment:
      POSTGRES_USER: app
      POSTGRES_PASSWORD: app
      POSTGRES_DB: app
    command:
      - postgres
      - -c
      - wal_level=logical
      - -c
      - max_replication_slots=10
      - -c
      - max_wal_senders=10
    ports:
      - "127.0.0.1:5432:5432"
    volumes:
      - postgres-data:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U app -d app"]
      interval: 5s
      timeout: 5s
      retries: 20

volumes:
  kafka-data:
  postgres-data:
```

Start and verify the stack:

```bash
docker compose up -d --wait --wait-timeout 180
docker compose ps
docker compose exec postgres psql -U app -d app -c 'SHOW wal_level;'
```

`SHOW wal_level` returns `logical`. The base stack is a deterministic local lab,
not a highly available Kafka deployment: replication factor one has no failover
replica.

## 3. Shared project files

Create this small tree inside the current project. Later pages either show a
replacement file in full or tell you to copy the shared file unchanged.

```text
project-name/
  compose.yaml
  requirements.txt
  common.py
  schema.sql
  producer.py
  consumer.py
```

The pages use these exact database and Kafka conventions:

- `psycopg.connect(os.environ["DATABASE_URL"], row_factory=dict_row)`
- `%s` placeholders; never `$1`, `:name`, or interpolated values
- `Jsonb(value)` for `JSONB` parameters
- `with connection.transaction():` around one database unit of work
- `consumer.commit(message=message, asynchronous=False)` only after processing
- `acks=all` and `enable.idempotence=true` for application producers
- a delivery callback plus `flush()` before treating a produce as successful

## 4. Shared `common.py`

Copy this complete helper into each project that uses PostgreSQL or Kafka. Project
files import these functions rather than inventing an async database wrapper.

```python title="common.py"
from datetime import datetime, timezone
import json
import os
from typing import Any

from confluent_kafka import Producer
import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb


def connect() -> psycopg.Connection:
    return psycopg.connect(
        os.environ["DATABASE_URL"],
        row_factory=dict_row,
    )


def make_event(
    event_id: str,
    event_type: str,
    aggregate_type: str,
    aggregate_id: str,
    data: dict[str, Any],
) -> dict[str, Any]:
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
        raise TimeoutError(f"{remaining} Kafka record(s) remain undelivered")
    if errors:
        raise RuntimeError(errors[0])
    producer.poll(0)


def jsonb(value: Any) -> Jsonb:
    return Jsonb(value)
```

`make_event` requires the caller to supply a stable `event_id`. A random event ID
inside a handler defeats deduplication when that handler is retried. Each project
shows where its event ID comes from.

## 5. Create and inspect a topic

Topics are explicit because auto-creation makes partition counts and replication
factors implicit.

```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:29092 \
  --create --if-not-exists \
  --topic orders.events \
  --partitions 3 \
  --replication-factor 1 \
  --config min.insync.replicas=1

docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:29092 \
  --describe \
  --topic orders.events
```

A stable Kafka key is the event's `aggregate_id`. With a fixed topic partition
count and partitioner, the same key maps to the same partition. Do not predict the
partition number with Python's `hash()`.

## 6. Apply and reset a schema

Each project supplies its complete `schema.sql`. Apply it from the project root:

```bash
docker compose exec -T postgres psql -v ON_ERROR_STOP=1 -U app -d app < schema.sql
```

For a disposable lab reset, stop the Python processes and run:

```bash
docker compose down -v
docker compose up -d --wait --wait-timeout 180
```

Removing volumes also removes Kafka topics, consumer offsets, and PostgreSQL data.

## Checkpoint

??? question "Which database API do the project snippets use?"
    !!! success "Expected result"
        They use `psycopg.connect`, raw SQL, `%s` parameters, `dict_row`, `Jsonb`,
        and explicit transactions. They do not use SQLAlchemy, SQLModel, psycopg2,
        asyncpg, or an undefined `db.fetchrow()` abstraction.

??? question "Why is the Kafka key the aggregate ID?"
    !!! success "Expected result"
        All events for one entity stay on one partition while its partition count
        stays fixed. This gives per-entity order; it does not give global order.

## Learn more

- **Docs** — [Psycopg 3 usage](https://www.psycopg.org/psycopg3/docs/basic/index.html)
- **Docs** — [Confluent's Python client](https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html)

Next: [Delivery Semantics](../02-concepts/delivery-semantics.md).
