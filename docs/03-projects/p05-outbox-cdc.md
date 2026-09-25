# P5 · Outbox with Debezium CDC

**Read first:** [The Transactional Outbox Pattern](../02-concepts/outbox.md)

P5 keeps P4's business transaction and replaces only the publisher. PostgreSQL
writes the outbox row in the same transaction as the order; Debezium reads the
WAL and routes each insert to the existing `orders.events` topic.

## Shared files and setup

Copy P4's complete `schema.sql`, `api.py`, `inventory_worker.py`, `common.py`, and
outbox contract unchanged. Do not add a `published` update handler and do not
change the event envelope. P4's `relay.py` is not part of the CDC path.

This complete second file is an alternate stack for P5. It keeps the shared
`INTERNAL` and `EXTERNAL` Kafka listeners, and gives the one-broker Connect
cluster replication factor one for all of its internal topics. Use it instead
of starting the shared Compose file at the same time.

```yaml title="compose.connect.yaml"
name: kafka-postgres-lab-cdc

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

  connect:
    image: quay.io/debezium/connect:3.0
    depends_on:
      kafka:
        condition: service_healthy
      postgres:
        condition: service_healthy
    ports:
      - "127.0.0.1:8083:8083"
    environment:
      BOOTSTRAP_SERVERS: kafka:29092
      GROUP_ID: connect-cdc
      CONFIG_STORAGE_TOPIC: connect_configs
      OFFSET_STORAGE_TOPIC: connect_offsets
      STATUS_STORAGE_TOPIC: connect_statuses
      CONFIG_STORAGE_REPLICATION_FACTOR: 1
      OFFSET_STORAGE_REPLICATION_FACTOR: 1
      STATUS_STORAGE_REPLICATION_FACTOR: 1
      CONNECT_TOPIC_CREATION_ENABLE: "true"
      KEY_CONVERTER: org.apache.kafka.connect.storage.StringConverter
      VALUE_CONVERTER: org.apache.kafka.connect.json.JsonConverter
      KEY_CONVERTER_SCHEMAS_ENABLE: "false"
      VALUE_CONVERTER_SCHEMAS_ENABLE: "false"
      REST_ADVERTISED_HOST_NAME: connect
      REST_PORT: "8083"

volumes:
  kafka-data:
  postgres-data:
```

Start it, create the topic, and apply P4's unchanged schema:

```bash
docker compose -f compose.connect.yaml up -d --wait --wait-timeout 180
docker compose -f compose.connect.yaml exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:29092 --create --if-not-exists \
  --topic orders.events --partitions 3 --replication-factor 1 \
  --config min.insync.replicas=1
docker compose -f compose.connect.yaml exec -T postgres \
  psql -v ON_ERROR_STOP=1 -U app -d app < schema.sql
```

The Connect container uses `kafka:29092`; host Python still uses the shared
`KAFKA_BOOTSTRAP_SERVERS=localhost:9092` setting.

## Register the Debezium 3.x connector

The connector is created only after the topic exists. The source topic prefix
is retained for connector bookkeeping, while the Event Router replacement
always sends the outbox payload to the fixed `orders.events` topic.

```bash
curl --fail-with-body -X POST http://127.0.0.1:8083/connectors \
  -H 'Content-Type: application/json' --data-binary @- <<'JSON'
{
  "name": "outbox-orders",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "tasks.max": "1",
    "database.hostname": "postgres",
    "database.port": "5432",
    "database.user": "app",
    "database.password": "app",
    "database.dbname": "app",
    "topic.prefix": "dbserver",
    "plugin.name": "pgoutput",
    "table.include.list": "public.orders_outbox",
    "publication.name": "dbz_publication",
    "publication.autocreate.mode": "filtered",
    "slot.name": "dbz_outbox",
    "slot.drop.on.stop": "false",
    "snapshot.mode": "no_data",
    "skipped.operations": "u,d",
    "tombstones.on.delete": "false",
    "transforms": "outbox",
    "transforms.outbox.type": "io.debezium.transforms.outbox.EventRouter",
    "transforms.outbox.route.by.field": "event_type",
    "transforms.outbox.route.topic.regex": "(.*)",
    "transforms.outbox.route.topic.replacement": "orders.events",
    "transforms.outbox.table.field.event.id": "event_id",
    "transforms.outbox.table.field.event.key": "aggregate_id",
    "transforms.outbox.table.field.event.payload": "payload",
    "transforms.outbox.table.fields.additional.placement": "event_type:header:event_type"
  }
}
JSON
```

Check the exact status and connector configuration:

```bash
curl --fail-with-body http://127.0.0.1:8083/connectors/outbox-orders/status
curl --fail-with-body http://127.0.0.1:8083/connectors/outbox-orders/config
```

A healthy response has connector state `RUNNING` and task state `RUNNING`.
`route.topic.replacement` is the current fixed-topic property;
`route.topic` and the old `database.server.name` setting are not used.
`skipped.operations` is plural. The event ID is P4's `event_id`, and the Kafka
key is P4's `aggregate_id`.

## Start P4's unchanged path

Start P4's API and consumer in separate terminals, then place an order only
after the connector is running:

```bash
python -m uvicorn api:app --host 127.0.0.1 --port 8000
python inventory_worker.py
```

```bash
ORDER_ID="$(curl -sS -X POST http://127.0.0.1:8000/orders \
  -H 'Content-Type: application/json' \
  -d '{"customer_id":"c-1","sku":"sku-1","quantity":2,"amount":"25.00"}' \
  | python -c 'import json,sys; print(json.load(sys.stdin)["order_id"])')"
printf '%s\n' "$ORDER_ID"
docker compose -f compose.connect.yaml exec kafka \
  /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:29092 \
  --topic orders.events --from-beginning --max-messages 1
```

The value is the P4 envelope, not a Debezium wrapper. The connector filters
updates and deletes because P4 later marks a row `published`; otherwise that
maintenance update would become a second routed event. `snapshot.mode=no_data`
also prevents a new connector from replaying every historical outbox row. If a
historical bootstrap is intentional, choose `initial` deliberately and expect
P4's consumer dedup to absorb the replay. A running connector must see the API
insert; an insert made before `no_data` starts is not a stream event.

## WAL, publication, and slot checks

```bash
docker compose -f compose.connect.yaml exec postgres psql -U app -d app \
  -c 'SHOW wal_level;'
docker compose -f compose.connect.yaml exec postgres psql -U app -d app \
  -c 'SELECT slot_name, plugin, slot_type, active, confirmed_flush_lsn FROM pg_replication_slots;'
docker compose -f compose.connect.yaml exec postgres psql -U app -d app \
  -c 'SELECT pubname FROM pg_publication WHERE pubname = '\''dbz_publication'\'';'
docker compose -f compose.connect.yaml exec postgres psql -U app -d app \
  -c 'SELECT application_name, state, sent_lsn, write_lsn FROM pg_stat_replication;'
docker compose -f compose.connect.yaml logs --tail=80 connect
```

The slot retains WAL while the connector is stopped. Deleting a Connect
connector does not drop its PostgreSQL slot. Delete the connector, verify the
slot, and remove it deliberately when it is retired:

```bash
curl --fail-with-body -X DELETE http://127.0.0.1:8083/connectors/outbox-orders
curl --fail-with-body http://127.0.0.1:8083/connectors/outbox-orders/status
  || true
docker compose -f compose.connect.yaml exec postgres psql -U app -d app \
  -c "SELECT pg_drop_replication_slot('dbz_outbox');"
```

## Verify P4 consumer deduplication

CDC can replay an event after a connector or consumer restart. The unchanged
P4 `inventory_worker.py` must still write one adjustment. This deliberately
uses P4's relay once, only as a duplicate generator; it is not the P5 runtime.

```bash
docker compose -f compose.connect.yaml exec -T postgres \
  psql -v ON_ERROR_STOP=1 -U app -d app \
  -c "UPDATE orders_outbox SET published = FALSE WHERE aggregate_id = '$ORDER_ID';"
timeout 5s python relay.py || test "$?" -eq 124
docker compose -f compose.connect.yaml exec -T postgres \
  psql -v ON_ERROR_STOP=1 -U app -d app \
  -c "SELECT event_id, count(*) AS adjustments FROM inventory_adjustments WHERE event_id = 'order-$ORDER_ID-placed' GROUP BY event_id;"
docker compose -f compose.connect.yaml exec -T postgres \
  psql -v ON_ERROR_STOP=1 -U app -d app \
  -c "SELECT consumer_group, event_id FROM processed_events WHERE event_id = 'order-$ORDER_ID-placed';"
```

The first query returns one row with `adjustments = 1`, and the processed-event
claim exists. A second CDC emission is therefore harmless. The source key keeps
all events for one order on one partition; it does not create a global order.

## Operational notes

- Stopping Connect pauses WAL retention at the slot, not event delivery. Monitor
  slot age and WAL bytes before deleting a connector.
- A schema change can require connector review. Test it before changing the
  outbox columns; the P4 contract is intentionally unchanged here.
- P4's `published` flag is not a CDC acknowledgement and stays `FALSE` in this
  path. Do not run a polling relay alongside the connector.

Next: **[P6 · Saga by Choreography](p06-saga-choreography.md)**.
