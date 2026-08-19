# Local Environment Setup

Everything in this guide runs locally on Docker. The stack grows as you progress:

| Project stages | Services |
|----------------|----------|
| P1 – P4 | Kafka (KRaft, 1 broker) + PostgreSQL |
| P5 | + Debezium (CDC) |
| P9 | + Schema Registry |
| P12 | + Jaeger, OpenTelemetry collector |

## Base Docker Compose

```yaml title="docker-compose.yml" linenums="1"
services:
  kafka:
    image: apache/kafka:3.8.0
    container_name: kafka
    ports:
      - "9092:9092"            # client access from your host
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"
    volumes:
      - kafka-data:/var/lib/kafka/data
  postgres:
    image: postgres:16
    container_name: postgres
    environment:
      POSTGRES_USER: app
      POSTGRES_PASSWORD: app
      POSTGRES_DB: app
    ports:
      - "5432:5432"

volumes:
  kafka-data:
```

```bash
docker compose up -d
docker compose ps          # both healthy
docker compose logs -f kafka
```

!!! note "Controlled chaos"
    This is a **single broker** with replication factor 1 — fast and simple, *not*
    durable. Production-grade behavior (ISR failover, `acks=all`) is still explained
    conceptually and exercised in break-it experiments where possible with a
    3-node `kraft-combined` setup (add brokers 2 and 3 porting the pattern above).
    Multi-broker compose files are included in the code recipes for P2/P10.

## Verify Kafka works

```bash
# inside the container, or use kcat (recommended, see below)
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

# interactive: type lines, see them arrive on the other side
docker exec -it kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic quickstart
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic quickstart --from-beginning
```

### kcat (a.k.a. kafkacat) — your Swiss army knife

```bash
brew install kcat
kcat -b localhost:9092 -t orders -P -K: <<<'key1:{"order":1}'   # produce with key
kcat -b localhost:9092 -t orders -C -K: -f 'key=%k value=%s\n'   # consume, show keys
kcat -b localhost:9092 -L                                           # list topics & partitions
kcat -b localhost:9092 -C -t orders -o -5 -e                         # last 5 records, exit
```

## Python environment

Each project uses the same pattern. Only `confluent-kafka` is non-obvious.

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install fastapi "uvicorn[standard]" confluent-kafka sqlalchemy psycopg2-binary \
    httpx pytest "tenacity" python-json-logger
```

!!! tip
    Any consumer in this course runs `enable.auto.offset.commit=false` and commits
    **manually after processing** from day one. You will understand exactly why by
    P3, and it will save you from silent message loss in every later project.

## Project skeleton (shared from P1 onward)

```
code/
  pXX-name/
    docker-compose.yml        # services for that project
    services/
      <service>/app.py        # FastAPI entrypoint
      <service>/kafka_cfg.py  # producer/consumer config helpers
    scripts/
      demo.sh                 # demonstrates the happy path
      break_it.sh             # the "prove it" failure experiments
    tests/                    # pytest, run against the live stack
```

Doing it yourself for P1 in your own repo first is the recommended hands-on path;
the `code/` folder has reference implementations.

## Checkpoint

??? question "1. Start the stack. Create topic `test-orders` with 4 partitions via `kafka-topics.sh` — how do you know it worked?"
    !!! success "Expected result"
        `docker compose up -d` brings up `kafka` and `postgres` as healthy, and
        after running
        `docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
        --create --topic test-orders --partitions 4 --replication-factor 1`
        you see:
        ```
        Created topic test-orders.
        ```
        Verify with `kafka-topics.sh ... --describe --topic test-orders` → it lists
        **4 partitions** (0–3), each with 1 replica. `kcat -L` shows the same.

??? question "2. Produce 10 messages with the same key and consume them — note the partition column."
    !!! success "Expected result"
        All 10 land on **one partition** (say `partition=1`), with offsets
        **0, 1, 2 … 9** in produce order. That's `hash(key) % 3` in action — same
        key → same partition → strict order. (With the console producer, prefix
        each line with `key:` the first time you use it in a session to send a key;
        with `kcat` use `-K:`.)

??? question "3. Produce 10 messages with *no* key. Which partitions do they land on? Why?"
    !!! success "Expected result"
        They spread across the 3 partitions (roughly round-robin) with **no
        meaningful order between partitions**. With no key, librdkafka picks a
        partition by round-robin/sticky balancing instead of hashing — so ordering
        between any two of these messages is **not guaranteed** by the broker.
        This is exactly why "no key" is a decision, not a default, for entity
        event streams (see [ordering](../02-concepts/ordering.md)).

## Learn more

- **Docs** — [Apache Kafka Quickstart](https://kafka.apache.org/quickstart) — the official single-broker walkthrough (what this page's compose approximates).
- **Watch** — [Apache Kafka 101 (Confluent Developer)](https://developer.confluent.io/courses/apache-kafka/events/) — "Your First Kafka Application" module runs the same producer/consumer steps.
- **Docs** — [Confluent Developer — Python client guide](https://developer.confluent.io/languages/python/) — `confluent_kafka` API for everything the projects build.

Next: [Delivery Semantics](../02-concepts/delivery-semantics.md) — what the broker
actually promises, and the choice between at-most-once and at-least-once.