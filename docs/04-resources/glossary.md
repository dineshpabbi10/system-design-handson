# Glossary

Quick reference in the guide's exact vocabulary. Definitions match how the terms are
used on the concept pages.

## Broker & cluster

| Term | Meaning |
|------|---------|
| **Broker** | A Kafka server; stores partition data, serves producers/consumers |
| **Cluster** | The set of brokers running Kafka together |
| **Controller** | Broker that manages partition leadership (KRaft metadata quorum is the modern controller) |
| **KRaft** | Kafka's own consensus protocol replacing ZooKeeper (since 3.x) |
| **Replication factor** | How many brokers hold a copy of a partition (default 1 — never in prod) |
| **ISR (In-Sync Replicas)** | Followers caught up with the leader; only ISR can become leader |
| `min.insync.replicas` | Min ISR count for a produce ack with `acks=all` |
| **Leader / follower** | The replica that serves reads+writes / the replicas replicating from it |

## Topics & records

| Term | Meaning |
|------|---------|
| **Topic** | Named, partitioned, append-only log |
| **Partition** | Ordered sub-log; the unit of parallelism and ordering |
| **Offset** | Position of a record within its partition (starts at 0) |
| **Key** | Message bytes deciding the partition: `hash(key) % partitions` |
| **Record batch** | Group of records produced together; the unit of commits |
| **Retention** | How long the broker keeps records (`retention.ms`, size caps) |
| **Compacted topic** | Keeps the latest value per key (log compaction) |
| **Headers** | Key-value metadata on a record (trace ids, error info, attempts) |

## Producer / consumer

| Term | Meaning |
|------|---------|
| `acks` | Durability of produce: 0 / 1 / all |
| **Idempotent producer** | Broker-level dedup via (producer_id, sequence) |
| **Transactional producer** | Atomic commit across produce(s) + consumed offsets (P10) |
| **Consumer group** | Set of consumers sharing partitions of a topic |
| **Rebalance** | Redistribution of partitions when a member joins/leaves |
| **Committed offset** | Last position checkpointed to `__consumer_offsets` |
| **Lag** | `LOG-END-OFFSET - CURRENT-OFFSET` per partition; the health metric |
| `max.poll.interval.ms` | Max time between polls before the group kicks a member |
| `auto.offset.reset` | Where a *new* group starts: earliest / latest |

## Semantics & patterns

| Term | Meaning |
|------|---------|
| **At-most-once** | Commit before process → loss on crash |
| **At-least-once** | Process before commit → duplicates on crash/rebalance (**Kafka default**) |
| **Exactly-once (EOS)** | Broker/producer + Streams only; everything else = at-least-once + idempotency |
| **Idempotency key** | Client-supplied unique id; server dedups and replays the stored response |
| `processed_events` | Consumer-side dedup table keyed by event id |
| **Dual-write problem** | "Write DB, then publish" — the atomicity hole |
| **Outbox table** | Events written atomically with business data; relay publishes them |
| **Polling publisher** | Relay: poll unpublished rows, publish (acks=all), then mark |
| **CDC** | Change Data Capture (Debezium): WAL → events, the no-code relay |
| **Saga** | Chain of local transactions + compensations; no global lock |
| **Choreography** | Sagas by events only; no coordinator |
| **Orchestration** | Sagas with a central durable state machine (commands!) |
| **Compensation** | A new operation that undoes a committed effect (not a rollback) |
| **DLQ (Dead Letter Queue)** | Topic for records that exhausted their retry budget |
| **Retry topic** | Topic that holds records for delayed re-delivery (time-bucketed keys) |
| **Poison message** | Record that can never be processed as-is |
| **Event sourcing** | Events are the truth; state is a projection |
| **CQRS** | Separate write (commands) and read (projections) sides |
| **Projection / read model** | Derived view of the event stream |
| **Schema Registry** | Central registry: schemas, versions, compatibility rules |
| **Subject** | Registry key: `<topic>-key` / `<topic>-value` typically |
| **Backward / forward / full compatibility** | Old-consume-new / new-consume-old / both |
| **Tracecontext (W3C)** | `traceparent`/`tracestate` headers propagated via Kafka record headers |

## When you see short-hand in this guide

| In the guide | Means |
|--------------|-------|
| *"P3 dedup table"* | The `processed_events` unique-row pattern, conceptually |
| *"P4 relay"* | The polling-publisher loop |
| *"P7 watchdog"* | Periodic scan for stuck `running` sagas |
| *"x-attempts"* | Header-based retry budget convention |
| *"acks=all, idempotence"* | The default durable producer config used everywhere |