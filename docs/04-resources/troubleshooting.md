# Troubleshooting & Pitfalls

The incident board, distilled. Every scenario here appears in the projects — know
the shape before it bites.

## 1. "Messages are processed twice"

**Where you'll meet it:** P2 break-it, P3 break-it, P8 break-it.

| Cause | Diagnosis | Fix |
|-------|-----------|-----|
| Consumer crashed after process, before commit | Log shows same `event_id` twice with a gap | "Process then commit" is correct — add dedup: unique key / `processed_events` |
| Producer retried after timeout | Broker shows same key+payload twice, close offsets | `enable.idempotence=true` |
| Downgraded `acks` | Duplicates even with idempotence | `acks=all` + `min.insync.replicas` |
| Auto-commit on (`enable.auto.offset.commit=true`) | Duplicates after *every* crash | Manual commit after the side-effect transaction (always) |

Extra: rebalance storms surface as duplicate bursts around a specific wall-clock
time. Cross-check group membership `--describe` at that moment.

## 2. "Lag is growing but consumers look fine"

**Where you'll meet it:** P2, P12.

Check in order:

1. `--describe --group` → are consumers **more than partitions** (idle members)?
2. Any member **not** in the list (or `STABLE` but unassigned) → it was kicked —
   check `max.poll.interval.ms` vs slowest handler.
3. Poison record in a partition (bad bytes / schema violation) → consumer loops or
   DLQs? Look at retry topics and DLQ.
4. Is the batch commit waiting on a slow DB (row lock held by a long transaction)?

## 3. "Nothing arrives at all"

- `kafka-topics.sh --describe` → topic exists? partition count sane?
- Producer: **did you read the delivery callback?** Errors are invisible otherwise.
- `--describe --group` on consumer group → offsets committed ever? `auto.offset.reset`
  wrong (consumer group already exists with committed offsets → reset is ignored).
- Retention: topic kept records past `retention.ms` → `auto.offset.reset=earliest`
  still starts at the *first available* record, not "the beginning of time."
- **WAL / slot**: P5 — if a Debezium slot is broken/paused, connector shows
  `PAUSED`/`FAILED`: check `pg_replication_slots` and `publication` existence.

## 4. "Exactly-once is doing weird things"

Re-read [EOS](../02-concepts/exactly-once.md) — then remember:

- Transactions are scoped **per producer instance** (`transactional.id`); two
  producers sharing one id = zombie fencing fights *you*.
- `read_committed` consumers cannot read uncommitted batches — but rebalance
  duplicate behavior is unchanged: offsets committed by the transactional producer
  only cover *its* produces; dedup assumptions from P3 still stand.

## 5. "Saga stuck in running" (the famous one)

| Observation | Likely cause | Action |
|-------------|-------------|--------|
| `current_step=reserve_stock`, `updated_at` old, lag 0 | Orchestrator crashed between state+event | Watchdog re-emits command; log the re-emission |
| `current_step=charge_payment`, payments topic lag>0 | Payments consumer down / DB blocked | Fix consumer; do **not** manually compensate (double-charge risk) |
| `current_step=charge_payment`, payments topic lag 0, DLQ non-empty | Payment failed → DLQ | Runbook: verify charge state at provider, then replay or compensate |
| Orchestrator crashed *before* command outbox commit | Nothing emitted (that's fine) | Restart → rehydrate → continue |
| Command emitted twice (watchdog + replay) | Payments processed it twice? | Idempotency key = saga/step ids (P8/P12) |

Golden rule: **the saga state table is opinion, payment provider state is fact.**
Never "fix" a saga by hand before checking the external ledger.

## 6. Outbox anomalies

| Symptom | Cause | Fix |
|---------|-------|-----|
| `published=FALSE` rows older than a minute | Relay dead / relay wrong topic | Restart, alert on stale rows (P4 query) |
| Events out of order in the topic | Wrong key on the outbox row (`aggregate_id` vs random) | Key = aggregate id; `ORDER BY id` in relay |
| Relay double-publishes after crash | Ack-then-mark crash window (expected!) | Consumers dedup — if they don't, that's the bug |
| Outbox table huge | `published` rows never purged | Archival job with replay window slab |

## 7. Schema & registry pain

| Symptom | Fix |
|---------|-----|
| Consumer gets "Unknown magic byte" | Bytes are not Avro (JSON written by hand/kcat): DLQ + page producer |
| `io.confluent.kafka.schemaregistry.client.rest.exceptions` at produce | Registry down: AvroSerializer needs it at serialization; registry needs HA |
| Compatibility check fails on CI | Field removed / type changed — follow the add-default → deprecate → remove dance |
| Subject mismatch (silently separate schemas) | `orders-value` vs `orders_value` — same topic, two subjects, no errors till prod. Lint your subject naming |

## 8. Local dev-only traps

- **1 broker, `replication-factor=3`** → controller errors; `--replication-factor 1`
  locally (fine for learning, state it out loud in interviews).
- **`auto.create.topics`** on → typos create topics silently. Good for demos, bad
  for your education: disable it after P2 and create topics explicitly.
- **Docker volume loss** → Kafka data + `__consumer_offsets` reset; consumer groups
  start over (`auto.offset.reset` decides). Your idempotency tables (P3) are the
  safety net — good reason to start every project by reusing P3's pattern.
- **`kcat -K:` substitution** vs literal ':' — with `-K:` the first colon in the
  value is the key separator; payloads with colons need `-K` + careful quoting.

## The diagnostic order (memorize this)

```
1. consumer group      --describe   (lag, members, assignment)
2. consumer logs       (errors? DLQ recent? errors silent?)
3. producer logs       (delivery callbacks!)
4. topics              --describe   (ISR, retention)
5. business DB state   (outbox rows, processed_events, saga table)
6. tracing (P12)       (find the slow/failed span: first-hop reliability ≥ DB ≥ API)
```

Every incident in the projects is answerable in that order — and every interview
"debug the async system" question rewards the same sequence.