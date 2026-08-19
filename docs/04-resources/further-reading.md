# Further Reading

Curated, ordered by value-per-minute for this learning path. Read (or watch)
after the corresponding project, not before. Every concept and project page also
links its own **Learn more** items — this page is the full master index.

## Watch

The talks and courses below pair with the pages they're listed besides; the
three that earn repeated re-watching are starred.

### Courses & overviews

- **Apache Kafka 101** — Confluent Developer (Tim Berglund), 12 short videos.
  The video twin of 01-fundamentals: events, topics, brokers, producers,
  consumers, offsets, consumer groups. Start here if you prefer video to text.
- **The Transactional Outbox Pattern** — Confluent Developer (Wade Waldron),
  the video twin of outbox + saga (P4–P7).

### Talks

- **Turning the Database Inside Out** ★ — Martin Kleppmann (Strange Loop),
  talk + [transcript](https://martin.kleppmann.com/2015/03/04/turning-the-database-inside-out.html).
  The philosophical foundation of event sourcing, outbox and materialized views.
  Watch before starting the project ladder.
- **Ins and Outs of the Outbox Pattern** ★ — Gunnar Morling (Devoxx UK), the
  edge cases (ordering, idempotency, timeouts) of every outbox you will build.
- **Reliable Message Delivery with Apache Kafka** — Uber (Kafka Summit SF 2018):
  the whole reliability design space in 26 minutes — the perfect recap of
  delivery-semantics + failure-handling (P2, P8).
- **Everything you wanted to know about a Kafka consumer group, but were afraid
  to ask** — Matthias J. Sax (Kafka Summit London 2019): rebalance protocol,
  cooperative rebalancing, static membership (P2).
- **Introducing Exactly-Once Semantics in Apache Kafka** — Apurva Mehta & Jason
  Gustafson: the EOS design, 40 minutes (P10).
- **A Pragmatic Guide to Apache Kafka's Exactly-Once Semantics** — Bay Area
  meetup: where EOS pays off and where idempotence alone suffices (P10).
- [Kafka Summit's curated talk list](https://kafka.apache.org/community/videos/)
  — the official archive sorted by rating; mining it after P12 is a course in
  itself.

### Essential reading (recurring)

- **Designing Data-Intensive Applications** — Kleppmann. Book (any format) +
  [chapter list](http://dataintensive.net). Chapters 9–11 are the skeleton of
  this course.

## Kafka core (after P1–P2)

- **The Log: What every software engineer should know about real-time data's
  unifying abstraction** — Jay Kreps, Confluent blog. The essay that created the
  modern mental model of logs. Read it once, cite it forever.
- **Kafka: The Definitive Guide** (Confluent/Gunnar Morling ed., O'Reilly) — the
  reference book; chapters 1–5 cover everything in this course's first half.
- **Kafka docs — "Exactly-Once Semantics"** — the canonical EOS explanation. Read it
  *after* P10 to unpick the precise claims.

## Reliability patterns (after P3–P5)

- **"Reliable Microservices Data Exchange with the Outbox Pattern"** — Chris
  Richardson (microservices.io) — the pattern's founding post.
- **"Pattern: Transactional outbox"** — microservices.io, deep dive + variants
  (polling vs CDC) — matches P4/P5 exactly.
- **Debezium docs — Outbox Event Router** — the reference for the router config in
  P5; skim the "transactional outbox" tutorial.
- **"Saga" — Chris Richardson** — microservices.io "Pattern: Saga"; plus his
  "Choreography vs Orchestration" comparison post. Read after P6/P7 and compare
  against your measured experience.

## Idempotency & distributed systems fundamentals (after P3, P7)

- **"Making the Unfalsifiable Falsifiable" / idempotency** — Stripe's engineering
  blog on idempotency keys (this project's happy path is *their* happy path).
- **Designing Data-Intensive Applications** (Kleppmann, O'Reilly) — chapters on
  replication/ordering + "the trouble with distributed systems" + "consistency
  patterns" — the theoretical skeletal for everything in 02-concepts.
- **"If you have too many guarantees, you have none"** (Kleppmann blog essay) —
  the honest framing of EOS claims, companion to P10.

## Event sourcing & CQRS (after P11)

- **"Event Sourcing" and "CQRS"** — Martin Fowler's three-part essays — the
  canonical definitions; read after building (definitions land better with the
  ledger in hand).
- **Greg Young, "A Versioned Model of Event Sourcing" / his talks** — the
  version/stream discipline you implemented as `UNIQUE (account_id, version)`.
- **"Why events and event sourcing?"** — Facundo Molina (Baseline/Evolutive) if
  you want the adversarial engineering take on when *not* to.

## Schemas & CDC (after P5, P9)

- **Confluent Schema Registry docs — "Compatibility and Validation"** — precise
  matrix of backward/forward/full with Avro examples.
- **Debezium docs "Data change events / order guarantees"** — the partition and
  ordering semantics of CDC output.

## Observability (at P12)

- **OpenTelemetry Python + Kafka instrumentation docs** — the exact span-propagation
  kitchen (tracecontext propagator ↔ Kafka headers) implemented in the capstone.
- **"Monitoring Kafka"** — Confluent KIP-729 era docs, plus your own lag graphs
  (the only reliable teacher).

## System design interview alignment

- **Grokking & Alex Xu's "System Design Interview"** vol. 1–2 — to map this
  course's vocabulary onto interview questions (search the table of contents for
  *Kafka/outbox/saga*).
- **"The Log" (Kreps)** again — still the single best "one question deep" answer.
- **Cloudflare / Stripe / Uber tech blog** case studies of async pipelines — the
  "production reality" layer under the patterns.

## If you want the hardest possible next step

1. Rework P12 with **one hot partition per minute** (that "hot key" problem
   mentioned in ordering) → splitting strategies, per-partition isolation.
2. **Multi-region Kafka** (Cluster Linking / MirrorMaker 2): design the
   active-active event routing + conflict resolution story.
3. **Rebuild P4 with Streams**: replace your relay with a Kafka Streams topology
   (state store + outbox) — you will appreciate every line you hand-wrote.
4. **Schema governance at scale**: subject naming, CI gates on your registry from
   day one — plus the migration matrix from P9.

## Reading sequence that matches this course

| When | Read | Watch |
|------|------|-------|
| Before P1 | The Log (Kreps) — skim | Kafka 101 (ep. 1–3) |
| After P2 | — | Consumer group talk (Sax) |
| After P3 | Stripe idempotency post + DDIA "trouble with distributed systems" | — |
| After P4/P5 | Outbox pattern (Richardson) + Debezium docs | Outbox talk (Morling) |
| After P6/P7 | Richardson saga posts + Fowler CQRS | Outbox pattern course |
| After P8 | — | Reliable Message Delivery (Uber) |
| After P9 | Registry compatibility docs | Schema Registry 101 course |
| After P10 | Kafka EOS docs (now they make sense) | EOS talks (Mehta; Pragmatic Guide) |
| After P11 | Fowler event sourcing + versioned events | Turning the Database Inside Out (re-watch) |
| After P12 | DDIA consistency chapters + cloud case studies | Kafka Summit curated talks |

Happy breaking.