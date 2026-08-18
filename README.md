# System Design with Kafka — Hands-On Guide

Interactive documentation site for learning system design with Kafka, built with
[MkDocs Material](https://squidfunk.github.io/mkdocs-material/).

## Prerequisites

- Python 3.10+
- Docker + Docker Compose (for local Kafka, Debezium, Schema Registry, etc.)

## Develop locally

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
mkdocs serve --open
```

## Build static site

```bash
mkdocs build
# output lands in site/
```

## Repo layout

| Path | What lives there |
|------|------------------|
| `docs/01-fundamentals/` | Messaging basics, Kafka architecture, environment setup |
| `docs/02-concepts/` | The patterns: idempotency, outbox, saga, DLQ, EOS, event sourcing, ... |
| `docs/03-projects/` | 12 progressive hands-on projects (the learning ladder) |
| `docs/04-resources/` | Glossary, troubleshooting, further reading |
| `code/` | (planned) Runnable FastAPI project code, project by project |