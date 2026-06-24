# agent-orchestrator

A durable, multi-worker task orchestration platform built on PostgreSQL. Tasks
are submitted over HTTP, persisted in Postgres, and executed by a pool of
workers that claim work concurrently. The system survives worker crashes without
losing or duplicating tasks.

The design bet is that **Postgres is the coordinator** — claiming,
deduplication, and crash recovery are all expressed as SQL against a single
`tasks` table using row locks and constraints. No Redis, no message broker, no
separate lock service.

## Features

- **Durable state machine** — task state (`PENDING / RUNNING / SUCCEEDED /
  FAILED`) lives in Postgres, so a process crash never loses a task.
- **Concurrent claiming** — workers claim with `SELECT ... FOR UPDATE SKIP
  LOCKED`, so multiple workers fan out across tasks instead of contending for
  the same row.
- **Idempotent submission** — an `Idempotency-Key` header dedupes retried
  submissions via `ON CONFLICT`, returning the existing task instead of creating
  a duplicate.
- **Retries with exponential backoff** — transient execution failures are
  retried with `2 ** retry_count` backoff to avoid retry storms.
- **Crash recovery (lease + heartbeat + reaper)** — running tasks hold a
  time-bounded lease renewed by a heartbeat thread; if a worker dies, its lease
  expires and a reaper returns the task to the queue.
- **Poison-task protection** — a separate `delivery_count` bounds how many times
  a task can be redelivered, so a payload that crashes its worker every time is
  eventually failed rather than redelivered forever.

## Stack

Python · FastAPI · PostgreSQL · psycopg2 · Docker

## Quick start

```bash
# Postgres (Docker)
docker run --name orchestrator-pg -e POSTGRES_PASSWORD=devpass \
  -e POSTGRES_DB=orchestrator -p 5432:5432 -d postgres
psql ... -f schema.sql        # create the tasks table

# API
uvicorn main:app --reload

# Worker
python scheduler.py

# Submit a task
curl -X POST localhost:8000/tasks \
  -H "Idempotency-Key: demo-1" -H "Content-Type: application/json" \
  -d '{"prompt": "hello"}'
```

The DB password is read from the `DB_PASSWORD` environment variable, defaulting
to `devpass` for local development.

### Crash-recovery demo

Submit a long-running task, `kill -9` the worker mid-execution, and watch the
task sit `RUNNING` until its lease expires — then the reaper returns it to
`PENDING`, a fresh worker re-claims it, and it completes.

## Design

See [DESIGN.md](./DESIGN.md) for the full architecture, the reasoning behind
each decision, and the known limitations.

## Project layout

| File | Responsibility |
| --- | --- |
| `main.py` | FastAPI app — task submission and status endpoints |
| `scheduler.py` | Worker loop: claim, execute, heartbeat, reaper |
| `db.py` | Connection handling and submission / dedup queries |
| `schema.sql` | `tasks` table definition |
| `DESIGN.md` | Architecture and design decisions |
