# Distributed AI Agent Orchestration Platform — Design

A durable, multi-worker task orchestration system built on PostgreSQL. Tasks are
submitted via an HTTP API, persisted in Postgres, and executed by a pool of
workers that claim work concurrently. The system is designed to survive process
crashes without losing or duplicating work.

This document records the design decisions and their tradeoffs. It is written to
be read alongside the code, not as a substitute for it.

---

## 1. Goals and non-goals

**Goals**

- Durable task state: a task is never lost because a process died.
- Safe concurrency: multiple workers claim and run tasks without stepping on
  each other.
- Crash recovery: a worker that dies mid-execution does not strand its task
  forever.
- At-least-once execution with deduplication at submission time.

**Non-goals (for this iteration)**

- Horizontal scaling beyond a single Postgres instance.
- Sub-second latency. The polling model trades a little latency for simplicity.
- Exactly-once execution. We target at-least-once and make execution idempotent
  where it matters, rather than attempting distributed exactly-once.

---

## 2. Architecture overview

```
            HTTP (FastAPI)
                 |
            submit_task()  ---- INSERT --->  +-----------------+
                                             |   PostgreSQL    |
   worker 1  --- claim/execute/update --->   |   tasks table   |
   worker 2  --- claim/execute/update --->   | (state machine) |
   reaper    --- reclaim stale --------->    +-----------------+
```

Three roles, all coordinating *through the database* rather than through a
separate coordination service:

- **API** (`main.py`): accepts task submissions, returns task id + status.
- **Worker / scheduler** (`scheduler.py`): claims PENDING tasks, executes them,
  writes results. Runs a heartbeat thread per in-flight task.
- **Reaper**: periodically reclaims tasks whose owning worker has died.

The central design bet is that **Postgres is the coordinator**. Claiming,
deduplication, and stale-task recovery are all expressed as SQL against the
`tasks` table, using row locks and constraints. No Redis, no message broker, no
separate lock service.

---

## 3. Task state machine

```
  PENDING --> RUNNING --> SUCCEEDED
     ^           |
     |           +------> FAILED        (retries exhausted)
     |           |
     +-----------+        (transient failure -> retry,
                           or reaper reclaim after worker death)
```

State is a column on the `tasks` row. Every transition is a single SQL UPDATE,
so the database is always the single source of truth for where a task is.

Why Postgres instead of an in-memory queue: durability. If the process holding
an in-memory queue dies, the queue dies with it. Persisting state to Postgres
means a crash loses at most the in-flight execution, not the task itself — the
task is recovered and re-run.

---

## 4. Concurrent claiming: `SELECT ... FOR UPDATE SKIP LOCKED`

Workers claim work with:

```sql
SELECT task_id, payload, retry_count FROM tasks
WHERE status = 'PENDING'
ORDER BY created_at
FOR UPDATE SKIP LOCKED
LIMIT 1
```

**The problem it solves:** with multiple workers, two workers running the same
SELECT must not claim the same task.

**Why `SKIP LOCKED` specifically:** a plain `FOR UPDATE` would make the second
worker *block* waiting for the first worker's row lock — serializing the workers
and destroying throughput. `SKIP LOCKED` tells Postgres to skip rows already
locked by another transaction and move to the next available row. The result is
that concurrent workers fan out across different tasks instead of contending for
the same one.

This converts mutual exclusion ("only one worker may hold this row") into work
distribution ("each worker grabs a different row, nobody waits").

---

## 5. Idempotent submission

**The problem:** a client whose POST times out will retry. Without dedup, the
retry creates a second identical task.

**Design:** the client may send an `Idempotency-Key` HTTP header. The key is
stored in a `UNIQUE` column. The chosen design returns the *existing* task on a
duplicate, rather than erroring — a retry should observe the same result as the
original request, not a failure.

**Implementation** (`db.submit_task`):

```sql
INSERT INTO tasks (payload, idempotency_key)
VALUES (%s, %s)
ON CONFLICT (idempotency_key) DO NOTHING
RETURNING task_id
```

- If the INSERT returns a row, it's a new task.
- If it returns nothing, the key already existed; a fallback `SELECT` fetches
  the existing task id.

**Why a header, not a body field:** the idempotency key describes the *request*
(a transport concern), not the *task* (domain data). Keeping it in the header
mirrors the separation maintained elsewhere in the codebase.

**Why no select-then-insert:** checking "does this key exist?" then inserting
has a race — two concurrent requests both see "no" and both insert. Relying on
the `UNIQUE` constraint + `ON CONFLICT` pushes the race resolution into the
database, which serializes the conflicting inserts on the unique index.

**NULL handling:** requests without a key insert `NULL`. Postgres does not treat
multiple NULLs as duplicates under a UNIQUE constraint, so keyless tasks never
collide. The code branches on `idempotency_key is None` so the dedup path (and
its fallback `SELECT ... WHERE idempotency_key = %s`) is never reached with a
NULL — a `WHERE col = NULL` would match nothing and is structurally avoided
rather than defended against after the fact.

---

## 6. Retries with exponential backoff

A worker distinguishes two outcomes of `execute_task`:

- **Returns** -> success -> `mark_succeeded`.
- **Raises** -> failure -> `mark_failed_or_retry`.

On failure, if `retry_count + 1 < MAX_RETRY`, the task goes back to `PENDING`
with `retry_count` incremented; otherwise it becomes `FAILED` with the error
recorded. Before re-executing a retried task, the worker sleeps `2 ** retry_count`
seconds.

**Why backoff:** immediate retries against a struggling downstream create a
retry storm. Exponential backoff spreads the load and gives the downstream time
to recover.

**Separation of concerns:** `execute_task` only executes — it returns on success
and raises on failure, and never touches the database. All state writes live in
`mark_succeeded` / `mark_failed_or_retry`. This keeps the execution logic free
of persistence details and makes the failure semantics explicit at the call
site.

---

## 7. Crash recovery: lease + heartbeat + reaper

This is the core of the system's fault tolerance and the hardest part.

**The problem:** a worker sets a task to `RUNNING`, then the process dies
(SIGKILL, OOM, power loss) mid-execution. The task is stuck `RUNNING` forever:
no worker will re-claim it (claim only looks at `PENDING`), and it never
completes. The task is silently lost.

**The model shift:** owning a task is not permanent ("I marked it RUNNING so
it's mine forever"). It is a *lease* — a time-bounded claim that must be
actively renewed.

### 7.1 Lease

On claim, the worker records `lease_expires_at = now() + 30s`. This is a promise:
"I will be done, or will have renewed, within 30 seconds."

### 7.2 Heartbeat

A long-running task would outlive a fixed 30s lease, so the worker renews it. A
dedicated **heartbeat thread**, started per in-flight task, pushes
`lease_expires_at` forward every 10 seconds while the task runs.

The key invariant: **heartbeat interval must be well below the lease duration.**
With a 30s lease renewed every 10s, a single renewal can be lost (e.g. a
transient network blip) without the lease expiring — it takes two consecutive
missed renewals to trigger a false reclaim. The 3x ratio is the safety margin.

When the worker dies, the heartbeat thread dies with it, renewals stop, and the
lease is allowed to expire. **A stopped heartbeat is the signal of a dead
worker.**

### 7.3 Reaper

A background loop periodically reclaims tasks that are `RUNNING` but whose lease
has expired (or was never set):

```sql
UPDATE tasks
SET status = 'PENDING', lease_expires_at = NULL, delivery_count = delivery_count + 1
WHERE status = 'RUNNING'
  AND (lease_expires_at IS NULL OR lease_expires_at < now())
```

`IS NULL` is required, not optional: a worker that crashed immediately after
claiming (before its first heartbeat) leaves `lease_expires_at` as NULL, and
`NULL < now()` is not true, so an `IS NULL` clause is needed to catch those.

**Atomicity / no double-reclaim:** the reclaim is a single `UPDATE` with a
`WHERE` predicate. If two reapers target the same stale row, Postgres serializes
them on the row lock; the second one re-evaluates its `WHERE` after the first
commits, finds the row is no longer `RUNNING`, and affects zero rows. The same
row-lock principle that makes claiming safe makes reclaiming safe. A
select-then-update would reintroduce the race; a single conditional UPDATE does
not.

---

## 8. Two independent counters: `retry_count` vs `delivery_count`

A subtle but important distinction the design insists on:

| Counter | Incremented when | Meaning |
| --- | --- | --- |
| `retry_count` | `execute_task` **raises** | the task's own execution failed |
| `delivery_count` | the **reaper reclaims** it | the task was handed out again because a worker vanished |

A worker *crashing* is not the same failure as a task *erroring*. The first is
"the executor disappeared"; the second is "the work itself failed." Conflating
them into one counter would mean a worker crash consumes a retry the task never
actually used — or that a genuinely failing task escapes its retry limit by
being reclaimed.

Observed in practice: tasks recovered after a worker kill end up with
`delivery_count = 1` *and* a non-zero `retry_count` if they then hit a transient
execution failure — the two numbers tell two different stories about the same
task.

---

## 9. Thread coordination details (heartbeat)

The heartbeat runs in a separate thread because the main thread is blocked
inside `execute_task` and cannot also renew on a timer.

- **Separate DB connection per thread.** A psycopg2 connection is not safe for
  concurrent use by multiple threads. The heartbeat thread opens its own
  connection. Conflicts on the *same row* (heartbeat renewing while the main
  thread writes final status) are left to Postgres row locks — that is a data
  concern, handled by the database, not a connection concern.

- **Short transactions.** Each renewal is its own committed transaction, and the
  `wait()` between renewals happens *outside* any open transaction. An earlier
  version held one transaction open across the whole loop and slept inside it —
  holding a row lock while sleeping, which deadlocked the main thread's final
  status write. The fix (and the general rule): never sleep or do slow work
  inside an open transaction.

- **Stop signal via `threading.Event`.** The main thread cannot safely kill the
  heartbeat thread. Instead it sets an Event; the heartbeat loop checks
  `is_set()` each iteration and uses `event.wait(10)` (not `time.sleep(10)`) so
  it wakes immediately when signalled instead of sleeping out the full interval.

- **`set()` then `join()`.** On task completion the main thread sets the Event
  *and* joins the thread before writing final status. Setting only signals;
  joining guarantees the heartbeat has actually stopped issuing renewals before
  the final status write, eliminating a race where a stale renewal could land
  after completion.

- **Daemon thread.** The heartbeat is a daemon thread so a dying main process
  is never blocked from exiting by an outstanding heartbeat.

---

## 10. Known limitations (intentional, deferred)

These are known and deliberately out of scope for the current iteration. They
are recorded here rather than hidden.

- **`delivery_count` has no cap yet.** The reaper currently always returns a
  stale task to `PENDING`. A "poison task" that crashes its worker on every
  attempt would be reclaimed forever, and because a crash does not increment
  `retry_count`, the retry limit does not catch it. The intended fix:
  past a `MAX_DELIVERY` threshold, the reaper marks the task `FAILED` instead of
  re-queuing it. (Counter and increment already exist; the threshold branch does
  not.)

- **Reaper is single-process.** Recovery stops if the reaper is down. The
  reclaim is already concurrency-safe (single atomic UPDATE), so running
  multiple reapers would be correct — it just isn't set up yet.

- **Success-write failure is mis-handled.** If `execute_task` succeeds but the
  subsequent `mark_succeeded` write to the DB fails, the exception currently
  routes the task into the failure/retry path — re-running already-completed
  work. "Execution failed" and "recording the result failed" are two different
  failures that should be handled separately.

- **No connection pooling.** Each operation opens and closes its own
  connection. Correct, but every call pays TCP + auth handshake cost. A pool
  (`psycopg_pool`) is the natural next step under load.

- **Polling latency.** Workers poll every 2 seconds when idle. Fine for this
  scale; a `LISTEN/NOTIFY` push model would cut idle latency if needed.

---

## 11. How to run

```bash
# Postgres (Docker)
docker run --name orchestrator-pg -e POSTGRES_PASSWORD=devpass \
  -e POSTGRES_DB=orchestrator -p 5432:5432 -d postgres
psql ... -f schema.sql        # create the tasks table

# API
uvicorn main:app --reload

# Worker
python scheduler.py

# Submit
curl -X POST localhost:8000/tasks \
  -H "Idempotency-Key: demo-1" -H "Content-Type: application/json" \
  -d '{"prompt": "hello"}'
```

**Crash-recovery demo:** submit a long task, `kill -9` the worker mid-execution,
observe the task stuck `RUNNING`, then watch the reaper return it to `PENDING`
(with `delivery_count` incremented) and a fresh worker complete it.
