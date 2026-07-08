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
SELECT task_id, payload, retry_count, delivery_count FROM tasks
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
has expired (or was never set), returning them to `PENDING`:

```sql
UPDATE tasks
SET status = 'PENDING', lease_expires_at = NULL
WHERE status = 'RUNNING'
  AND (lease_expires_at IS NULL OR lease_expires_at < now())
```

The reaper only returns the task to the queue; it does **not** increment
`delivery_count`. The counting happens when the task is re-claimed (see §8) — a
reclaim is not itself a delivery, the subsequent claim is.

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
| `delivery_count` | a worker **claims** the task (PENDING -> RUNNING) | the task was dispatched to a worker again |

A worker *crashing* is not the same failure as a task *erroring*. The first is
"the executor disappeared"; the second is "the work itself failed." Conflating
them into one counter would mean a worker crash consumes a retry the task never
actually used — or that a genuinely failing task escapes its retry limit by
being reclaimed.

Observed in practice: tasks recovered after a worker kill end up with
`delivery_count = 1` *and* a non-zero `retry_count` if they then hit a transient
execution failure — the two numbers tell two different stories about the same
task.

**Where the increment lives.** `delivery_count` is incremented exactly once per
dispatch, at claim time, when a task transitions PENDING -> RUNNING. The reaper
does **not** touch it (see §7.3) — reclaiming a stale task only returns it to
PENDING; the subsequent re-claim is what counts as the next delivery. Keeping
the increment in exactly one place (claim) means `delivery_count` equals "times
handed to a worker," with no double-counting between claim and reaper.

**Redelivery cap (poison-task protection).** The cap is enforced as admission
control at claim: if `delivery_count + 1 > MAX_DELIVERY`, the task is marked
`FAILED` (with `last_error = 'exceeded max delivery'`) instead of being executed.
This bounds poison tasks — a payload that crashes its worker every time would
otherwise be reclaimed and re-dispatched forever, since a crash never increments
`retry_count` and so never trips the retry limit. The check lives at claim
because claim is the single gateway from PENDING to RUNNING, making it the
natural admission point — the reaper stays purely a recovery mechanism, and the
"give up" decision stays with the component about to execute the task.

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

## 10. Observability: metrics and per-task duration

The system exposes a snapshot of its own health, computed by scanning the
`tasks` table, plus a per-task execution-duration measurement that feeds the
aggregate.

### 10.1 Aggregate metrics (`compute_metrics`)

A single function scans the table and returns a snapshot dict:

- **Status distribution** — `SELECT status, COUNT(*) FROM tasks GROUP BY status`,
  collected into a dict (`{'SUCCEEDED': 24, 'FAILED': 2, ...}`). Answers "where
  are tasks right now."
- **Average retry count** — `AVG(retry_count)` over the whole table.
- **Reclaimed count** — how many tasks have been redelivered at least once,
  i.e. recovered by the reaper at some point (`delivery_count > 1`).
- **Average execution duration** — `AVG(duration_seconds)` over completed tasks.

The numeric aggregates are computed in a single query using `FILTER`:

```sql
SELECT AVG(retry_count),
       COUNT(*) FILTER (WHERE delivery_count > 1),
       AVG(duration_seconds)
FROM tasks
```

**Why `FILTER` rather than `WHERE`:** the three aggregates need different row
scopes — the averages are over the whole table, but the reclaimed count is over
a subset (`delivery_count > 1`). A top-level `WHERE` would constrain *all* the
aggregates to that subset. `FILTER` attaches a predicate to one aggregate only,
so a single scan produces aggregates with independent scopes instead of issuing
multiple queries.

**NULL handling:** `AVG` returns `NULL` on an empty (or all-NULL) input rather
than `0`. `avg_duration_seconds` is therefore left as `None` (serialized to JSON
`null`) when no completed task has a recorded duration yet — an honest "no data"
rather than a fabricated number. Postgres's `AVG` also silently skips NULL rows,
so tasks that never completed (no `duration_seconds`) don't distort the average.

### 10.2 HTTP exposure (`GET /metrics`)

The metric snapshot is exposed over HTTP as a thin endpoint:

```python
@app.get("/metrics")
def metrics():
    return db.compute_metrics()
```

The endpoint is deliberately a pure pass-through: all the query logic lives in
`db.compute_metrics`, and the route just forwards the result, which FastAPI
serializes to JSON. This keeps the HTTP layer (`main.py`) free of SQL, mirroring
the separation maintained elsewhere. The path is `/metrics` — a collection-level
endpoint kept distinct from `/tasks/{task_id}` so it isn't parsed as a task id.

### 10.3 Per-task execution duration

To measure how long a task actually executes, the system records two columns:

- `started_at` — written at claim time, when the task transitions
  PENDING -> RUNNING.
- `duration_seconds` — written at completion, as
  `EXTRACT(EPOCH FROM (now() - started_at))`, which converts the
  `now() - started_at` interval into a float number of seconds.

**Why `started_at` is written on every claim:** a task that is retried or
reclaimed and then re-claimed gets a fresh `started_at`, so `duration_seconds`
measures the most recent execution rather than including time the task spent
waiting in the queue between attempts.

**Why duration is stored, not derived (denormalization on purpose):** the
duration could in principle be recomputed from timestamps (`updated_at -
started_at`) at query time, but `updated_at` is a general-purpose column touched
by many transitions. If a future change ever mutates a completed task (e.g.
allowing a SUCCEEDED task to be re-run), every historical duration derived from
`updated_at` would silently become wrong. Snapshotting the duration at the
moment of completion makes it an immutable historical fact — "this run took this
long" — that no later state change can corrupt. This is the classic justification
for denormalization: when a derived value depends on a column whose semantics may
shift, capture the result at the event rather than recomputing it from a moving
source.

The duration is computed in SQL (`now() - started_at`) rather than in Python, so
the measurement uses a single clock (the database's) and avoids skew between the
application host and the DB.


## 11. Dynamic routing: local vs cloud backends

Tasks can execute on one of two backends with opposite trade-offs:

- **local** — a small model on limited local hardware: cheap (electricity),
  slow, weaker capability. Mocked as `time.sleep(8)`.
- **cloud** — a large model on specialized inference hardware: fast, more
  capable, billed per use. Mocked as `time.sleep(2)`.

The routing question is a cost / latency / capability trade-off: not every task
justifies the expensive backend, and not every task can be handled by the cheap
one. The strategy is cost-first — default to local to save money, and spill to
cloud when the task demands it or the system is overloaded.

The `route` column records which backend actually ran each task, written at
execution start alongside `started_at`.

### 11.1 Decision function: two signals, OR'd

`decide_route(payload, metrics)` is a pure function — it reads its inputs and
returns `'local'` or `'cloud'`, with no database access — so it can be unit
tested and reasoned about in isolation. It combines two signals:

1. **Content signal**: a long prompt (`len(prompt) > THRESHOLD`) routes to
   cloud. Prompt length is a cheap proxy for task difficulty — long inputs are
   more likely to need the stronger model.
2. **Load signal**: a PENDING backlog above `PENDING_THRESHOLD` routes to
   cloud. A growing queue means local consumption is falling behind
   submission; spilling new work to the faster backend drains the backlog.

Either signal alone is sufficient (logical OR): hard tasks go to cloud even
when the system is idle; easy tasks go to cloud when the system is swamped.

```
route decision (only when route is unset):
    prompt longer than THRESHOLD?        -> cloud
    PENDING backlog over PENDING_THRESHOLD? -> cloud
    otherwise                            -> local
```

### 11.2 Explicit over implicit: user-specified routes are respected

`decide_route` only fills in a route when none was specified
(`if route is None`). If a task already carries a route — e.g. a client that
knows its task needs the large model — the system honors that choice rather
than overriding it with automatic logic. The decision function is a smart
default, not a mandate.

(The HTTP API does not yet expose a route field on submission; the branch
exists in the worker so adding the entry point is a small change.)

### 11.3 The load signal closes the loop with observability

The load signal is where routing and observability connect: the same
`compute_metrics()` snapshot that powers `GET /metrics` is fed into
`decide_route`. Metrics are not just a dashboard for humans — they are an input
to the system's own decisions.

This creates a self-regulating negative feedback loop:

```
backlog grows past threshold
    -> new tasks spill to cloud (fast, 2s)
    -> backlog drains quickly
    -> backlog falls below threshold
    -> new tasks return to local (cheap)
```

The system automatically balances "save money" (local, when idle) against "buy
speed" (cloud, under load) with no operator intervention. In the logs this is
visible as a burst of submissions routing to cloud, followed by tasks reverting
to local once the queue drains.

Metrics are currently recomputed for every routing decision — a full-table scan
per task. This is fine at this scale and keeps the decision maximally fresh,
but at higher volume the snapshot should be cached or sampled (flagged as a
TODO in the code and listed under known limitations).

### 11.4 Cost tracking

Each backend has a fixed per-task cost (`LOCAL_COST = 0.001`,
`CLOUD_COST = 0.01` — cloud is 10x). The executing backend returns
`(result, cost)`, and `mark_succeeded` records the cost in the same UPDATE as
the duration — both are facts snapshotted at completion.

Storing cost (rather than deriving it from `route` at query time) follows the
same reasoning as storing `duration_seconds`: the recorded value is an
immutable historical fact. If per-route prices change later, past tasks keep
the cost they were actually charged, instead of history being silently
rewritten by a price constant.

The `cost` column is `NUMERIC`, not a float: money must be exact, and floating
point accumulates rounding error. (`duration_seconds` stays `DOUBLE PRECISION`
— a measurement tolerates float imprecision; a charge does not.)

`compute_metrics` aggregates `SUM(cost)` grouped by route (excluding tasks that
never ran, whose route is NULL):

```json
"cost_by_route": {"local": 0.012, "cloud": 0.07}
```

This makes the routing strategy's economics quantifiable: how much work stayed
on the cheap backend, and what the spill to cloud actually cost.

**MVP simplifications, deliberately accepted:**

- Only successful tasks are charged. In reality a failed execution still burns
  compute; charging failures requires the cost to survive the exception path
  (e.g. a custom exception carrying the cost), deferred to keep the MVP small.
- Cost is a per-route constant. Real billing is usage-based (tokens or
  seconds), which is why the cost is computed inside the executing backend and
  bubbled up with the result — when per-token pricing arrives, the change is
  confined to the backend functions; the plumbing already carries a computed
  value.

---

## 12. Known limitations (intentional, deferred)

These are known and deliberately out of scope for the current iteration. They
are recorded here rather than hidden.

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

- **Metrics recomputed per routing decision.** Every automatic routing decision
  runs `compute_metrics()`, a full-table scan. Should be cached or sampled at
  scale.
- **Failed tasks are not charged.** Cost is only recorded on success; failed
  executions consume compute but report zero cost, understating true spend.
- **Static thresholds.** `THRESHOLD` (prompt length) and `PENDING_THRESHOLD`
  (backlog) are hardcoded constants, not adaptive or configurable at runtime.

---

## 13. How to run

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

The DB password is read from the `DB_PASSWORD` environment variable, defaulting
to `devpass` for local development.

**Crash-recovery demo:** submit a long task, `kill -9` the worker mid-execution,
observe the task stuck `RUNNING`, then watch the reaper return it to `PENDING`
and a fresh worker re-claim it (incrementing `delivery_count`) and complete it.
