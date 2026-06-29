# Debugging notes

Real debugging sessions from building this system, kept as a record of how
issues were found and reasoned about — not just what the final code looks like.

---

## Duration metric measured too long for retried tasks

### Symptom

`duration_seconds` (per-task execution time) came out wrong for tasks that had
been retried: a task whose backend work is ~8s was recorded as 10s, and in one
case 20s. Tasks that succeeded on the first attempt measured correctly (~8s).

### Investigation

Rather than guess, I added timestamp logging around the suspect code paths
(execution entry, the `started_at` write, and backend return) and submitted
tasks until I hit retries.

The logs revealed two separate problems stacked on top of each other.

### Root cause 1 — `started_at` recorded too early, including backoff

`started_at` was written at **claim** time. But between claim and actual
execution, the worker sleeps for retry backoff. Since
`duration = now() - started_at`, the measurement absorbed the backoff wait.

The fix was to move the `started_at` write from claim into `execute_task`,
*after* the backoff sleep and immediately before invoking the backend, so the
start point marks "work begins" and excludes the backoff.

Conceptually: this metric is meant to capture *backend execution time* (it feeds
routing decisions about which backend is faster). Backoff is part of the retry
policy, unrelated to backend speed, so it must not be counted.

### Root cause 2 — heartbeat held a row lock while sleeping

After fixing #1, the `started_at` UPDATE itself sometimes stalled for ~10s. The
timestamp logs showed that exact same statement taking 14ms on one run and 10s
on another. The 10s matched the heartbeat interval, which pointed at the
heartbeat thread.

The heartbeat's `stop_event.wait(10)` had ended up **inside** the `with conn`
(transaction) block. So each renewal held the task's row lock and then slept 10s
before committing. When `execute_task` tried to write `started_at` on the same
row, it blocked on that lock until the heartbeat woke up and released it.

The fix: move `wait(10)` outside the transaction block, so each renewal is a
short committed transaction that releases its lock immediately, and the sleep
holds no lock.

General rule reinforced: **never sleep or do slow work inside an open
transaction** — a long transaction holding a lock blocks everyone contending for
that row.

### The part worth remembering: this was an intermittent concurrency bug

At one point I measured the `started_at` UPDATE running in 14ms and briefly
concluded the lock-contention theory was wrong. **That conclusion was the
mistake.** The contention was always present in the code; that run simply didn't
trigger it, because whether `execute_task`'s UPDATE or the heartbeat's renewal
grabs the row lock first depends on thread scheduling.

This is the defining trait of concurrency bugs: they are **timing-dependent and
intermittent**. A single run that doesn't reproduce the problem proves nothing —
it only shows that particular interleaving didn't hit it. Treating "it passed
once" as "the bug isn't there" is exactly how concurrency issues slip into
production and then surface rarely and unreproducibly.

To actually confirm it, I raised the failure rate to force more retry paths and
reproduced the stall reliably. And the real guarantee of the fix isn't "it
passed in testing" — it's the structural argument: with `wait()` outside the
transaction, a renewal cannot hold the row lock for longer than a single UPDATE,
so the contention window is gone by construction.

### It was a regression

Root cause 2 was not new. The "heartbeat holding a lock while sleeping" issue
had been fixed earlier when the lease/heartbeat mechanism was first built; a
later edit moved `wait()` back inside the transaction and the bug returned.

Takeaway: a fixed bug can come back, and nothing was guarding it. A test
asserting the heartbeat's transaction stays short would have caught the
regression immediately instead of it being rediscovered by reading logs. This is
the clearest argument in the project for adding tests around the key
concurrency invariants.

### Method, in short

1. Notice the anomaly and take the discrepancy seriously (20s vs expected 8s).
2. Add instrumentation (timestamps) instead of guessing.
3. Don't trust a single non-reproduction for a concurrency bug — force the path
   and reproduce reliably.
4. Trace the specific number (the 10s = heartbeat interval) to the root cause.
5. Prefer a structural guarantee over "it passed once."
