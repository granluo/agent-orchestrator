#!/usr/bin/env bash
set -e

PSQL="docker exec -i orchestrator-pg psql -U postgres -d orchestrator -P pager=off -t -A"

step() { echo; echo "==> $1"; }

step "Submitting a long task (routes to the large model, ~15-20s)"
RESP=$(curl -s -X POST localhost:8000/tasks -H "Content-Type: application/json" \
  -d '{"prompt": "Explain why crash recovery matters in a distributed task queue, and describe how a lease, heartbeat, and reaper work together to recover from worker crashes, in about four sentences."}')
TASK_ID=$(echo "$RESP" | python3 -c "import sys,json; print(json.load(sys.stdin)['task_id'])")
echo "    task_id = $TASK_ID"

step "Starting a worker in the background"
python scheduler.py & WORKER_PID=$!
sleep 6   # let it claim and start executing

step "Task state while running:"
STATE=$($PSQL -c "SELECT status FROM tasks WHERE task_id=$TASK_ID;")
echo "    $STATE"
if [ "$STATE" != "RUNNING" ]; then
  echo "!! task is $STATE, not RUNNING — demo timing failed"; kill $WORKER_PID; exit 1
fi

step "Killing the worker mid-task (simulated crash, pid $WORKER_PID)"
kill -9 $WORKER_PID

step "Task is stranded in RUNNING; lease will expire in ~30s"
$PSQL -c "SELECT status, lease_expires_at FROM tasks WHERE task_id=$TASK_ID;"
echo "    waiting 35s for the lease to expire..."
sleep 35

step "Starting a fresh worker; its reaper reclaims the stale task"
python scheduler.py & WORKER_PID=$!

step "Waiting for the task to complete..."
for i in $(seq 1 30); do
  STATUS=$($PSQL -c "SELECT status FROM tasks WHERE task_id=$TASK_ID;")
  [ "$STATUS" = "SUCCEEDED" ] && break
  sleep 3
done

step "Final state — delivery_count=2 (delivered twice) but retry_count=0 (task never failed):"
$PSQL -c "SELECT status, delivery_count, retry_count, route, cost FROM tasks WHERE task_id=$TASK_ID;"

kill $WORKER_PID 2>/dev/null || true
step "Demo complete: a worker crash is redelivery, not task failure."
