import db, time, json, random

MAX_RETRY=3

# get task

def claim_one_task():

    conn = db.get_conn()
    try:
        with conn, conn.cursor() as cur:
            cur.execute("""
            SELECT task_id, payload, retry_count FROM tasks
            WHERE status = 'PENDING'
            ORDER BY created_at
            FOR UPDATE SKIP LOCKED
            LIMIT 1
            """)
            row = cur.fetchone()
            if row is None:
                return None
            task_id, payload, retry_count = row
            cur.execute(
                    "UPDATE tasks SET status='RUNNING', updated_at=now() WHERE task_id=%s", (task_id,)
                    )
            return task_id, payload, retry_count
    finally:
        conn.close()

# execute task
def execute_task(task_id, payload):
    print(f"[worker] running task {task_id}: {payload}")
    if random.random() < 0.3 :
        raise RuntimeError("simulated transient failure")
    return {"echo": payload.get("prompt", "")}

def mark_succeeded(task_id, result):
    conn = db.get_conn()
    try:
        with conn, conn.cursor() as cur:
            cur.execute("UPDATE tasks SET status='SUCCEEDED', result=%s, updated_at=now() WHERE task_id=%s",
                        (json.dumps(result), task_id)
                        )
    except Exception as e:
        print(f"Failed to mark succeeded, {e}")
        raise e
    finally:
        conn.close()


def mark_failed_or_retry(task_id, retry_count, error_msg):
    conn = db.get_conn()
    try:
        with conn, conn.cursor() as cur:
            if retry_count + 1 >= MAX_RETRY:
                cur.execute("UPDATE tasks SET status='FAILED', last_error=%s, retry_count=%s, updated_at=now() WHERE task_id=%s", (error_msg, retry_count+1, task_id))
                print(f"[worker] task {task_id} FAILED after {retry_count + 1} attempts.")
            else:
                cur.execute("UPDATE tasks SET status='PENDING', last_error=%s, retry_count=%s, updated_at=now() WHERE task_id=%s", (error_msg, retry_count+1, task_id))
                print(f"[worker] task {task_id} will retry (attempt {retry_count + 1} ).")
    finally:
        conn.close()

def run_loop():
    print("[scheduler] started.")
    while True:
        claim=claim_one_task()
        if claim is None:
            time.sleep(2)
            continue
        task_id, payload, retry_count = claim
        if retry_count > 0:
            backoff = 2 ** retry_count
            print(f"[worker] backoff {backoff}s before retry")
            time.sleep(backoff)
        try:
            result = execute_task(task_id, payload)
            mark_succeeded(task_id, result)
            print(f"[worker] task {task_id} done")
        except Exception as e:
            mark_failed_or_retry(task_id, retry_count, str(e))

if __name__ == "__main__":
    run_loop()


