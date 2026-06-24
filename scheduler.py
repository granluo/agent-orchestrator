import db, time, json, random, threading

MAX_RETRY=3
MAX_DELIVERY=3

# get task

def claim_one_task():

    conn = db.get_conn()
    try:
        while True:
            with conn, conn.cursor() as cur:
                cur.execute("""
                SELECT task_id, payload, retry_count, delivery_count FROM tasks
                WHERE status = 'PENDING'
                ORDER BY created_at
                FOR UPDATE SKIP LOCKED
                LIMIT 1
                """)
                row = cur.fetchone()
                if row is None:
                    return None
                task_id, payload, retry_count, delivery_count = row
                if delivery_count + 1 > MAX_DELIVERY:
                    cur.execute("UPDATE tasks SET status='FAILED', last_error='exceeded max delivery', updated_at=now() WHERE task_id=%s", (task_id,))
                    print(f"[worker] task {task_id} FAILED after {delivery_count + 1} delivery.")
                    continue
                else:
                    cur.execute(
                        "UPDATE tasks SET status='RUNNING', delivery_count = delivery_count + 1, updated_at=now() WHERE task_id=%s", (task_id,)
                        )
                    return task_id, payload, retry_count
    finally:
        conn.close()

# execute task
def execute_task(task_id, payload):
    print(f"[worker] running task {task_id}: {payload}")
    time.sleep(25)
    print(f"executed")
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
                cur.execute("UPDATE tasks SET status='FAILED', last_error=%s, retry_count=%s,  updated_at=now() WHERE task_id=%s", (error_msg, retry_count+1,  task_id))
                print(f"[worker] task {task_id} FAILED after {retry_count + 1} attempts.")
            else:
                cur.execute("UPDATE tasks SET status='PENDING', last_error=%s, retry_count=%s, updated_at=now() WHERE task_id=%s", (error_msg, retry_count+1, task_id))
                print(f"[worker] task {task_id} will retry (attempt {retry_count + 1} ).")
    finally:
        conn.close()

def heartbeat(task_id, stop_event):
    conn =db.get_conn()
    try:
        while not stop_event.is_set():
            with conn, conn.cursor() as cur:
                print(f"task {task_id} extends lease")
                cur.execute("UPDATE tasks SET lease_expires_at = now() + interval '30 seconds' WHERE task_id = %s", (task_id,))
                stop_event.wait(10)

    finally:
        conn.close()

def reaper():
    conn = db.get_conn()
    try:
        with conn, conn.cursor() as cur:
            cur.execute("UPDATE tasks SET lease_expires_at=NULL, status='PENDING' WHERE (lease_expires_at is NULL OR now() > lease_expires_at) AND status='RUNNING';")
            print(f"[reaper] reclaimed {cur.rowcount} tasks.")
    finally:
        conn.close()

def run_loop():
    print("[scheduler] started.")
    while True:
        reaper()
        claim=claim_one_task()
        if claim is None:
            time.sleep(2)
            continue
        task_id, payload, retry_count = claim
        if retry_count > 0:
            backoff = 2 ** retry_count
            print(f"[worker] backoff {backoff}s before retry")
            time.sleep(backoff)
        stop_event = threading.Event()
        hb_thread = threading.Thread(target=heartbeat, args=(task_id, stop_event))
        hb_thread.daemon = True
        hb_thread.start()
        try:
            result = execute_task(task_id, payload)
            mark_succeeded(task_id, result)
            print(f"[worker] task {task_id} done")
        except Exception as e:
            mark_failed_or_retry(task_id, retry_count, str(e))
        finally:
            stop_event.set()
            hb_thread.join()

if __name__ == "__main__":
    run_loop()


