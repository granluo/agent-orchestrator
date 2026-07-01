import db, time, json, random, threading

MAX_RETRY=3
MAX_DELIVERY=3
THRESHOLD=100
PENDING_THRESHOLD=5
LOCAL_COST=0.001
CLOUD_COST=0.01

# get task

def claim_one_task():

    conn = db.get_conn()
    try:
        while True:
            with conn, conn.cursor() as cur:
                cur.execute("""
                SELECT task_id, payload, retry_count, delivery_count, route FROM tasks
                WHERE status = 'PENDING'
                ORDER BY created_at
                FOR UPDATE SKIP LOCKED
                LIMIT 1
                """)
                row = cur.fetchone()
                if row is None:
                    return None
                task_id, payload, retry_count, delivery_count, route = row
                if delivery_count + 1 > MAX_DELIVERY:
                    cur.execute("UPDATE tasks SET status='FAILED', last_error='exceeded max delivery', updated_at=now() WHERE task_id=%s", (task_id,))
                    print(f"[worker] task {task_id} FAILED after {delivery_count + 1} delivery.")
                    continue
                else:
                    cur.execute(
                        "UPDATE tasks SET status='RUNNING', delivery_count = delivery_count + 1, updated_at=now() WHERE task_id=%s", (task_id,)
                        )
                    return task_id, payload, retry_count, route
    finally:
        conn.close()

def decide_route(payload, metrics):
    prompt = payload.get("prompt", "")
    if len(prompt) > THRESHOLD:
        return 'cloud'
    pending = metrics.get("by_status", {}).get("PENDING", 0)
    if pending > PENDING_THRESHOLD:
        return 'cloud'
    return 'local'
class ExecutionError(Exception):
    def __init__(self, message, cost):
        super().__init__(message)
        self.cost =cost

# execute task
def execute_task(task_id, payload, route):
    conn = db.get_conn()
    try:
        with conn, conn.cursor() as cur:
            cur.execute("UPDATE tasks SET started_at=now(), updated_at=now(), route=%s WHERE task_id=%s", (route, task_id))
    finally:
        conn.close()
    print(f"[worker] running task {task_id} through {route} : {payload}")
    if route == 'cloud':
        return execute_cloud(task_id, payload)
    else:
        return execute_local(task_id, payload)


def execute_local(task_id, payload):
    print(f"[worker] task {task_id} is running locally.")
    time.sleep(8)
    if random.random() < 0.3 :
        raise ExecutionError("simulated local transient failure", LOCAL_COST)
    return ({"echo": payload.get("prompt", "")}, LOCAL_COST)

def execute_cloud(task_id, payload):
    print(f"[worker] task {task_id} is running on the cloud.")
    time.sleep(2)
    if random.random() < 0.3 :
        raise ExecutionError("simulated cloud transient failure", CLOUD_COST)
    return ({"echo": payload.get("prompt", "")},  CLOUD_COST)

def mark_succeeded(task_id, result, cost):
    conn = db.get_conn()
    try:
        with conn, conn.cursor() as cur:
            cur.execute("UPDATE tasks SET status='SUCCEEDED', result=%s, cost=%s, updated_at=now(), duration_seconds = EXTRACT(EPOCH FROM (now() - started_at)) WHERE task_id=%s",
                        (json.dumps(result), cost, task_id)
                        )
    except Exception as e:
        print(f"Failed to mark succeeded, {e}")
        raise e
    finally:
        conn.close()


def mark_failed_or_retry(task_id, retry_count, cost, error_msg):
    conn = db.get_conn()
    try:
        with conn, conn.cursor() as cur:
            if retry_count + 1 >= MAX_RETRY:
                cur.execute("UPDATE tasks SET status='FAILED', last_error=%s, retry_count=%s, cost=cost+%s,  updated_at=now() WHERE task_id=%s", (error_msg, retry_count+1, cost, task_id))
                print(f"[worker] task {task_id} FAILED after {retry_count + 1} attempts.")
            else:
                cur.execute("UPDATE tasks SET status='PENDING', last_error=%s, retry_count=%s, cost=cost+%s, updated_at=now() WHERE task_id=%s", (error_msg, retry_count+1, cost, task_id))
                print(f"[worker] task {task_id} will retry (attempt {retry_count + 1} ).")
    finally:
        conn.close()

def heartbeat(task_id, stop_event):
    conn =db.get_conn()
    try:
        while not stop_event.is_set():
            with conn, conn.cursor() as cur:
                print(f"[heartbeat] task {task_id} extends lease")
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
        task_id, payload, retry_count, route = claim
        if route is None:
            # TODO: get metrics for each task claim is heavy, might cache it 
            metrics = db.compute_metrics()
            route = decide_route(payload, metrics)
        if retry_count > 0:
            backoff = 2 ** retry_count
            print(f"[worker] backoff {backoff}s before retry")
            time.sleep(backoff)
        stop_event = threading.Event()
        hb_thread = threading.Thread(target=heartbeat, args=(task_id, stop_event))
        hb_thread.daemon = True
        hb_thread.start()
        try:
            result, cost = execute_task(task_id, payload, route)
            mark_succeeded(task_id, result, cost)
            print(f"[worker] task {task_id} done")
        except Exception as e:
            mark_failed_or_retry(task_id, retry_count, e.cost, str(e))
        finally:
            stop_event.set()
            hb_thread.join()

if __name__ == "__main__":
    run_loop()


