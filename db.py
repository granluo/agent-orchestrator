import psycopg2
import json

def get_conn():
    return psycopg2.connect(
        host="localhost", port=5432,
        dbname="orchestrator", user="postgres", password="devpass"
    )

def submit_task(payload: dict, idempotency_key: str | None) -> int:
    conn = get_conn()
    try:
        with conn, conn.cursor() as cur:
            if idempotency_key is None:
                cur.execute("INSERT INTO tasks (payload) VALUES (%s) RETURNING task_id",
                (json.dumps(payload),)
                            )
                return cur.fetchone()[0]
            else:
                cur.execute(
                    "INSERT INTO tasks (payload, idempotency_key) VALUES (%s, %s) ON CONFLICT (idempotency_key) DO NOTHING RETURNING task_id",
                    (json.dumps(payload), idempotency_key)
                )
                row = cur.fetchone()
                if row is not None:
                    return row[0]
                cur.execute("SELECT task_id FROM tasks WHERE idempotency_key=%s",
                            (idempotency_key,))
                return cur.fetchone()[0]
    finally:
        conn.close()

def get_status(task_id: int) -> str | None:
    conn = get_conn()
    try:
        with conn, conn.cursor() as cur:
            cur.execute("SELECT status from tasks WHERE task_id=%s",
                        (task_id,)
                        )
            row = cur.fetchone()
            if row is not None:
                return row[0]
            return None
    finally:
        conn.close()

