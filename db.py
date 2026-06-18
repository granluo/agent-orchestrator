import psycopg2
import psycopg2.extras
import json

def get_conn():
    return psycopg2.connect(
        host="localhost", port=5432,
        dbname="orchestrator", user="postgres", password="devpass"
    )

def submit_task(payload: dict) -> int:
    conn = get_conn()
    try:
        with conn, conn.cursor() as cur:
            cur.execute(
                "INSERT INTO tasks (payload) VALUES (%s) RETURNING task_id",
                (json.dumps(payload),)
            )
            task_id = cur.fetchone()[0]
        return task_id
    finally:
        conn.close()
