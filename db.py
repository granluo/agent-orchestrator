import psycopg2
import json
import os

password=os.environ.get("DB_PASSWORD", "devpass")

def get_conn():
    return psycopg2.connect(
        host="localhost", port=5432,
        dbname="orchestrator", user="postgres", password=password
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

def compute_metrics() -> dict:
    conn = get_conn()
    metrics = {}
    try:
        with conn, conn.cursor() as cur:
            cur.execute("SELECT status, COUNT(*) FROM tasks GROUP BY status")
            by_status = dict(cur.fetchall())
            cur.execute("SELECT route, SUM(cost) FROM tasks WHERE route IS NOT NULL GROUP BY route")
            cost_by_route = dict(cur.fetchall())
            cur.execute("SELECT AVG(retry_count), COUNT(*) FILTER (WHERE delivery_count>1),AVG(duration_seconds) FROM tasks ")
            row = cur.fetchone()
            avg_retry = 0.0
            if row[0] is not None:
                avg_retry = float(round(row[0], 2))
            reclaimed_count = row[1]
            avg_duration_seconds = row[2]
            metrics['by_status'] = by_status
            metrics['avg_retry'] = avg_retry
            metrics['reclaimed_count'] = reclaimed_count
            metrics['avg_duration_seconds'] = avg_duration_seconds
            metrics['cost_by_route'] = cost_by_route
            return metrics

    finally:
        conn.close()

if __name__ == '__main__':
    print(compute_metrics())
