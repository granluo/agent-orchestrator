from fastapi import FastAPI
from pydantic import BaseModel
import db

app = FastAPI()

class TaskRequest(BaseModel):
    prompt: str

@app.post("/tasks")
def submit(req: TaskRequest):
    task_id = db.submit_task({"prompt": req.prompt})
    return {"task_id": task_id, "status": "PENDING"}

@app.get("/tasks/{task_id}")
def get_task(task_id: int):
    conn = db.get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT status, result FROM tasks WHERE task_id=%s", (task_id,))
            row = cur.fetchone()
            if not row:
                return {"error": "not found"}
            return {"task_id": task_id, "status": row[0], "result": row[1]}
    finally:
        conn.close()
