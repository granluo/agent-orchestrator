CREATE TABLE tasks (
    task_id         BIGSERIAL PRIMARY KEY,
    idempotency_key TEXT UNIQUE,              -- Day 2 用,先建好
    status          TEXT NOT NULL DEFAULT 'PENDING',  -- PENDING/RUNNING/SUCCEEDED/FAILED
    payload         JSONB NOT NULL,           -- task 内容(prompt 等)
    result          JSONB,                    -- 执行结果
    retry_count     INT NOT NULL DEFAULT 0,   -- Day 2 用
    last_error      TEXT,
    lease_expires_at TIMESTAMPTZ,             -- Day 3 用
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    delivery_count INT NOT NULL DEFAULT 0,
    started_at      TIMESTAMPTZ,
    duration_seconds DOUBLE PRECISION
);

-- 索引:scheduler 要快速找 PENDING 任务
CREATE INDEX idx_tasks_status ON tasks (status, created_at);
