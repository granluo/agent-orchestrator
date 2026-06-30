CREATE TABLE tasks (
    task_id          BIGSERIAL PRIMARY KEY,
    idempotency_key  TEXT UNIQUE,                       -- dedup key for retried submissions; NULL when not provided
    status           TEXT NOT NULL DEFAULT 'PENDING',   -- PENDING / RUNNING / SUCCEEDED / FAILED
    payload          JSONB NOT NULL,                    -- task input (e.g. prompt)
    result           JSONB,                             -- task output, set on success
    retry_count      INT NOT NULL DEFAULT 0,            -- times execution raised and was retried
    delivery_count   INT NOT NULL DEFAULT 0,            -- times handed to a worker; caps poison tasks
    last_error       TEXT,                              -- last failure reason
    lease_expires_at TIMESTAMPTZ,                       -- lease deadline; reaper reclaims if past or NULL
    started_at       TIMESTAMPTZ,                       -- set when execution begins (excludes queue/backoff)
    duration_seconds DOUBLE PRECISION,                  -- execution time, recorded on success
    route            TEXT,                              -- backend that ran the task: 'local' or 'cloud'
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- claim looks up the oldest PENDING task; this index serves that hot path
CREATE INDEX idx_tasks_status ON tasks (status, created_at);
