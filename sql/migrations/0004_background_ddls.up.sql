BEGIN;

CREATE TABLE background_ddl_jobs (
    id UUID PRIMARY KEY,
    cluster_uuid UUID NOT NULL REFERENCES clusters(cluster_uuid) ON DELETE CASCADE,
    database_name TEXT NOT NULL,
    statement TEXT NOT NULL,
    task_id INTEGER,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at TIMESTAMPTZ,
    cancel_requested_at TIMESTAMPTZ,
    cancelled_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    failed_at TIMESTAMPTZ,
    failure_reason TEXT
);

CREATE INDEX idx_background_ddl_jobs_created_at ON background_ddl_jobs (created_at DESC);
CREATE INDEX idx_background_ddl_jobs_cluster_database_created_at ON background_ddl_jobs (cluster_uuid, database_name, created_at DESC);

CREATE TABLE background_ddl_progresses (
    id UUID PRIMARY KEY,
    job_id UUID NOT NULL REFERENCES background_ddl_jobs(id) ON DELETE CASCADE,
    seq INTEGER NOT NULL,
    statement TEXT NOT NULL,
    statement_kind TEXT NOT NULL CHECK (statement_kind IN ('SET', 'TRACKED_DDL', 'DIRECT')),
    target_kind TEXT NOT NULL CHECK (target_kind IN ('none', 'relation', 'function')),
    target_type TEXT,
    target_schema TEXT,
    target_name TEXT,
    target_identity TEXT,
    expect_relation_exists BOOLEAN,
    rw_job_ids BIGINT[] NOT NULL DEFAULT '{}',
    dispatched_at TIMESTAMPTZ,
    started_at TIMESTAMPTZ,
    last_progress DOUBLE PRECISION,
    last_progress_tracked_at TIMESTAMPTZ,
    estimated_finished_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    cancelled_at TIMESTAMPTZ,
    failed_at TIMESTAMPTZ,
    failure_reason TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (job_id, seq)
);

CREATE INDEX idx_background_ddl_progresses_job_seq ON background_ddl_progresses (job_id, seq);
CREATE INDEX idx_background_ddl_progresses_job_running ON background_ddl_progresses (job_id, seq)
WHERE finished_at IS NULL AND failed_at IS NULL AND cancelled_at IS NULL;

COMMIT;
