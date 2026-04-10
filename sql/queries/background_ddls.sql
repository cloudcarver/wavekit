-- name: CreateBackgroundDdlJob :one
INSERT INTO background_ddl_jobs (
    id,
    cluster_uuid,
    database_name,
    statement
) VALUES (
    $1, $2, $3, $4
)
RETURNING *;

-- name: GetBackgroundDdlJob :one
SELECT *
FROM background_ddl_jobs
WHERE id = $1;

-- name: ListBackgroundDdlJobs :many
SELECT *
FROM background_ddl_jobs
ORDER BY created_at DESC, id DESC;

-- name: UpdateBackgroundDdlJobTaskID :exec
UPDATE background_ddl_jobs
SET task_id = $2
WHERE id = $1;

-- name: MarkBackgroundDdlJobStarted :exec
UPDATE background_ddl_jobs
SET started_at = COALESCE(started_at, NOW())
WHERE id = $1;

-- name: MarkBackgroundDdlJobCancelRequested :exec
UPDATE background_ddl_jobs
SET cancel_requested_at = COALESCE(cancel_requested_at, NOW())
WHERE id = $1;

-- name: MarkBackgroundDdlJobCancelled :exec
UPDATE background_ddl_jobs
SET cancelled_at = COALESCE(cancelled_at, NOW())
WHERE id = $1;

-- name: MarkBackgroundDdlJobFinished :exec
UPDATE background_ddl_jobs
SET finished_at = COALESCE(finished_at, NOW())
WHERE id = $1;

-- name: MarkBackgroundDdlJobFailed :exec
UPDATE background_ddl_jobs
SET failed_at = COALESCE(failed_at, NOW()),
    failure_reason = $2
WHERE id = $1;

-- name: CreateBackgroundDdlProgress :exec
INSERT INTO background_ddl_progresses (
    id,
    job_id,
    seq,
    statement,
    statement_kind,
    target_kind,
    target_type,
    target_schema,
    target_name,
    target_identity,
    expect_relation_exists
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11
);

-- name: ListBackgroundDdlProgresses :many
SELECT *
FROM background_ddl_progresses
ORDER BY job_id ASC, seq ASC;

-- name: ListBackgroundDdlProgressesByJob :many
SELECT *
FROM background_ddl_progresses
WHERE job_id = $1
ORDER BY seq ASC;

-- name: GetNextBackgroundDdlProgress :one
SELECT *
FROM background_ddl_progresses
WHERE job_id = $1
  AND finished_at IS NULL
  AND failed_at IS NULL
  AND cancelled_at IS NULL
ORDER BY seq ASC
LIMIT 1;

-- name: MarkBackgroundDdlProgressStarted :exec
UPDATE background_ddl_progresses
SET dispatched_at = COALESCE(dispatched_at, NOW()),
    started_at = COALESCE(started_at, NOW()),
    updated_at = NOW()
WHERE id = $1;

-- name: UpdateBackgroundDdlProgressTracking :exec
UPDATE background_ddl_progresses
SET rw_job_ids = $2,
    last_progress = $3,
    last_progress_tracked_at = $4,
    estimated_finished_at = $5,
    updated_at = NOW()
WHERE id = $1;

-- name: MarkBackgroundDdlProgressFinished :exec
UPDATE background_ddl_progresses
SET finished_at = COALESCE(finished_at, NOW()),
    updated_at = NOW()
WHERE id = $1;

-- name: MarkBackgroundDdlProgressCancelled :exec
UPDATE background_ddl_progresses
SET cancelled_at = COALESCE(cancelled_at, NOW()),
    updated_at = NOW()
WHERE id = $1;

-- name: MarkBackgroundDdlProgressFailed :exec
UPDATE background_ddl_progresses
SET failed_at = COALESCE(failed_at, NOW()),
    failure_reason = $2,
    updated_at = NOW()
WHERE id = $1;

-- name: CancelPendingBackgroundDdlProgresses :exec
UPDATE background_ddl_progresses
SET cancelled_at = COALESCE(cancelled_at, NOW()),
    updated_at = NOW()
WHERE job_id = $1
  AND started_at IS NULL
  AND finished_at IS NULL
  AND failed_at IS NULL
  AND cancelled_at IS NULL;
