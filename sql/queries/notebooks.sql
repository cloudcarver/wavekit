-- name: CreateNotebook :one
INSERT INTO notebooks (
    notebook_name
) VALUES (
    $1
)
RETURNING *;

-- name: ListNotebooks :many
SELECT *
FROM notebooks
ORDER BY updated_at DESC, created_at DESC;

-- name: GetNotebook :one
SELECT *
FROM notebooks
WHERE notebook_uuid = $1;

-- name: DeleteNotebook :exec
DELETE FROM notebooks
WHERE notebook_uuid = $1;

-- name: UpdateNotebookTimestamp :exec
UPDATE notebooks
SET updated_at = NOW()
WHERE notebook_uuid = $1;

-- name: ListNotebookCells :many
SELECT *
FROM notebook_cells
WHERE notebook_uuid = $1
ORDER BY order_index ASC, created_at ASC;

-- name: GetNotebookCell :one
SELECT *
FROM notebook_cells
WHERE notebook_uuid = $1 AND cell_uuid = $2;

-- name: GetNextNotebookCellOrder :one
SELECT COALESCE(MAX(order_index) + 1, 0)::INTEGER AS next_order
FROM notebook_cells
WHERE notebook_uuid = $1;

-- name: CreateNotebookCell :one
INSERT INTO notebook_cells (
    notebook_uuid,
    cell_type,
    cluster_uuid,
    database_name,
    background_ddl,
    content,
    order_index
) VALUES (
    $1, $2, $3, $4, $5, $6, $7
)
RETURNING *;

-- name: UpdateNotebookCell :one
UPDATE notebook_cells
SET cell_type = $3,
    cluster_uuid = $4,
    database_name = $5,
    background_ddl = $6,
    content = $7,
    updated_at = NOW()
WHERE notebook_uuid = $1 AND cell_uuid = $2
RETURNING *;

-- name: UpdateNotebookCellOrder :exec
UPDATE notebook_cells
SET order_index = $3,
    updated_at = NOW()
WHERE notebook_uuid = $1 AND cell_uuid = $2;

-- name: DeleteNotebookCell :exec
DELETE FROM notebook_cells
WHERE notebook_uuid = $1 AND cell_uuid = $2;
