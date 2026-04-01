-- name: CreateCluster :one
INSERT INTO clusters (
    cluster_name,
    sql_connection_string,
    meta_node_grpc_url,
    meta_node_http_url
) VALUES (
    $1, $2, $3, $4
)
RETURNING *;

-- name: ListClusters :many
SELECT *
FROM clusters
ORDER BY created_at DESC, cluster_name ASC;

-- name: GetCluster :one
SELECT *
FROM clusters
WHERE cluster_uuid = $1;

-- name: UpdateCluster :one
UPDATE clusters
SET cluster_name = $2,
    sql_connection_string = $3,
    meta_node_grpc_url = $4,
    meta_node_http_url = $5,
    updated_at = NOW()
WHERE cluster_uuid = $1
RETURNING *;

-- name: DeleteCluster :exec
DELETE FROM clusters
WHERE cluster_uuid = $1;
