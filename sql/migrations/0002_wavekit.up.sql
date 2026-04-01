BEGIN;

CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE clusters (
    cluster_uuid UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    cluster_name TEXT NOT NULL UNIQUE,
    sql_connection_string TEXT NOT NULL,
    meta_node_grpc_url TEXT NOT NULL,
    meta_node_http_url TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE notebooks (
    notebook_uuid UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    notebook_name TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE notebook_cells (
    cell_uuid UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    notebook_uuid UUID NOT NULL REFERENCES notebooks(notebook_uuid) ON DELETE CASCADE,
    cell_type TEXT NOT NULL CHECK (cell_type IN ('SQL', 'Shell')),
    cluster_uuid UUID REFERENCES clusters(cluster_uuid) ON DELETE SET NULL,
    database_name TEXT,
    content TEXT NOT NULL DEFAULT '',
    order_index INTEGER NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (notebook_uuid, order_index)
);

CREATE INDEX idx_notebook_cells_notebook_order ON notebook_cells (notebook_uuid, order_index);

COMMIT;
