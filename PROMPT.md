# WaveKit

Unofficial tool for managing RisingWave clusters.

## Professional Product Prompt

```text
Build a tool for managing RisingWave clusters with the following capabilities:

Core Features
1. Allow users to connect to a RisingWave cluster by providing:
   - Cluster name
   - SQL connection string (postgres://...)
   - Meta node gRPC URL (http://...)
   - Meta node HTTP URL (http://...)
2. Retrieve metadata for Sources, Tables, Sinks, Materialized Views, UDFs, and other objects through internal metadata tables such as rw_relations.
3. Provide a SQL Notebook experience where users can write SQL, save SQL, execute SQL, and inspect query results.
4. Automatically download the matching risectl binary for the target cluster version and use it to run risectl commands.

API Requirements

POST /clusters/connect
- Input: cluster name, SQL connection string, meta node gRPC URL, meta node HTTP URL
- Output: cluster UUID

GET /clusters
- Return the list of connected clusters, including:
  - cluster UUID
  - cluster name
  - connection status
- Validate the SQL connection by running `SELECT 1`.
- Validate the meta node connection by running a simple `risectl` command.

PUT /clusters/{cluster_uuid}
- Update cluster connection settings, including:
  - SQL connection string
  - meta node gRPC URL
  - meta node HTTP URL

DELETE /clusters/{cluster_uuid}
- Remove a connected cluster.

POST /clusters/{cluster_uuid}/{database}/sql
- Input: SQL statement
- Output: SQL execution result

GET /notebooks
- Return the list of SQL notebooks, including notebook UUID and notebook name.

POST /notebooks
- Create a new notebook from a provided notebook name.

GET /notebooks/{notebook_uuid}
- Return the notebook content.
- The response should include SQL content only, without execution results.
- In addition to notebook metadata, the notebook contains a list of cells.
- A cell may support multiple types in the future, but for now only one type is required:
  - SQL: includes connection context (cluster ID and database) and the SQL statement

DELETE /notebooks/{notebook_uuid}
- Delete a notebook.

POST /notebooks/{notebook_uuid}/cells
- Create a new cell.
- Input:
  - cell type (SQL or Shell)
  - connection context (cluster ID and database)
  - SQL statement or shell command
- Output: cell UUID

PUT /notebooks/{notebook_uuid}/cells/{cell_uuid}
- Update cell content, including the SQL statement or shell command.

DELETE /notebooks/{notebook_uuid}/cells/{cell_uuid}
- Delete a cell.

POST /notebooks/{notebook_uuid}/cells/order
- Input: ordered list of cell UUIDs
- Update the display and execution order of cells.

Frontend Requirements
- Style: A minimalist dashboard with muted colors, fine lines, spacious layout, and a clean, utilitarian developer-focused feel.
- The default page should display both:
  - a cluster list, with support for viewing, adding, and deleting clusters
  - a notebook list, with support for viewing, adding, and deleting notebooks
- These two lists should appear on the same page.

Notebook Workspace
- Opening a notebook should navigate to the notebook workspace.
- The workspace should contain three columns:

1. Left column: cluster explorer
   - A hierarchical tree view.
   - Level 1: connected clusters
   - Level 2: databases under each cluster, retrieved via `SHOW DATABASE`
   - Level 3: relation categories under each database, including source, table, materialized view, and sink
   - Level 4: individual relation details, including column information retrieved from metadata tables such as `rw_relations`

2. Middle column: notebook cells
   - Display cells in their defined order.
   - Each cell header should show its connection context (selectable cluster and database).
   - Each cell should provide a Run button.

3. Right column: execution results
   - Render query results in a table view.

Non-Requirements
- No authentication, authorization, or login flow is required.
```

