BEGIN;

ALTER TABLE notebook_cells
DROP COLUMN IF EXISTS background_ddl;

COMMIT;
