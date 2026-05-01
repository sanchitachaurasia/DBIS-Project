/* contrib/parquet_gsi/parquet_gsi--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION parquet_gsi" to load this file. \quit

-- -------------------------------------------------------------------------
-- Core catalog tables
-- indexed_files stores one row per discovered Parquet file, while row_group_zonemap
-- stores the per-column row-group statistics that power zone-map pruning. The pair
-- acts as the durable metadata contract between the worker, SQL helpers, and FDW.
-- -------------------------------------------------------------------------

CREATE TABLE indexed_files (
    file_path        text PRIMARY KEY,
    file_size        bigint,
    file_mtime       double precision,
    row_count        bigint,
    row_group_count  integer NOT NULL,
    indexed_at       timestamptz NOT NULL DEFAULT now(),
    retry_count      integer NOT NULL DEFAULT 0,
    last_error_at    timestamptz,
    last_error       text,
    index_status     text NOT NULL DEFAULT 'indexed'
        CHECK (index_status IN ('indexed', 'failed', 'pending'))
);

CREATE TABLE row_group_zonemap (
    indexed_column    text NOT NULL,
    file_path         text NOT NULL REFERENCES indexed_files(file_path) ON DELETE CASCADE,
    row_group_id      integer NOT NULL,
    row_count         bigint NOT NULL,
    null_count        bigint NOT NULL DEFAULT 0,
    min_value_text    text,
    max_value_text    text,
    min_value_numeric double precision,
    max_value_numeric double precision,
    indexed_at        timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (indexed_column, file_path, row_group_id)
);

-- Indexes that make the zone-map lookup fast (this is the GSI B-tree)
-- The planner path depends on these composite indexes so range predicates can
-- narrow candidate files and row groups without scanning every catalog row.
-- Without these indexes the pruning query would devolve into a catalog scan and
-- the extension would lose most of its performance advantage.
CREATE INDEX row_group_zonemap_column_numeric_bounds_idx
    ON row_group_zonemap (indexed_column, min_value_numeric, max_value_numeric);

CREATE INDEX row_group_zonemap_column_text_bounds_idx
    ON row_group_zonemap (indexed_column, min_value_text, max_value_text);

CREATE INDEX row_group_zonemap_file_row_group_idx
    ON row_group_zonemap (file_path, row_group_id);

CREATE TABLE parquet_gsi_discovered_files (
    file_path      text PRIMARY KEY,
    file_size      bigint NOT NULL,
    file_mtime     double precision NOT NULL,
    discovered_at  timestamptz NOT NULL DEFAULT now(),
    last_seen_at   timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE parquet_gsi_worker_state (
    worker_name       text PRIMARY KEY,
    pid               integer,
    database_name     text,
    directory_path    text,
    last_start_at     timestamptz NOT NULL DEFAULT now(),
    last_heartbeat_at timestamptz NOT NULL DEFAULT now(),
    last_scan_at      timestamptz,
    files_seen        integer NOT NULL DEFAULT 0,
    files_registered  integer NOT NULL DEFAULT 0,
    last_error        text
);

CREATE TABLE parquet_gsi_indexed_columns (
    column_name text PRIMARY KEY,
    created_at  timestamptz NOT NULL DEFAULT now()
);

-- -------------------------------------------------------------------------
-- Views
-- These helper views give operators a quick status snapshot without requiring
-- them to join the raw catalog tables by hand. They are also stable reporting
-- surfaces for dashboards and benchmark summaries.
-- -------------------------------------------------------------------------

CREATE VIEW parquet_gsi_coverage AS
SELECT
    f.file_path,
    f.row_count,
    f.row_group_count,
    f.indexed_at,
    count(z.*) AS zonemap_rows
FROM indexed_files AS f
LEFT JOIN row_group_zonemap AS z ON z.file_path = f.file_path
GROUP BY f.file_path, f.row_count, f.row_group_count, f.indexed_at;

CREATE VIEW parquet_gsi_worker_status AS
SELECT
    worker_name,
    pid,
    database_name,
    directory_path,
    last_start_at,
    last_heartbeat_at,
    last_scan_at,
    files_seen,
    files_registered,
    last_error
FROM parquet_gsi_worker_state;

CREATE VIEW parquet_gsi_index_status AS
SELECT
    file_path,
    file_size,
    file_mtime,
    row_count,
    row_group_count,
    indexed_at,
    retry_count,
    last_error_at,
    index_status,
    last_error
FROM indexed_files;

CREATE VIEW parquet_gsi_failed_files AS
SELECT
    file_path,
    file_size,
    file_mtime,
    retry_count,
    last_error_at,
    last_error
FROM indexed_files
WHERE index_status = 'failed';

CREATE VIEW parquet_gsi_unindexed_files AS
SELECT
    d.file_path,
    d.file_size,
    d.file_mtime,
    d.discovered_at,
    d.last_seen_at
FROM parquet_gsi_discovered_files AS d
LEFT JOIN indexed_files AS i ON i.file_path = d.file_path
WHERE i.file_path IS NULL
   OR i.index_status <> 'indexed'
   OR i.file_mtime IS DISTINCT FROM d.file_mtime;

-- -------------------------------------------------------------------------
-- Core SQL query functions
-- The SQL helpers all work in terms of file paths and row-group IDs so callers
-- can reuse the same pruning logic from SQL, Python, and the FDW layer. That keeps
-- the pruning rules centralized instead of duplicating them in each client.
-- -------------------------------------------------------------------------

-- Returns (file_path, row_group_id) pairs whose min/max range overlaps the
-- given bounds. This is the heart of the pruning logic: if a row-group bound
-- cannot overlap the predicate, that row group can be skipped entirely. The OR
-- terms intentionally stay conservative so NULL statistics never hide a match.
CREATE FUNCTION parquet_gsi_candidate_row_groups(
    p_column text,
    p_lower_numeric double precision DEFAULT NULL,
    p_upper_numeric double precision DEFAULT NULL,
    p_lower_text text DEFAULT NULL,
    p_upper_text text DEFAULT NULL
)
RETURNS TABLE(file_path text, row_group_id integer)
LANGUAGE sql
STABLE
AS $$
    SELECT z.file_path, z.row_group_id
    FROM row_group_zonemap AS z
    WHERE z.indexed_column = p_column
      AND (
            p_lower_numeric IS NULL
            OR z.max_value_numeric IS NULL
            OR z.max_value_numeric >= p_lower_numeric
          )
      AND (
            p_upper_numeric IS NULL
            OR z.min_value_numeric IS NULL
            OR z.min_value_numeric <= p_upper_numeric
          )
      AND (
            p_lower_text IS NULL
            OR z.max_value_text IS NULL
            OR z.max_value_text >= p_lower_text
          )
      AND (
            p_upper_text IS NULL
            OR z.min_value_text IS NULL
            OR z.min_value_text <= p_upper_text
          )
    ORDER BY z.file_path, z.row_group_id;
$$;

-- Returns distinct candidate file paths (collapses row groups to file level).
-- This is the file-pruning view used by higher-level callers that do not need
-- to reason about individual row groups. It is the most common entry point for
-- callers that only need file-level pruning decisions.
CREATE FUNCTION parquet_gsi_candidate_files(
    p_column text,
    p_lower_numeric double precision DEFAULT NULL,
    p_upper_numeric double precision DEFAULT NULL,
    p_lower_text text DEFAULT NULL,
    p_upper_text text DEFAULT NULL
)
RETURNS TABLE(file_path text)
LANGUAGE sql
STABLE
AS $$
    SELECT DISTINCT c.file_path
    FROM parquet_gsi_candidate_row_groups(
        p_column,
        p_lower_numeric,
        p_upper_numeric,
        p_lower_text,
        p_upper_text
    ) AS c
    ORDER BY c.file_path;
$$;

-- Full query helper: indexed candidate files UNION fallback files not yet indexed.
-- The scan_mode column tells callers whether a file was selected by the index
-- ('indexed') or needs a full scan because it is not yet covered ('fallback_full_scan').
-- This lets clients preserve correctness while still taking advantage of pruning.
-- It also gives the executor a single place to distinguish fast-path and fallback
-- files when a data lake is only partially indexed.
CREATE FUNCTION parquet_gsi_query_files(
    p_column text,
    p_lower_numeric double precision DEFAULT NULL,
    p_upper_numeric double precision DEFAULT NULL,
    p_lower_text text DEFAULT NULL,
    p_upper_text text DEFAULT NULL,
    p_include_unindexed boolean DEFAULT true
)
RETURNS TABLE(file_path text, scan_mode text)
LANGUAGE sql
STABLE
AS $$
    SELECT c.file_path, 'indexed'::text
    FROM parquet_gsi_candidate_files(
        p_column,
        p_lower_numeric,
        p_upper_numeric,
        p_lower_text,
        p_upper_text
    ) AS c
    UNION
    SELECT u.file_path, 'fallback_full_scan'::text
    FROM parquet_gsi_unindexed_files AS u
    WHERE p_include_unindexed
    ORDER BY file_path, scan_mode;
$$;

-- -------------------------------------------------------------------------
-- C-backed administrative functions (BGW + indexing)
-- These functions are the extension's operational surface: background scanning,
-- one-shot indexing, bulk reindexing, and indexed-column registration. The Python
-- integration scripts call into this same surface so the behavior stays consistent.
-- -------------------------------------------------------------------------

CREATE FUNCTION parquet_gsi_scan_once(directory text DEFAULT NULL)
RETURNS integer
AS 'MODULE_PATHNAME', 'parquet_gsi_scan_once'
LANGUAGE C;

CREATE FUNCTION parquet_gsi_launch_worker()
RETURNS integer
AS 'MODULE_PATHNAME', 'parquet_gsi_launch_worker'
LANGUAGE C;

CREATE FUNCTION parquet_gsi_index_file(file_path text)
RETURNS integer
AS 'MODULE_PATHNAME', 'parquet_gsi_index_file'
LANGUAGE C;

CREATE FUNCTION parquet_gsi_reindex_all(directory text DEFAULT NULL)
RETURNS integer
AS 'MODULE_PATHNAME', 'parquet_gsi_reindex_all'
LANGUAGE C;

CREATE FUNCTION parquet_gsi_add_indexed_column(column_name text)
RETURNS integer
AS 'MODULE_PATHNAME', 'parquet_gsi_add_indexed_column'
LANGUAGE C;

CREATE FUNCTION parquet_gsi_remove_indexed_column(column_name text)
RETURNS integer
AS 'MODULE_PATHNAME', 'parquet_gsi_remove_indexed_column'
LANGUAGE C;

CREATE FUNCTION parquet_gsi_reconcile_missing_files()
RETURNS integer
AS 'MODULE_PATHNAME', 'parquet_gsi_reconcile_missing_files'
LANGUAGE C;

-- -------------------------------------------------------------------------
-- FDW handler — this is the new piece that makes the planner automatically
-- use the GSI index when a foreign table is queried with a predicate on an
-- indexed column.
-- The handler exposes a normal PostgreSQL FDW, but its row estimates and scan
-- plan are derived from the parquet_gsi catalog instead of raw table stats.
-- That keeps the pruning logic inside PostgreSQL's normal planner pipeline.
-- -------------------------------------------------------------------------

CREATE FUNCTION parquet_gsi_fdw_handler()
RETURNS fdw_handler
AS 'MODULE_PATHNAME', 'parquet_gsi_fdw_handler'
LANGUAGE C STRICT;

CREATE FUNCTION parquet_gsi_fdw_validator(options text[], catalog oid)
RETURNS void
AS 'MODULE_PATHNAME', 'parquet_gsi_fdw_validator'
LANGUAGE C STRICT;

-- The FDW itself. A foreign table using this FDW will automatically benefit
-- from GSI pruning when queried with predicates on indexed columns.
CREATE FOREIGN DATA WRAPPER parquet_gsi_fdw
    HANDLER parquet_gsi_fdw_handler
    VALIDATOR parquet_gsi_fdw_validator;

-- -------------------------------------------------------------------------
-- Comments
-- The comments below document the schema contract so future changes can keep
-- the worker, FDW, and SQL helper functions aligned. Any change to these
-- comments should be mirrored in the Python integration layer and worker code.
-- -------------------------------------------------------------------------

COMMENT ON TABLE indexed_files IS
'Tracks which Parquet files have been indexed by the parquet_gsi worker.';

COMMENT ON TABLE row_group_zonemap IS
'Stores one zonemap row per (indexed column, file, row group). '
'The B-tree indexes on this table are the heart of the GSI.';

COMMENT ON TABLE parquet_gsi_discovered_files IS
'Tracks Parquet files discovered by the native parquet_gsi background worker.';

COMMENT ON TABLE parquet_gsi_worker_state IS
'Tracks parquet_gsi background worker heartbeat, directory scan status, and last error.';

COMMENT ON TABLE parquet_gsi_indexed_columns IS
'Lists the parquet column names that parquet_gsi should index. If empty, all metadata columns are indexed.';

COMMENT ON FUNCTION parquet_gsi_candidate_row_groups(text, double precision, double precision, text, text) IS
'Returns candidate file/row-group pairs whose min/max ranges overlap the requested bounds.';

COMMENT ON FUNCTION parquet_gsi_candidate_files(text, double precision, double precision, text, text) IS
'Returns distinct candidate file paths whose row-group zonemaps overlap the requested bounds.';

COMMENT ON FUNCTION parquet_gsi_query_files(text, double precision, double precision, text, text, boolean) IS
'Returns indexed candidate files plus optional fallback full-scan files for unindexed or stale Parquet files.';

COMMENT ON FUNCTION parquet_gsi_scan_once(text) IS
'Scans the configured Parquet directory once and registers discovered files.';

COMMENT ON FUNCTION parquet_gsi_launch_worker() IS
'Launches a parquet_gsi dynamic background worker for the current database.';

COMMENT ON FUNCTION parquet_gsi_index_file(text) IS
'Indexes one Parquet file by loading row-group metadata from pg_parquet parquet.metadata() and parquet.file_metadata().';

COMMENT ON FUNCTION parquet_gsi_reindex_all(text) IS
'Scans and reindexes all discovered Parquet files.';

COMMENT ON FUNCTION parquet_gsi_add_indexed_column(text) IS
'Registers a parquet column name that parquet_gsi should index.';

COMMENT ON FUNCTION parquet_gsi_remove_indexed_column(text) IS
'Removes a parquet column name from the parquet_gsi indexed column configuration.';

COMMENT ON FUNCTION parquet_gsi_reconcile_missing_files() IS
'Removes catalog rows for discovered/indexed files that no longer exist on disk.';

COMMENT ON VIEW parquet_gsi_unindexed_files IS
'Lists discovered Parquet files that are not yet fully covered by the parquet_gsi index.';

COMMENT ON VIEW parquet_gsi_failed_files IS
'Lists files whose latest indexing attempt failed, with retry metadata.';

COMMENT ON FOREIGN DATA WRAPPER parquet_gsi_fdw IS
'FDW that wraps Parquet file reads with automatic GSI-based file pruning. '
'Create a server and foreign table using this FDW; queries with predicates '
'on indexed columns will automatically skip non-matching Parquet files.';
