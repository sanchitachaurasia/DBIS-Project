#!/usr/bin/env bash
# =============================================================================
# setup.sh  –  Build, install, and run the full parquet_gsi stack
#
# Usage:
#   ./scripts/setup.sh [--pg-src /path/to/postgres] [--data-dir benchmarks/testdata/parquet_gsi_bench]
#                      [--dsn "host=... dbname=..."] [--runs 5]
#
# What this script does:
#   1.  Apply the postgres/contrib/Makefile and meson.build patches
#   2.  Build and install the parquet_gsi C extension (via pg_config / pgxs)
#   3.  Create the extension in PostgreSQL
#   4.  Register user_id as an indexed column
#   5.  Run the Python indexer daemon (one-shot mode) to populate the catalog
#   6.  Run the single end-to-end benchmark
# =============================================================================

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------
PG_SRC=""
DATA_DIR="$REPO_ROOT/benchmarks/testdata/parquet_gsi_bench"
DSN="host=localhost dbname=postgres user=$(whoami)"
RUNS=5
NUM_FILES=100
ROWS_PER_FILE=50000
TARGET_USER_ID=42
SKIP_BUILD=0
SKIP_GENERATE=0
SKIP_REINDEX=0

# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------
while [[ $# -gt 0 ]]; do
    case "$1" in
        --pg-src)      PG_SRC="$2";         shift 2 ;;
        --data-dir)    DATA_DIR="$2";        shift 2 ;;
        --dsn)         DSN="$2";             shift 2 ;;
        --runs)        RUNS="$2";            shift 2 ;;
        --num-files)   NUM_FILES="$2";       shift 2 ;;
        --rows)        ROWS_PER_FILE="$2";   shift 2 ;;
        --target-uid)  TARGET_USER_ID="$2";  shift 2 ;;
        --skip-build)  SKIP_BUILD=1;         shift   ;;
        --skip-generate) SKIP_GENERATE=1;   shift   ;;
        --skip-reindex)  SKIP_REINDEX=1;    shift   ;;
        *) echo "Unknown option: $1" >&2; exit 1 ;;
    esac
done

PARQUET_GSI_DIR="$REPO_ROOT/postgres/contrib/parquet_gsi"
PG_PARQUET_SQL="$REPO_ROOT/pg_parquet/sql/pg_parquet.sql"

echo "================================================================"
echo "  parquet_gsi setup"
echo "  repo  : $REPO_ROOT"
echo "  data  : $DATA_DIR"
echo "  dsn   : $DSN"
echo "================================================================"
echo ""

# ---------------------------------------------------------------------------
# Step 1: Apply contrib Makefile / meson.build patches
# ---------------------------------------------------------------------------
echo ""
echo "[1/6] Patching postgres/contrib/Makefile and meson.build ..."
CONTRIB_MAKEFILE="$REPO_ROOT/postgres/contrib/Makefile"
CONTRIB_MESON="$REPO_ROOT/postgres/contrib/meson.build"

if [[ -f "$CONTRIB_MAKEFILE" ]]; then
    if ! grep -q 'parquet_gsi' "$CONTRIB_MAKEFILE"; then
        sed -i '/passwordcheck/a \\t\tparquet_gsi\t\\' "$CONTRIB_MAKEFILE"
        echo "    Patched Makefile."
    else
        echo "    Makefile already contains parquet_gsi."
    fi
fi

if [[ -f "$CONTRIB_MESON" ]]; then
    if ! grep -q 'parquet_gsi' "$CONTRIB_MESON"; then
        sed -i "/subdir('passwordcheck')/a subdir('parquet_gsi')" "$CONTRIB_MESON"
        echo "    Patched meson.build."
    else
        echo "    meson.build already contains parquet_gsi."
    fi
fi

# ---------------------------------------------------------------------------
# Step 2: Build and install the C extension
# ---------------------------------------------------------------------------
echo ""
echo "[2/6] Building parquet_gsi C extension ..."
if [[ $SKIP_BUILD -eq 1 ]]; then
    echo "    Skipped (--skip-build)."
else
    if [[ -n "$PG_SRC" && -d "$PG_SRC" ]]; then
        # In-tree build inside the postgres source tree
        echo "    Building in-tree inside $PG_SRC ..."
        DEST="$PG_SRC/contrib/parquet_gsi"
        mkdir -p "$DEST"
        cp -r "$PARQUET_GSI_DIR/"* "$DEST/"
        (cd "$PG_SRC" && make -C contrib/parquet_gsi install)
    else
        # Out-of-tree build via PGXS
        echo "    Building out-of-tree (USE_PGXS=1) ..."
        (cd "$PARQUET_GSI_DIR" && make USE_PGXS=1 install)
    fi
    echo "    Build done."
fi

# ---------------------------------------------------------------------------
# Step 3: Create extension in PostgreSQL
# ---------------------------------------------------------------------------
echo ""
echo "[3/6] Creating extension in PostgreSQL ..."
psql "$DSN" <<'SQL'
CREATE TABLE IF NOT EXISTS indexed_files (
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

CREATE TABLE IF NOT EXISTS row_group_zonemap (
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

CREATE INDEX IF NOT EXISTS row_group_zonemap_column_numeric_bounds_idx
    ON row_group_zonemap (indexed_column, min_value_numeric, max_value_numeric);

CREATE INDEX IF NOT EXISTS row_group_zonemap_column_text_bounds_idx
    ON row_group_zonemap (indexed_column, min_value_text, max_value_text);

CREATE INDEX IF NOT EXISTS row_group_zonemap_file_row_group_idx
    ON row_group_zonemap (file_path, row_group_id);

CREATE TABLE IF NOT EXISTS parquet_gsi_discovered_files (
    file_path      text PRIMARY KEY,
    file_size      bigint NOT NULL,
    file_mtime     double precision NOT NULL,
    discovered_at  timestamptz NOT NULL DEFAULT now(),
    last_seen_at   timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS parquet_gsi_worker_state (
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

CREATE TABLE IF NOT EXISTS parquet_gsi_indexed_columns (
    column_name text PRIMARY KEY,
    created_at  timestamptz NOT NULL DEFAULT now()
);

CREATE OR REPLACE FUNCTION parquet_gsi_candidate_row_groups(
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
      AND (p_lower_numeric IS NULL OR z.max_value_numeric IS NULL OR z.max_value_numeric >= p_lower_numeric)
      AND (p_upper_numeric IS NULL OR z.min_value_numeric IS NULL OR z.min_value_numeric <= p_upper_numeric)
      AND (p_lower_text IS NULL OR z.max_value_text IS NULL OR z.max_value_text >= p_lower_text)
      AND (p_upper_text IS NULL OR z.min_value_text IS NULL OR z.min_value_text <= p_upper_text)
    ORDER BY z.file_path, z.row_group_id;
$$;

CREATE OR REPLACE FUNCTION parquet_gsi_candidate_files(
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

CREATE OR REPLACE FUNCTION parquet_gsi_query_files(
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
    SELECT d.file_path, 'fallback_full_scan'::text
    FROM parquet_gsi_discovered_files AS d
    WHERE p_include_unindexed
    ORDER BY 1, 2;
$$;

CREATE OR REPLACE FUNCTION parquet_gsi_add_indexed_column(column_name text)
RETURNS integer
LANGUAGE sql
AS $$
    INSERT INTO parquet_gsi_indexed_columns(column_name)
    VALUES ($1)
    ON CONFLICT (column_name) DO NOTHING;
    SELECT 1;
$$;

CREATE OR REPLACE FUNCTION parquet_gsi_remove_indexed_column(column_name text)
RETURNS integer
LANGUAGE sql
AS $$
    DELETE FROM parquet_gsi_indexed_columns WHERE column_name = $1;
    SELECT 1;
$$;

SELECT parquet_gsi_add_indexed_column('user_id');
SELECT parquet_gsi_add_indexed_column('amount');
SQL

echo "    Extension created."

# ---------------------------------------------------------------------------
# Step 4: Generate Parquet test data
# ---------------------------------------------------------------------------
echo ""
echo "[4/6] Generating Parquet test data ..."
if [[ $SKIP_GENERATE -eq 1 ]]; then
    echo "    Skipped (--skip-generate)."
else
    python3 - <<PYEOF
import sys, os
sys.path.insert(0, "$REPO_ROOT")
from benchmarks.benchmark_e2e import generate_parquet_files
generate_parquet_files("$DATA_DIR", $NUM_FILES, $ROWS_PER_FILE)
PYEOF
fi

# ---------------------------------------------------------------------------
# Step 5: Index all files (Python daemon, one-shot via run.py)
# ---------------------------------------------------------------------------
echo ""
echo "[5/6] Indexing Parquet files ..."
if [[ $SKIP_REINDEX -eq 1 ]]; then
    echo "    Skipped (--skip-reindex)."
else
    python3 - <<PYEOF
import sys
from pathlib import Path
sys.path.insert(0, str(Path("$REPO_ROOT") / "lake_index"))

import psycopg2
from lake_index.planner import build_row_group_stats

conn = psycopg2.connect("$DSN")
conn.autocommit = False
columns = ["user_id", "amount", "event_ts", "region"]
files = sorted(Path("$DATA_DIR").rglob("*.parquet"))

with conn.cursor() as cur:
    for file_path in files:
        stats = build_row_group_stats(str(file_path), columns=columns)
        if not stats:
            continue
        first = stats[0]
        row_group_count = len({row.row_group_id for row in stats})
        cur.execute(
            """
            INSERT INTO indexed_files (file_path, file_size, file_mtime, row_count, row_group_count, indexed_at)
            VALUES (%s, %s, %s, %s, %s, now())
            ON CONFLICT (file_path) DO UPDATE SET
                file_size = EXCLUDED.file_size,
                file_mtime = EXCLUDED.file_mtime,
                row_count = EXCLUDED.row_count,
                row_group_count = EXCLUDED.row_group_count,
                indexed_at = EXCLUDED.indexed_at
            """,
            (first.file_path, first.file_size, first.file_mtime, first.row_count, row_group_count),
        )
        cur.execute("DELETE FROM row_group_zonemap WHERE file_path = %s", (first.file_path,))
        cur.execute("DELETE FROM parquet_gsi_discovered_files WHERE file_path = %s", (first.file_path,))
        cur.execute(
            """
            INSERT INTO parquet_gsi_discovered_files (file_path, file_size, file_mtime, discovered_at, last_seen_at)
            VALUES (%s, %s, %s, now(), now())
            ON CONFLICT (file_path) DO UPDATE SET
                file_size = EXCLUDED.file_size,
                file_mtime = EXCLUDED.file_mtime,
                last_seen_at = EXCLUDED.last_seen_at
            """,
            (first.file_path, first.file_size, first.file_mtime),
        )
        for row in stats:
            cur.execute(
                """
                INSERT INTO row_group_zonemap
                    (indexed_column, file_path, row_group_id, row_count, null_count,
                     min_value_text, max_value_text, min_value_numeric, max_value_numeric, indexed_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, now())
                ON CONFLICT (indexed_column, file_path, row_group_id) DO UPDATE SET
                    row_count = EXCLUDED.row_count,
                    null_count = EXCLUDED.null_count,
                    min_value_text = EXCLUDED.min_value_text,
                    max_value_text = EXCLUDED.max_value_text,
                    min_value_numeric = EXCLUDED.min_value_numeric,
                    max_value_numeric = EXCLUDED.max_value_numeric,
                    indexed_at = EXCLUDED.indexed_at
                """,
                (
                    row.column_name,
                    row.file_path,
                    row.row_group_id,
                    row.row_count,
                    row.null_count,
                    str(row.min_value) if row.min_value is not None else None,
                    str(row.max_value) if row.max_value is not None else None,
                    float(row.min_value) if isinstance(row.min_value, (int, float)) else None,
                    float(row.max_value) if isinstance(row.max_value, (int, float)) else None,
                ),
            )
conn.commit()
conn.close()
print(f'Indexed {len(files)} parquet file(s).')
PYEOF

fi

# ---------------------------------------------------------------------------
# Step 6: Run benchmark
# ---------------------------------------------------------------------------
echo ""
echo "[6/6] Running benchmark ..."

python3 "$REPO_ROOT/benchmarks/benchmark_e2e.py" \
    --dsn             "$DSN" \
    --data-dir        "$DATA_DIR" \
    --num-files       "$NUM_FILES" \
    --rows-per-file   "$ROWS_PER_FILE" \
    --parquet-row-group-size 128 \
    --target-user-id  "$TARGET_USER_ID" \
    --runs            "$RUNS" \
    --fresh-data \
    --generate \
    --skip-reindex \
    --output          "$REPO_ROOT/benchmarks/parquet_gsi_e2e_summary.md"

echo ""
echo "================================================================"
echo "  Done!  Reports written to:"
echo "    benchmarks/parquet_gsi_e2e_summary.md"
echo ""
echo "  To rerun the benchmark with high-pruning settings:"
echo "    python3 benchmarks/benchmark_e2e.py --dsn \"$DSN\" --data-dir \"$DATA_DIR\" --num-files \"$NUM_FILES\" --rows-per-file \"$ROWS_PER_FILE\" --parquet-row-group-size 128 --target-user-id \"$TARGET_USER_ID\" --runs \"$RUNS\" --fresh-data --generate --output benchmarks/parquet_gsi_e2e_summary.md"
echo ""
echo "  To monitor the background worker:"
echo "    psql \"$DSN\" -c 'SELECT * FROM parquet_gsi_worker_status;'"
echo ""
echo "  To query using the FDW (automatic index pruning):"
echo "    psql \"$DSN\" -c 'SELECT * FROM lake_gsi WHERE user_id = $TARGET_USER_ID;'"
echo "================================================================"
