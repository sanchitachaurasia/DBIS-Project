#!/usr/bin/env python3
"""
Parquet Lake Indexer — General Purpose Daemon
==============================================
Watches a directory of Parquet files, indexes them as they arrive,
and keeps PostgreSQL in sync via index metadata tables.

Runs indefinitely until SIGINT (Ctrl+C) or SIGTERM.

Usage
-----
    # Minimal — watch a directory, no Postgres
    python run.py --data-dir /data/lake

    # With Postgres
    python run.py --data-dir /data/lake \
                  --pg-dsn "host=localhost dbname=mydb user=postgres password=secret"

    # Full options
    python run.py \
        --data-dir     /data/lake \
        --index-db     /data/lake/.index.db \
        --pg-dsn       "host=localhost dbname=mydb user=postgres" \
        --pg-table     row_group_zonemap \
        --pg-schema    public \
        --columns      user_id,amount,event_date \
        --poll         5 \
        --pattern      "*.parquet"

Arguments
---------
--data-dir   DIR        Directory (or tree) containing Parquet files [required]
--index-db   PATH       SQLite index database path
                        [default: <data-dir>/.parquet_index.db]
--pg-dsn     DSN        libpq connection string for Postgres
                        [optional; skip to run index-only mode]
--pg-table   NAME       Postgres row-group zonemap table
                        [default: row_group_zonemap]
--pg-schema  NAME       Postgres schema  [default: public]
--columns    COL,...    Comma-separated columns to index
                        [default: index ALL columns in each file]
--poll       SECONDS    How often to scan for new/changed files [default: 5]
--pattern    GLOB       Filename glob to match  [default: *.parquet]
--verbose               Print a line for every file indexed

Postgres index table schema (created automatically)
----------------------------------------------------
CREATE TABLE indexed_files (
    file_path       TEXT PRIMARY KEY,
    file_size       BIGINT,
    file_mtime      DOUBLE PRECISION,
    row_count       BIGINT,
    row_group_count INTEGER,
    indexed_at      TIMESTAMP WITH TIME ZONE
);

CREATE TABLE row_group_zonemap (
    indexed_column    TEXT,
    file_path         TEXT,
    row_group_id      INTEGER,
    row_count         BIGINT,
    null_count        BIGINT,
    min_value_text    TEXT,
    max_value_text    TEXT,
    min_value_numeric DOUBLE PRECISION,
    max_value_numeric DOUBLE PRECISION,
    indexed_at        TIMESTAMP WITH TIME ZONE,
    PRIMARY KEY (indexed_column, file_path, row_group_id)
);
"""

import argparse
import logging
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

# ---------------------------------------------------------------------------
# Make lake_index importable regardless of cwd
# ---------------------------------------------------------------------------
_PROJECT_ROOT = Path(__file__).resolve().parent
_LAKE_INDEX_ROOT = _PROJECT_ROOT / "lake_index"
if str(_LAKE_INDEX_ROOT) not in sys.path:
    sys.path.insert(0, str(_LAKE_INDEX_ROOT))
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(1, str(_PROJECT_ROOT))

import importlib.util


def _load_lake_index_module(module_name: str, file_name: str):
    package_name = "lake_index"
    package = sys.modules.get(package_name)
    if package is None:
        package_spec = importlib.util.spec_from_loader(package_name, loader=None, is_package=True)
        package = importlib.util.module_from_spec(package_spec)
        package.__path__ = [str(_LAKE_INDEX_ROOT)]
        sys.modules[package_name] = package

    full_name = f"{package_name}.{module_name}"
    module_path = _LAKE_INDEX_ROOT / "lake_index" / file_name
    spec = importlib.util.spec_from_file_location(full_name, module_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[full_name] = module
    spec.loader.exec_module(module)
    return module


_parquet_index = _load_lake_index_module("parquet_index", "parquet_index.py")
_planner = _load_lake_index_module("planner", "planner.py")

IndexStore = _parquet_index.IndexStore
build_file_stats = _planner.build_file_stats
build_row_group_stats = _planner.build_row_group_stats

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("parquet-indexer")


# ---------------------------------------------------------------------------
# Postgres sync
# ---------------------------------------------------------------------------

PG_DDL = """
CREATE TABLE IF NOT EXISTS {schema}.indexed_files (
    file_path       TEXT PRIMARY KEY,
    file_size       BIGINT,
    file_mtime      DOUBLE PRECISION,
    row_count       BIGINT,
    row_group_count INTEGER NOT NULL,
    indexed_at      TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS {schema}.{table} (
    indexed_column    TEXT NOT NULL,
    file_path         TEXT NOT NULL REFERENCES {schema}.indexed_files(file_path) ON DELETE CASCADE,
    row_group_id      INTEGER NOT NULL,
    row_count         BIGINT NOT NULL,
    null_count        BIGINT NOT NULL,
    min_value_text    TEXT,
    max_value_text    TEXT,
    min_value_numeric DOUBLE PRECISION,
    max_value_numeric DOUBLE PRECISION,
    indexed_at        TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (indexed_column, file_path, row_group_id)
);

CREATE INDEX IF NOT EXISTS {table}_column_numeric_bounds
    ON {schema}.{table} (indexed_column, min_value_numeric, max_value_numeric);
CREATE INDEX IF NOT EXISTS {table}_column_text_bounds
    ON {schema}.{table} (indexed_column, min_value_text, max_value_text);
CREATE INDEX IF NOT EXISTS {table}_file_row_group
    ON {schema}.{table} (file_path, row_group_id);
"""

PG_FILE_UPSERT = """
INSERT INTO {schema}.indexed_files
    (file_path, file_size, file_mtime, row_count, row_group_count, indexed_at)
VALUES
    (%(file_path)s, %(file_size)s, %(file_mtime)s,
     %(row_count)s, %(row_group_count)s, %(indexed_at)s)
ON CONFLICT (file_path) DO UPDATE SET
    file_size       = EXCLUDED.file_size,
    file_mtime      = EXCLUDED.file_mtime,
    row_count       = EXCLUDED.row_count,
    row_group_count = EXCLUDED.row_group_count,
    indexed_at      = EXCLUDED.indexed_at;
"""

PG_ROW_GROUP_DELETE = "DELETE FROM {schema}.{table} WHERE file_path = %(file_path)s;"
PG_FILE_DELETE = "DELETE FROM {schema}.indexed_files WHERE file_path = %(file_path)s;"

PG_ROW_GROUP_INSERT = """
INSERT INTO {schema}.{table}
    (indexed_column, file_path, row_group_id, row_count, null_count,
     min_value_text, max_value_text, min_value_numeric, max_value_numeric, indexed_at)
VALUES
    (%(indexed_column)s, %(file_path)s, %(row_group_id)s, %(row_count)s, %(null_count)s,
     %(min_value_text)s, %(max_value_text)s, %(min_value_numeric)s, %(max_value_numeric)s,
     %(indexed_at)s)
ON CONFLICT (indexed_column, file_path, row_group_id) DO UPDATE SET
    row_count         = EXCLUDED.row_count,
    null_count        = EXCLUDED.null_count,
    min_value_text    = EXCLUDED.min_value_text,
    max_value_text    = EXCLUDED.max_value_text,
    min_value_numeric = EXCLUDED.min_value_numeric,
    max_value_numeric = EXCLUDED.max_value_numeric,
    indexed_at        = EXCLUDED.indexed_at;
"""


class PostgresSyncer:
    """Pushes index metadata rows to Postgres tables."""

    def __init__(self, dsn: str, table: str, schema: str):
        import psycopg2
        import psycopg2.extras
        self._psycopg2 = psycopg2
        self._extras = psycopg2.extras
        self.dsn = dsn
        self.table = table
        self.schema = schema
        self._conn = None
        self._connect()
        self._ensure_table()

    def _connect(self):
        self._conn = self._psycopg2.connect(self.dsn)
        self._conn.autocommit = False
        log.info("Connected to Postgres")

    def _ensure_table(self):
        ddl = PG_DDL.format(schema=self.schema, table=self.table)
        with self._conn.cursor() as cur:
            cur.execute(ddl)
        self._conn.commit()
        log.info(
            "Postgres index tables '%s.indexed_files' and '%s.%s' are ready",
            self.schema, self.schema, self.table,
        )

    def _cursor(self):
        # Reconnect if connection dropped
        try:
            self._conn.isolation_level  # cheap liveness check
        except Exception:
            log.warning("Postgres connection lost — reconnecting")
            self._connect()
            self._ensure_table()
        return self._conn.cursor()

    def upsert(self, stats_list: list, row_group_stats_list: list) -> None:
        """Push one file's worth of stats to Postgres."""
        if not stats_list:
            return

        s0 = stats_list[0]
        indexed_at = datetime.now(timezone.utc)
        file_row = {
            "file_path": s0.file_path,
            "file_size": s0.file_size,
            "file_mtime": s0.file_mtime,
            "row_count": s0.row_count,
            "row_group_count": len({row.row_group_id for row in row_group_stats_list}),
            "indexed_at": indexed_at,
        }
        row_group_rows = []
        for row in row_group_stats_list:
            row_group_rows.append(
                {
                    "indexed_column": row.column_name,
                    "file_path": row.file_path,
                    "row_group_id": row.row_group_id,
                    "row_count": row.row_count,
                    "null_count": row.null_count,
                    "min_value_text": _text_or_none(row.min_value),
                    "max_value_text": _text_or_none(row.max_value),
                    "min_value_numeric": _numeric_or_none(row.min_value),
                    "max_value_numeric": _numeric_or_none(row.max_value),
                    "indexed_at": indexed_at,
                }
            )

        try:
            with self._cursor() as cur:
                cur.execute(PG_FILE_UPSERT.format(schema=self.schema), file_row)
                cur.execute(
                    PG_ROW_GROUP_DELETE.format(schema=self.schema, table=self.table),
                    {"file_path": s0.file_path},
                )
                if row_group_rows:
                    self._extras.execute_batch(
                        cur,
                        PG_ROW_GROUP_INSERT.format(schema=self.schema, table=self.table),
                        row_group_rows,
                        page_size=1000,
                    )
            self._conn.commit()
        except Exception as exc:
            self._conn.rollback()
            log.error("Postgres upsert failed for %s: %s", s0.file_path, exc)

    def delete(self, file_path: str) -> None:
        try:
            with self._cursor() as cur:
                cur.execute(
                    PG_ROW_GROUP_DELETE.format(schema=self.schema, table=self.table),
                    {"file_path": file_path},
                )
                cur.execute(PG_FILE_DELETE.format(schema=self.schema), {"file_path": file_path})
            self._conn.commit()
        except Exception as exc:
            self._conn.rollback()
            log.error("Postgres delete failed for %s: %s", file_path, exc)

    def close(self):
        try:
            self._conn.close()
        except Exception:
            pass


def _text_or_none(v):
    if v is None:
        return None
    return str(v)


def _numeric_or_none(v):
    try:
        return float(v) if v is not None else None
    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------------------------
# Core indexer loop
# ---------------------------------------------------------------------------

class IndexerDaemon:
    """
    Polls *data_dir* every *poll_interval* seconds.
    For every new or modified Parquet file found:
      1. Builds column stats + bloom filters (lake_index)
      2. Writes to the local SQLite index
      3. Pushes a metadata row to Postgres (if configured)
    Stops cleanly on SIGINT / SIGTERM.
    """

    def __init__(
        self,
        data_dir: str,
        index_db: str,
        pg_syncer: Optional[PostgresSyncer],
        indexed_columns: Optional[list[str]],
        poll_interval: float,
        pattern: str,
        verbose: bool,
    ):
        self.data_dir = Path(data_dir).resolve()
        self.store = IndexStore(index_db)
        self.pg = pg_syncer
        self.indexed_columns = indexed_columns
        self.poll_interval = poll_interval
        self.pattern = pattern
        self.verbose = verbose
        self._stop = False

        # Totals for the session
        self._session_indexed = 0
        self._session_skipped = 0
        self._session_errors = 0

    # -- signal handling -----------------------------------------------------

    def _handle_signal(self, signum, frame):
        sig_name = signal.Signals(signum).name
        log.info("Received %s — shutting down gracefully …", sig_name)
        self._stop = True

    # -- main loop -----------------------------------------------------------

    def run(self):
        signal.signal(signal.SIGINT,  self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)

        log.info("Parquet Indexer started")
        log.info("  data dir    : %s", self.data_dir)
        log.info("  index db    : %s", self.store.db_path)
        log.info("  poll every  : %ss", self.poll_interval)
        log.info("  file pattern: %s", self.pattern)
        log.info("  columns     : %s", self.indexed_columns or "ALL")
        log.info("  postgres    : %s", "yes" if self.pg else "no (index-only mode)")
        log.info("Press Ctrl+C to stop.\n")

        # Initial pass
        self._scan()

        while not self._stop:
            # Sleep in short increments so SIGINT is handled promptly
            for _ in range(int(self.poll_interval * 10)):
                if self._stop:
                    break
                time.sleep(0.1)
            if not self._stop:
                self._scan()

        self._shutdown()

    # -- scan ----------------------------------------------------------------

    def _scan(self):
        files = sorted(self.data_dir.rglob(self.pattern))
        if not files and self.verbose:
            log.info("No Parquet files found in %s", self.data_dir)
            return

        for fp in files:
            if self._stop:
                break
            self._process_file(fp)

    def _process_file(self, fp: Path):
        try:
            mtime = fp.stat().st_mtime
        except OSError as exc:
            log.warning("Cannot stat %s: %s", fp, exc)
            return

        # Skip if already indexed at this mtime
        if self.store.is_indexed(str(fp), mtime):
            self._session_skipped += 1
            if self.verbose:
                log.debug("SKIP  %s (unchanged)", fp.name)
            return

        # Build stats
        try:
            stats = build_file_stats(str(fp), self.indexed_columns)
            row_group_stats = build_row_group_stats(str(fp), self.indexed_columns)
        except Exception as exc:
            self._session_errors += 1
            log.error("ERROR indexing %s: %s", fp.name, exc)
            return

        # Write local SQLite index
        try:
            self.store.upsert_file(stats)
        except Exception as exc:
            self._session_errors += 1
            log.error("ERROR writing index for %s: %s", fp.name, exc)
            return

        # Push to Postgres
        if self.pg:
            self.pg.upsert(stats, row_group_stats)

        self._session_indexed += 1
        row_count = stats[0].row_count if stats else "?"
        col_count = len(stats)
        log.info(
            "INDEXED  %-50s  rows=%-8s  cols=%d",
            fp.name, f"{row_count:,}" if isinstance(row_count, int) else row_count,
            col_count,
        )

    # -- shutdown ------------------------------------------------------------

    def _shutdown(self):
        if self.pg:
            self.pg.close()
        log.info("")
        log.info("─" * 55)
        log.info("Session summary")
        log.info("  Indexed : %d file(s)", self._session_indexed)
        log.info("  Skipped : %d file(s) (already up-to-date)", self._session_skipped)
        log.info("  Errors  : %d", self._session_errors)
        log.info("─" * 55)
        log.info("Parquet Indexer stopped.")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args():
    p = argparse.ArgumentParser(
        description="Parquet Lake Indexer — watches a directory and indexes Parquet files into SQLite + Postgres",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__.split("Usage")[0].strip(),
    )
    p.add_argument(
        "--data-dir", required=True, metavar="DIR",
        help="Directory containing Parquet files (searched recursively)",
    )
    p.add_argument(
        "--index-db", metavar="PATH", default=None,
        help="SQLite index database file [default: <data-dir>/.parquet_index.db]",
    )
    p.add_argument(
        "--pg-dsn", metavar="DSN", default=None,
        help='Postgres connection string e.g. "host=localhost dbname=mydb user=postgres"',
    )
    p.add_argument(
        "--pg-table", metavar="NAME", default="row_group_zonemap",
        help="Postgres row-group zonemap table [default: row_group_zonemap]",
    )
    p.add_argument(
        "--pg-schema", metavar="NAME", default="public",
        help="Postgres schema [default: public]",
    )
    p.add_argument(
        "--columns", metavar="COL,...", default=None,
        help="Comma-separated list of columns to index [default: all columns]",
    )
    p.add_argument(
        "--poll", metavar="SECONDS", type=float, default=5.0,
        help="Poll interval in seconds [default: 5]",
    )
    p.add_argument(
        "--pattern", metavar="GLOB", default="*.parquet",
        help="Filename glob [default: *.parquet]",
    )
    p.add_argument(
        "--verbose", action="store_true",
        help="Log skipped (unchanged) files too",
    )
    return p.parse_args()


def main():
    args = parse_args()

    data_dir = Path(args.data_dir).resolve()
    if not data_dir.exists():
        log.error("--data-dir does not exist: %s", data_dir)
        sys.exit(1)

    index_db = args.index_db or str(data_dir / ".parquet_index.db")

    indexed_columns = None
    if args.columns:
        indexed_columns = [c.strip() for c in args.columns.split(",") if c.strip()]

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # Postgres (optional)
    pg_syncer = None
    if args.pg_dsn:
        try:
            pg_syncer = PostgresSyncer(
                dsn=args.pg_dsn,
                table=args.pg_table,
                schema=args.pg_schema,
            )
        except Exception as exc:
            log.error("Cannot connect to Postgres: %s", exc)
            log.error("Continuing in index-only mode (no Postgres sync).")

    daemon = IndexerDaemon(
        data_dir=str(data_dir),
        index_db=index_db,
        pg_syncer=pg_syncer,
        indexed_columns=indexed_columns,
        poll_interval=args.poll,
        pattern=args.pattern,
        verbose=args.verbose,
    )
    daemon.run()


if __name__ == "__main__":
    main()
