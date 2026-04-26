#!/usr/bin/env python3
"""
Parquet Lake Indexer — General Purpose Daemon
==============================================
Watches a directory of Parquet files, indexes them as they arrive,
and keeps PostgreSQL in sync via an index metadata table.

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
        --pg-table     parquet_file_index \
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
--pg-table   NAME       Postgres table to write index metadata into
                        [default: parquet_file_index]
--pg-schema  NAME       Postgres schema  [default: public]
--columns    COL,...    Comma-separated columns to index
                        [default: index ALL columns in each file]
--poll       SECONDS    How often to scan for new/changed files [default: 5]
--pattern    GLOB       Filename glob to match  [default: *.parquet]
--verbose               Print a line for every file indexed

Postgres index table schema (created automatically)
----------------------------------------------------
CREATE TABLE parquet_file_index (
    file_path    TEXT PRIMARY KEY,
    file_size    BIGINT,
    file_mtime   DOUBLE PRECISION,
    row_count    BIGINT,
    indexed_at   TIMESTAMP WITH TIME ZONE,
    columns_json JSONB       -- {col: {min, max, null_count}} per column
);

With this table in Postgres you can:
  • List all known files:           SELECT file_path FROM parquet_file_index;
  • Find files covering a value:    SELECT file_path FROM parquet_file_index
                                    WHERE (columns_json->'user_id'->>'min')::int <= 42
                                      AND (columns_json->'user_id'->>'max')::int >= 42;
  • Drive parquet_fdw dynamically via a function that reads this table.
"""

import argparse
import json
import logging
import os
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
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

import pyarrow.parquet as pq

from lake_index.parquet_index import IndexStore
from lake_index.planner import build_file_stats, DataLakeIndex

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
CREATE TABLE IF NOT EXISTS {schema}.{table} (
    file_path    TEXT PRIMARY KEY,
    file_size    BIGINT,
    file_mtime   DOUBLE PRECISION,
    row_count    BIGINT,
    indexed_at   TIMESTAMPTZ,
    columns_json JSONB
);
CREATE INDEX IF NOT EXISTS {table}_indexed_at
    ON {schema}.{table} (indexed_at);
"""

PG_UPSERT = """
INSERT INTO {schema}.{table}
    (file_path, file_size, file_mtime, row_count, indexed_at, columns_json)
VALUES
    (%(file_path)s, %(file_size)s, %(file_mtime)s,
     %(row_count)s, %(indexed_at)s, %(columns_json)s)
ON CONFLICT (file_path) DO UPDATE SET
    file_size    = EXCLUDED.file_size,
    file_mtime   = EXCLUDED.file_mtime,
    row_count    = EXCLUDED.row_count,
    indexed_at   = EXCLUDED.indexed_at,
    columns_json = EXCLUDED.columns_json;
"""

PG_DELETE = "DELETE FROM {schema}.{table} WHERE file_path = %(file_path)s;"


class PostgresSyncer:
    """Pushes index metadata rows to a Postgres table."""

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
        log.info("Postgres index table '%s.%s' is ready", self.schema, self.table)

    def _cursor(self):
        # Reconnect if connection dropped
        try:
            self._conn.isolation_level  # cheap liveness check
        except Exception:
            log.warning("Postgres connection lost — reconnecting")
            self._connect()
            self._ensure_table()
        return self._conn.cursor()

    def upsert(self, stats_list: list) -> None:
        """Push one file's worth of stats to Postgres."""
        if not stats_list:
            return

        s0 = stats_list[0]
        # Build columns_json: {col_name: {min, max, null_count}}
        cols_dict = {}
        for s in stats_list:
            cols_dict[s.column_name] = {
                "min": _json_safe(s.min_value),
                "max": _json_safe(s.max_value),
                "null_count": s.null_count,
            }

        row = {
            "file_path":    s0.file_path,
            "file_size":    s0.file_size,
            "file_mtime":   s0.file_mtime,
            "row_count":    s0.row_count,
            "indexed_at":   datetime.now(timezone.utc),
            "columns_json": json.dumps(cols_dict),
        }
        sql = PG_UPSERT.format(schema=self.schema, table=self.table)
        try:
            with self._cursor() as cur:
                cur.execute(sql, row)
            self._conn.commit()
        except Exception as exc:
            self._conn.rollback()
            log.error("Postgres upsert failed for %s: %s", s0.file_path, exc)

    def delete(self, file_path: str) -> None:
        sql = PG_DELETE.format(schema=self.schema, table=self.table)
        try:
            with self._cursor() as cur:
                cur.execute(sql, {"file_path": file_path})
            self._conn.commit()
        except Exception as exc:
            self._conn.rollback()
            log.error("Postgres delete failed for %s: %s", file_path, exc)

    def close(self):
        try:
            self._conn.close()
        except Exception:
            pass


def _json_safe(v):
    """Convert Arrow/Python value to something JSON-serialisable."""
    if v is None:
        return None
    if isinstance(v, (int, float, str, bool)):
        return v
    return str(v)


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
            self.pg.upsert(stats)

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
        "--pg-table", metavar="NAME", default="parquet_file_index",
        help="Postgres table name for index metadata [default: parquet_file_index]",
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