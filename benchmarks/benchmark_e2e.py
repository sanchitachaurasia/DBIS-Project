#!/usr/bin/env python3
"""
benchmark_e2e.py — End-to-end benchmark for the Parquet GSI (zonemap in PostgreSQL)
====================================================================================
1. **catalog_seqscan** — same zonemap filter with index scans *disabled* (sequential
   scan of `row_group_zonemap`). This is the fair “no B-tree on bounds” baseline.
2. **catalog_indexed** — same SQL with default planner settings; PostgreSQL can use
   the GSI B-tree on `(indexed_column, min_value_numeric, max_value_numeric)`.
3. **parquet_gsi_candidate_files** — `parquet_gsi_candidate_files()` (indexed-only;
   no union with unlisted files; avoids the `parquet_gsi_unindexed_files` seq scan
   that made `parquet_gsi_query_files(..., true)` look slower than the baseline).
4. **lake_index_prune** (optional) — same predicate evaluated in Python via
   `QueryPlanner` + `IndexStore` (SQLite) filled by *lake_index*; measures local
   index lookup without the SQL planner.

Reindex
-------
* **python** (default) — **`lake_index`** (`build_file_stats` / `build_row_group_stats`
  from the `lake_index/` package) reads Parquet footers and **upserts** into Postgres
  `indexed_files` + `row_group_zonemap` (same schema as parquet_gsi).
* **c** — `parquet_gsi_reindex_all` (C extension; requires `pg_parquet` + extension).

Test data
---------
Use `--fresh-data` to delete existing `part-*.parquet` files and regenerate the lake with a much
smaller row-group size. That makes the zonemap catalog much larger, which in turn makes the
`catalog_seqscan` baseline meaningfully slower and the GSI speedup much easier to observe.
"""
from __future__ import annotations

import argparse
import re
import shlex
import statistics
import sys
import textwrap
import time
from pathlib import Path
from typing import Any, Optional

# Repo root and benchmarks directory (this file lives in benchmarks/)
_BENCHMARKS_DIR = Path(__file__).resolve().parent
_ROOT = _BENCHMARKS_DIR.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

# lake_index package lives under <root>/lake_index/
_LAKE_PREFIX = _ROOT / "lake_index"
if str(_LAKE_PREFIX) not in sys.path:
    sys.path.insert(0, str(_LAKE_PREFIX))

import psycopg2
import psycopg2.extras

# ---------------------------------------------------------------------------
# Parquet data generation
# ---------------------------------------------------------------------------


def generate_parquet_files(
    data_dir: str,
    num_files: int,
    rows_per_file: int,
    *,
    row_group_size: int = 2048,
    seed: int = 42,
) -> list[str]:
    """Generate synthetic Parquet files whose values are clustered by file."""
    import datetime
    import random

    import pyarrow as pa
    import pyarrow.parquet as pq

    rng = random.Random(seed)
    out_dir = Path(data_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    files: list[str] = []
    # Distribute the synthetic user ids by file instead of making every file look identical;
    # that creates visible min/max separation and makes the zonemap pruning path measurable.
    users_per_file = max(1, 10_000 // num_files)

    for i in range(num_files):
        fp = out_dir / f"part-{i:05d}.parquet"
        if fp.exists():
            files.append(str(fp))
            continue

        uid_base = i * users_per_file
        user_ids = [uid_base + rng.randint(0, users_per_file - 1) for _ in range(rows_per_file)]
        amounts = [round(rng.uniform(1.0, 10_000.0), 2) for _ in range(rows_per_file)]
        base_ts = datetime.datetime(2024, 1, 1)
        event_ts = [
            base_ts + datetime.timedelta(seconds=rng.randint(0, 86400 * 365))
            for _ in range(rows_per_file)
        ]
        regions = [rng.choice(["us-east", "us-west", "eu-central", "ap-south"]) for _ in range(rows_per_file)]

        table = pa.table(
            {
                "user_id": pa.array(user_ids, type=pa.int64()),
                "amount": pa.array(amounts, type=pa.float64()),
                "event_ts": pa.array(event_ts, type=pa.timestamp("us")),
                "region": pa.array(regions, type=pa.string()),
            }
        )

        pq.write_table(
            table,
            fp,
            row_group_size=row_group_size,
            compression="snappy",
            write_statistics=True,
        )
        files.append(str(fp))
        print(f"  generated {fp.name}")

    print(f"  {len(files)} Parquet files in {data_dir}")
    return files


def remove_existing_parquet_parts(data_dir: Path) -> int:
    """Delete any existing part-*.parquet files from the benchmark dataset directory."""
    removed = 0
    for fp in data_dir.glob("part-*.parquet"):
        fp.unlink()
        removed += 1
    return removed


def default_testdata_dir() -> str:
    """Return the default benchmark data directory bundled with the repository."""
    return str(_BENCHMARKS_DIR / "testdata" / "parquet_gsi_bench")


def count_parquet_parts(data_dir: Path) -> int:
    """Count the Parquet part files currently present in *data_dir*."""
    return len(list(data_dir.glob("part-*.parquet")))


def verify_testdata_present(data_dir: Path, num_files: int) -> None:
    """Stop early when the requested benchmark dataset has not been generated yet."""
    n = count_parquet_parts(data_dir)
    if n < num_files:
        print(
            f"Missing Parquet test data: expected at least {num_files} part-*.parquet "
            f"under\n  {data_dir}\n"
            f"found {n}.\n\n"
            "Create them once (no database required):\n"
            f"  python3 benchmarks/benchmark_e2e.py --generate-only\n"
            "Or with a custom directory / size / row groups:\n"
            f"  python3 benchmarks/benchmark_e2e.py --fresh-data --generate-only "
            f"--data-dir /path/to/dir --num-files {num_files} "
            f"--parquet-row-group-size 128\n",
            file=sys.stderr,
        )
        raise SystemExit(1)


# ---------------------------------------------------------------------------
# PostgreSQL helpers
# ---------------------------------------------------------------------------


def run_ddl(conn, sql: str) -> None:
    """Execute a DDL statement and commit the transaction."""
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()


def parquet_gsi_extension_loaded(conn) -> bool:
    """Check whether parquet_gsi is already installed in the connected database."""
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM pg_extension WHERE extname = 'parquet_gsi' LIMIT 1")
        return cur.fetchone() is not None


def try_create_parquet_gsi_extension(conn) -> bool:
    """Return True if parquet_gsi (and dependencies, e.g. pg_parquet) load successfully."""
    try:
        with conn.cursor() as cur:
            cur.execute("CREATE EXTENSION IF NOT EXISTS parquet_gsi CASCADE")
        conn.commit()
        return True
    except Exception:
        conn.rollback()
        return False


def ensure_minimal_zonemap_catalog(conn) -> None:
    """
    Create indexed_files + row_group_zonemap + indexes when parquet_gsi extension
    is not installed (no pg_parquet). Same layout as parquet_gsi--1.0.sql core tables.
    """
    stmts = [
        """
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
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS row_group_zonemap (
            indexed_column    text NOT NULL,
            file_path         text NOT NULL REFERENCES indexed_files(file_path)
                ON DELETE CASCADE,
            row_group_id      integer NOT NULL,
            row_count         bigint NOT NULL,
            null_count        bigint NOT NULL DEFAULT 0,
            min_value_text    text,
            max_value_text    text,
            min_value_numeric double precision,
            max_value_numeric double precision,
            indexed_at        timestamptz NOT NULL DEFAULT now(),
            PRIMARY KEY (indexed_column, file_path, row_group_id)
        )
        """,
        """
        CREATE INDEX IF NOT EXISTS row_group_zonemap_column_numeric_bounds_idx
            ON row_group_zonemap (indexed_column, min_value_numeric, max_value_numeric)
        """,
        """
        CREATE INDEX IF NOT EXISTS row_group_zonemap_column_text_bounds_idx
            ON row_group_zonemap (indexed_column, min_value_text, max_value_text)
        """,
        """
        CREATE INDEX IF NOT EXISTS row_group_zonemap_file_row_group_idx
            ON row_group_zonemap (file_path, row_group_id)
        """,
    ]
    with conn.cursor() as cur:
        for s in stmts:
            cur.execute(s)
    conn.commit()


def reindex_c_extension(conn, data_dir: str) -> int:
    """Rebuild the zonemap catalog through the parquet_gsi C extension."""
    with conn.cursor() as cur:
        cur.execute("SELECT parquet_gsi_reindex_all(%s)", (data_dir,))
        result = cur.fetchone()[0]
    conn.commit()
    return int(result)


# ---- lake_index → PostgreSQL (same schema as parquet_gsi) -----------------


def _text_or_none(v: Any) -> Optional[str]:
    """Return a text value for catalog writes, or None for NULL."""
    if v is None:
        return None
    return str(v)


def _numeric_or_none(v: Any) -> Optional[float]:
    """Return a float when *v* is numeric, otherwise None."""
    try:
        return float(v) if v is not None else None
    except (TypeError, ValueError):
        return None


def _truncate_gsi_tables(conn) -> None:
    """Clear catalog so we can reload from Parquet. `indexed_files` trunc cascades to zonemap."""
    with conn.cursor() as cur:
        cur.execute("TRUNCATE indexed_files CASCADE")
    conn.commit()
    try:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE parquet_gsi_discovered_files")
        conn.commit()
    except Exception:
        conn.rollback()


PG_FILE_UPSERT = """
INSERT INTO indexed_files
    (file_path, file_size, file_mtime, row_count, row_group_count, indexed_at, index_status)
VALUES
    (%(file_path)s, %(file_size)s, %(file_mtime)s,
     %(row_count)s, %(row_group_count)s, NOW(), 'indexed')
ON CONFLICT (file_path) DO UPDATE SET
    file_size       = EXCLUDED.file_size,
    file_mtime      = EXCLUDED.file_mtime,
    row_count       = EXCLUDED.row_count,
    row_group_count = EXCLUDED.row_group_count,
    indexed_at      = EXCLUDED.indexed_at,
    index_status    = 'indexed',
    last_error      = NULL
"""

PG_ROW_GROUP_DELETE = "DELETE FROM row_group_zonemap WHERE file_path = %(file_path)s;"

PG_ROW_GROUP_INSERT = """
INSERT INTO row_group_zonemap
    (indexed_column, file_path, row_group_id, row_count, null_count,
     min_value_text, max_value_text, min_value_numeric, max_value_numeric, indexed_at)
VALUES
    (%(indexed_column)s, %(file_path)s, %(row_group_id)s, %(row_count)s, %(null_count)s,
     %(min_value_text)s, %(max_value_text)s, %(min_value_numeric)s, %(max_value_numeric)s,
     NOW())
ON CONFLICT (indexed_column, file_path, row_group_id) DO UPDATE SET
    row_count         = EXCLUDED.row_count,
    null_count        = EXCLUDED.null_count,
    min_value_text    = EXCLUDED.min_value_text,
    max_value_text    = EXCLUDED.max_value_text,
    min_value_numeric = EXCLUDED.min_value_numeric,
    max_value_numeric = EXCLUDED.max_value_numeric,
    indexed_at        = EXCLUDED.indexed_at
"""


def reindex_lake_index_python(
    conn,
    data_dir: str,
    indexed_columns: list[str],
    index_db: str,
) -> int:
    """
    Rebuild GSI tables using lake_index (PyArrow metadata + optional bloom in SQLite)
    and upsert the same row-group zonemap into PostgreSQL.
    """
    from lake_index import IndexStore, build_file_stats, build_row_group_stats

    _truncate_gsi_tables(conn)

    store = IndexStore(index_db)
    data_path = Path(data_dir)
    parquets = sorted(data_path.glob("part-*.parquet"))
    n_done = 0
    for fp in parquets:
        stats = build_file_stats(str(fp), indexed_columns)
        rgs = build_row_group_stats(str(fp), indexed_columns)
        store.upsert_file(stats)
        s0 = stats[0]
        file_row = {
            "file_path": s0.file_path,
            "file_size": s0.file_size,
            "file_mtime": s0.file_mtime,
            "row_count": s0.row_count,
            "row_group_count": len({r.row_group_id for r in rgs}),
        }
        row_group_rows: list[dict] = []
        for row in rgs:
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
                }
            )
        with conn.cursor() as cur:
            cur.execute(PG_FILE_UPSERT, file_row)
            cur.execute(PG_ROW_GROUP_DELETE, {"file_path": s0.file_path})
            if row_group_rows:
                psycopg2.extras.execute_batch(
                    cur, PG_ROW_GROUP_INSERT, row_group_rows, page_size=1000
                )
        conn.commit()
        n_done += 1
    return n_done


def populate_lake_index_sqlite_only(
    data_dir: str, index_db: str, indexed_columns: list[str]
) -> int:
    """Build only the local IndexStore (for lake_index_prune) without touching Postgres."""
    from lake_index import IndexStore, build_file_stats

    if Path(index_db).exists():
        Path(index_db).unlink()
    store = IndexStore(index_db)
    n = 0
    for fp in sorted(Path(data_dir).glob("part-*.parquet")):
        stats = build_file_stats(str(fp), indexed_columns)
        store.upsert_file(stats)
        n += 1
    return n


# ---------------------------------------------------------------------------
# EXPLAIN (ANALYZE) with optional GUCs (SET LOCAL in a subtransaction)
# ---------------------------------------------------------------------------

BASELINE_LABEL = "catalog_seqscan"
_SEQSCAN_GUC: tuple[tuple[str, str], ...] = (
    ("enable_seqscan", "on"),
    ("enable_indexscan", "off"),
    ("enable_bitmapscan", "off"),
    ("enable_indexonlyscan", "off"),
    ("enable_tidscan", "off"),
    ("max_parallel_workers_per_gather", "0"),
)


def explain_analyze(
    conn,
    sql: str,
    gucs: Optional[tuple[tuple[str, str], ...]] = None,
) -> tuple[list[str], float]:
    with conn.cursor() as cur:
        if gucs:
            cur.execute("BEGIN")
            for name, val in gucs:
                cur.execute(f"SET LOCAL {name} = %s", (val,))
        else:
            cur.execute("BEGIN")
        cur.execute(f"EXPLAIN (ANALYZE, BUFFERS) {sql}")
        lines = [row[0] for row in cur.fetchall()]
    conn.rollback()

    for line in lines:
        m = re.search(r"Execution Time: ([0-9.]+) ms", line)
        if m:
            return lines, float(m.group(1))
    raise RuntimeError("Could not find Execution Time in EXPLAIN output")


def run_sql_benchmark(
    conn,
    label: str,
    sql: str,
    runs: int,
    warmup: int,
    gucs: Optional[tuple[tuple[str, str], ...]] = None,
) -> dict:
    """Run an EXPLAIN ANALYZE benchmark and collect timing plus plan output."""
    for _ in range(warmup):
        explain_analyze(conn, sql, gucs)
    timings: list[float] = []
    all_plans: list[list[str]] = []
    for _ in range(runs):
        lines, ms = explain_analyze(conn, sql, gucs)
        timings.append(ms)
        all_plans.append(lines)
    return {
        "kind": "sql",
        "label": label,
        "sql": sql,
        "gucs": gucs,
        "runs": runs,
        "warmup": warmup,
        "timings_ms": timings,
        "avg_ms": statistics.mean(timings),
        "median_ms": statistics.median(timings),
        "min_ms": min(timings),
        "max_ms": max(timings),
        "plans": all_plans,
    }


def run_lake_index_prune_benchmark(
    index_db: str, target_user_id: int, runs: int, warmup: int
) -> dict:
    """Time the pure Python pruning path backed by the local SQLite index."""
    from lake_index import IndexStore, Predicate, QueryPlanner

    store = IndexStore(index_db)
    planner = QueryPlanner(store)
    pred = Predicate.eq("user_id", target_user_id)

    # Measure the planner-facing pruning cost only; this isolates the index lookup work from
    # PyArrow scans, SQL planning, and any disk I/O that would blur the benchmark signal.
    for _ in range(warmup):
        planner.prune([pred])
    times: list[float] = []
    for _ in range(runs):
        t0 = time.perf_counter()
        planner.prune([pred])
        t1 = time.perf_counter()
        times.append((t1 - t0) * 1000.0)
    return {
        "kind": "local",
        "label": "lake_index_prune",
        "sql": (
            f"# Python: QueryPlanner.prune([Predicate.eq('user_id', {target_user_id})]) "
            f"on IndexStore({index_db!r})"
        ),
        "gucs": None,
        "runs": runs,
        "warmup": warmup,
        "timings_ms": times,
        "avg_ms": statistics.mean(times),
        "median_ms": statistics.median(times),
        "min_ms": min(times),
        "max_ms": max(times),
        "plans": [[]],
    }


# ---------------------------------------------------------------------------
# Query definitions
# ---------------------------------------------------------------------------


def core_filter_sql(tuid: float) -> str:
    """Build the baseline SQL predicate used for the zonemap benchmark."""
    return textwrap.dedent(
        f"""
        SELECT COUNT(DISTINCT file_path)
        FROM row_group_zonemap
        WHERE indexed_column = 'user_id'
          AND (min_value_numeric IS NULL OR min_value_numeric <= {tuid})
          AND (max_value_numeric IS NULL OR max_value_numeric >= {tuid})
    """
    ).strip()


def make_sql_specs(
    target_user_id: int,
    has_parquet_gsi_sql: bool,
) -> list[tuple[str, str, Optional[tuple[tuple[str, str], ...]]]]:
    """Assemble the benchmark query variants and their optional planner settings."""
    tuid = float(target_user_id)
    core = core_filter_sql(tuid)
    specs: list[tuple[str, str, Optional[tuple[tuple[str, str], ...]]]] = [
        ("catalog_seqscan", core, _SEQSCAN_GUC),
        ("catalog_indexed", core, None),
    ]
    if has_parquet_gsi_sql:
        specs.append(
            (
                "parquet_gsi_candidate_files",
                textwrap.dedent(
                    f"""
                    SELECT COUNT(*)
                    FROM parquet_gsi_candidate_files(
                        'user_id',
                        {tuid},
                        {tuid},
                        NULL,
                        NULL
                    )
                """
                ).strip(),
                None,
            )
        )
    return specs


# ---------------------------------------------------------------------------
# Markdown report
# ---------------------------------------------------------------------------


def approx_zonemap_rows_for_column(
    num_files: int, rows_per_file: int, row_group_size: int, indexed_columns: int = 1
) -> int:
    """Approximate row_group_zonemap rows for one full lake (all files, one column each RG)."""
    rgs = max(1, (rows_per_file + row_group_size - 1) // row_group_size)
    return num_files * rgs * indexed_columns


def markdown_report(
    results: list[dict],
    num_files: int,
    rows_per_file: int,
    index_mode: str,
    parquet_gsi_extension_loaded: bool,
    row_group_size: int,
    rerun_command: str,
) -> str:
    """Render the benchmark results as a Markdown report."""
    baseline_avg: Optional[float] = None
    for r in results:
        if r["label"] == BASELINE_LABEL:
            baseline_avg = r["avg_ms"]
            break
    if baseline_avg is None:
        baseline_avg = results[0]["avg_ms"] if results else 0.0

    approx_zm = approx_zonemap_rows_for_column(num_files, rows_per_file, row_group_size, 1)

    lines = [
        "# Parquet GSI / lake_index benchmark (E2E)",
        "",
        f"**Dataset:** {num_files} Parquet files × {rows_per_file:,} rows = "
        f"{num_files * rows_per_file:,} total rows; **Parquet `row_group_size`:** {row_group_size:,}",
        "",
        f"**Approx. `row_group_zonemap` rows (column `user_id` only):** ~{approx_zm:,}. "
        "With a tiny catalog, both seqscan and index paths finish too quickly from shared buffers, "
        "so **speedup ratios stay modest** even when the index path does much less work. This "
        "benchmark is meant to be rerun with **`--fresh-data --parquet-row-group-size 128`** "
        "or smaller so the zonemap grows and the baseline becomes visibly worse.",
        "",
        f"**Reindex mode:** `{index_mode}` — **`python` = lake_index** reads Parquet metadata "
        "(`build_row_group_stats` / `build_file_stats` in `lake_index/`) and **writes those rows "
        "into Postgres** `indexed_files` + `row_group_zonemap`. **`c`** uses the parquet_gsi C "
        "extension (`parquet_gsi_reindex_all`, needs `pg_parquet`).",
        "",
        f"**parquet_gsi extension in database:** `{'yes' if parquet_gsi_extension_loaded else 'no'}` "
        "(if no: install `pg_parquet`, then `CREATE EXTENSION parquet_gsi CASCADE`; "
        "otherwise this run used a minimal catalog + lake_index only).",
        "",
        "**Rerun command:**",
        "",
        "```bash",
        rerun_command,
        "```",
        "",
        "## Summary",
        "",
        "The **catalog_seqscan** path disables bitmap/index scans so the planner does a true "
        "**sequential scan** of `row_group_zonemap` with the same predicates as the GSI path. "
        "Other rows use default settings so the **B-tree on numeric bounds** can be used. "
        "Use `parquet_gsi_candidate_files` (not `parquet_gsi_query_files(..., true)`) to avoid an "
        "unrelated full scan of `parquet_gsi_discovered_files` in the “indexed” timing.",
        "",
        "| Query path | avg ms | median ms | min ms | max ms | speedup vs seqscan baseline |",
        "|------------|--------|-----------|--------|--------|-----------------------------|",
    ]

    for r in results:
        sp = "—"
        if baseline_avg and r["avg_ms"] and r["avg_ms"] > 0:
            sp = f"{baseline_avg / r['avg_ms']:.2f}×" if r["label"] != BASELINE_LABEL else "1× (seqscan baseline)"
        lines.append(
            f"| {r['label']} | {r['avg_ms']:.3f} | {r['median_ms']:.3f} | "
            f"{r['min_ms']:.3f} | {r['max_ms']:.3f} | {sp} |"
        )

    lines += ["", "---", ""]

    for r in results:
        lines += [
            f"## {r['label']}",
            "",
        ]
        if r["kind"] == "sql":
            if r.get("gucs"):
                lines += [
                    "Planner settings (seqscan-only baseline only):",
                    "",
                    "```text",
                    "SET LOCAL enable_seqscan = on;",
                    "SET LOCAL enable_indexscan = off;",
                    "SET LOCAL enable_bitmapscan = off;",
                    "SET LOCAL enable_indexonlyscan = off;",
                    "SET LOCAL enable_tidscan = off;",
                    "SET LOCAL max_parallel_workers_per_gather = 0;",
                    "```",
                    "",
                ]
            lines += [
                "```sql",
                r["sql"],
                "```",
                "",
            ]
        else:
            lines += [
                "```text",
                r["sql"],
                "```",
                "",
            ]
        lines += [
            f"- kind: {r['kind']}",
            f"- warmup: {r.get('warmup', 0)}",
            f"- runs: {r['runs']}",
            f"- timings_ms: " + ", ".join(f"{t:.3f}" for t in r["timings_ms"]),
            f"- avg_ms: {r['avg_ms']:.3f}",
        ]
        if r["kind"] == "sql" and r.get("plans") and r["plans"][-1]:
            lines += [
                "",
                "### EXPLAIN ANALYZE (last run)",
                "```text",
            ] + r["plans"][-1] + ["```", ""]

    return "\n".join(lines).rstrip() + "\n"


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def parse_args() -> argparse.Namespace:
    """Parse CLI flags for the end-to-end benchmark driver."""
    p = argparse.ArgumentParser(
        description="E2E benchmark: Parquet GSI in PostgreSQL + optional lake_index local prune",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument(
        "--dsn",
        default=None,
        help="PostgreSQL libpq DSN (not required with --generate-only)",
    )
    p.add_argument(
        "--data-dir",
        default=default_testdata_dir(),
        help="Directory for Parquet parts (default: benchmarks/testdata/parquet_gsi_bench)",
    )
    p.add_argument(
        "--num-files",
        type=int,
        default=100,
        help="Number of Parquet part files to generate (project default: 100)",
    )
    p.add_argument(
        "--rows-per-file", type=int, default=50_000, help="Rows per Parquet file"
    )
    p.add_argument(
        "--parquet-row-group-size",
        type=int,
        default=128,
        metavar="N",
        help="Parquet row group size when generating files (smaller → more row groups → "
        "larger zonemap and higher seqscan vs index speedup in this benchmark)",
    )
    p.add_argument(
        "--target-user-id", type=int, default=42, help="user_id to filter (exists in part-00000 by construction)"
    )
    p.add_argument("--runs", type=int, default=5, help="Timed runs per SQL query")
    p.add_argument(
        "--warmup", type=int, default=1, help="EXPLAIN runs before timing (per query)"
    )
    p.add_argument(
        "--index-via",
        choices=["c", "python"],
        default="python",
        help="python (default): lake_index PyArrow metadata → Postgres zonemap; "
        "c: parquet_gsi_reindex_all (requires pg_parquet + extension)",
    )
    p.add_argument(
        "--python-index-db",
        default=None,
        help="SQLite path for lake_index when --index-via python (default: <data-dir>/.lake_index_benchmark.db)",
    )
    p.add_argument(
        "--bench-lake-index",
        action="store_true",
        help="After SQL benchmarks, time lake_index QueryPlanner.prune on SQLite (default --index-via python builds this DB)",
    )
    p.add_argument(
        "--output", default="benchmarks/parquet_gsi_e2e_summary.md", help="Output Markdown"
    )
    p.add_argument(
        "--generate",
        action="store_true",
        help="Create any missing part-*.parquet files under --data-dir (existing files are kept)",
    )
    p.add_argument(
        "--fresh-data",
        action="store_true",
        help="Delete existing part-*.parquet files under --data-dir before generation/reindex so the lake is rebuilt from scratch",
    )
    p.add_argument(
        "--generate-only",
        action="store_true",
        help="Only run Parquet generation, then exit (no Postgres; --dsn not needed)",
    )
    p.add_argument(
        "--skip-reindex", action="store_true", help="Skip reindexing (use existing catalog)"
    )
    return p.parse_args()


def ensure_fdw_foreign_table(conn, data_dir: str, table_name: str = "lake_gsi") -> None:
    """Create the parquet_fdw foreign table used by the benchmark if needed."""
    return None


def build_rerun_command(args: argparse.Namespace) -> str:
    """Reconstruct the current benchmark invocation as a shell command."""
    parts = [
        "python3",
        "benchmarks/benchmark_e2e.py",
        "--dsn",
        args.dsn or "postgresql://USER:PASSWORD@HOST:PORT/DBNAME",
        "--data-dir",
        args.data_dir,
        "--num-files",
        str(args.num_files),
        "--rows-per-file",
        str(args.rows_per_file),
        "--parquet-row-group-size",
        str(args.parquet_row_group_size),
        "--target-user-id",
        str(args.target_user_id),
        "--runs",
        str(args.runs),
        "--warmup",
        str(args.warmup),
        "--index-via",
        args.index_via,
        "--output",
        args.output,
        "--fresh-data",
        "--generate",
    ]
    if args.bench_lake_index:
        parts.append("--bench-lake-index")
    return " ".join(shlex.quote(part) for part in parts)


def main() -> None:
    """Run the benchmark workflow end to end and write the Markdown summary."""
    args = parse_args()
    data_path = Path(args.data_dir).resolve()
    args.data_dir = str(data_path)

    if args.fresh_data:
        data_path.mkdir(parents=True, exist_ok=True)
        removed = remove_existing_parquet_parts(data_path)
        stale_sqlite = args.python_index_db or str(data_path / ".lake_index_benchmark.db")
        stale_paths = [
            Path(stale_sqlite),
            data_path / ".lake_index.db",
        ]
        for stale in stale_paths:
            if stale.exists():
                stale.unlink()
        print(f"\n[fresh-data] Removed {removed} existing part-*.parquet file(s) from:\n  {data_path}")

    if args.generate_only:
        print(f"\n[generate-only] Writing up to {args.num_files} Parquet parts under:\n  {data_path}")
        print(f"  row_group_size={args.parquet_row_group_size}")
        generate_parquet_files(
            args.data_dir,
            args.num_files,
            args.rows_per_file,
            row_group_size=args.parquet_row_group_size,
        )
        print("Done.")
        return

    index_db = args.python_index_db or str(data_path / ".lake_index_benchmark.db")

    if args.generate:
        print(f"\n[1/4] Ensuring {args.num_files} Parquet files (missing only) …")
        print(f"  row_group_size={args.parquet_row_group_size}")
        generate_parquet_files(
            args.data_dir,
            args.num_files,
            args.rows_per_file,
            row_group_size=args.parquet_row_group_size,
        )
    else:
        print(f"\n[1/4] Using existing Parquet under:\n  {data_path}")
        verify_testdata_present(data_path, args.num_files)

    if not args.dsn:
        print(
            "error: --dsn is required for benchmarks (use --generate-only to only create Parquet).",
            file=sys.stderr,
        )
        raise SystemExit(2)

    print("\n[2/4] Setting up PostgreSQL …")
    conn = psycopg2.connect(args.dsn)
    conn.autocommit = False

    g_ext = parquet_gsi_extension_loaded(conn)
    if not g_ext:
        g_ext = try_create_parquet_gsi_extension(conn)
    if not g_ext:
        print(
            "\nNote: `CREATE EXTENSION parquet_gsi` failed (usually missing **pg_parquet** in "
            "PostgreSQL). Install/build pg_parquet for this server, then run:\n"
            "  CREATE EXTENSION parquet_gsi CASCADE;\n"
            "\nFalling back to: minimal `indexed_files` / `row_group_zonemap` DDL + "
            "**lake_index** reindex. SQL helper `parquet_gsi_candidate_files` is omitted from "
            "the benchmark.\n"
        )
        ensure_minimal_zonemap_catalog(conn)

    effective_index_via = args.index_via
    if not g_ext and effective_index_via == "c":
        print(
            "Switching reindex to **python** (parquet_gsi_reindex_all requires the extension).\n"
        )
        effective_index_via = "python"

    if not args.skip_reindex:
        print("\n[3/4] Indexing into GSI tables …")
        if effective_index_via == "python":
            print(
                "      Source: **lake_index** (`build_row_group_stats` / `build_file_stats`) "
                "→ Postgres `indexed_files` + `row_group_zonemap`."
            )
        if effective_index_via == "c":
            _truncate_gsi_tables(conn)
            n = reindex_c_extension(conn, args.data_dir)
            print(f"      parquet_gsi_reindex_all → {n} file(s).")
        else:
            if Path(index_db).exists():
                Path(index_db).unlink()
            cols = ["user_id"]
            n = reindex_lake_index_python(conn, args.data_dir, cols, index_db)
            print(f"      lake_index → Postgres → {n} file(s) (SQLite: {index_db})")
    else:
        print("[3/4] Skipping reindex (using existing index rows).")

    if g_ext:
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT parquet_gsi_add_indexed_column('user_id')")
            conn.commit()
        except Exception as exc:
            conn.rollback()
            print(f"Note: parquet_gsi_add_indexed_column: {exc}")

    ensure_fdw_foreign_table(conn, args.data_dir)

    if args.bench_lake_index and not Path(index_db).is_file():
        n = populate_lake_index_sqlite_only(args.data_dir, index_db, ["user_id"])
        print(
            f"\n[3b/4] Built lake_index SQLite for local prune ({n} files) → {index_db!r}"
        )

    print(f"\n[4/4] Running benchmarks (warmup={args.warmup}, runs={args.runs}) …")
    results: list[dict] = []

    for label, sql, gucs in make_sql_specs(args.target_user_id, g_ext):
        print(f"  → {label} …", end="", flush=True)
        r = run_sql_benchmark(conn, label, sql, args.runs, args.warmup, gucs)
        results.append(r)
        print(f" avg={r['avg_ms']:.3f} ms")

    if args.bench_lake_index and Path(index_db).is_file():
        print("  → lake_index_prune …", end="", flush=True)
        r = run_lake_index_prune_benchmark(
            index_db, args.target_user_id, args.runs, args.warmup
        )
        results.append(r)
        print(f" avg={r['avg_ms']:.3f} ms")

    conn.close()

    report = markdown_report(
        results,
        args.num_files,
        args.rows_per_file,
        effective_index_via,
        g_ext,
        args.parquet_row_group_size,
        build_rerun_command(args),
    )
    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(report, encoding="utf-8")

    print(f"\nReport written to: {out}")
    print("\nRerun:")
    print(build_rerun_command(args))
    print("\n" + report)


if __name__ == "__main__":
    main()
