"""
PostgreSQL Integration via multicorn / parquet_fdw
====================================================

Strategy
--------
PostgreSQL cannot natively use a Python-managed secondary index.
We bridge the gap with three complementary techniques, in order of
invasiveness:

1. **Index-aware view + SQL function** (no extension required)
   A PL/Python or PL/pgSQL function `pruned_parquet_files(where_clause)`
   calls the Python index and returns the candidate file list.  A view
   wraps the Parquet FDW table so that queries against the view only touch
   the pruned file subset.

2. **Multicorn FDW** (requires `multicorn` extension)
   Implement a Multicorn ForeignDataWrapper subclass that calls the index
   during `get_rel_size` and `get_paths` to influence the planner's cost
   estimates, and during `execute` to prune files before scanning.

3. **parquet_fdw + WHERE rewrite** (requires `parquet_fdw` extension)
   Generate a rewritten SQL query that passes the pruned file list to
   parquet_fdw directly.

This module implements all three approaches and provides a helper to
generate the necessary DDL.
"""

from __future__ import annotations

import json
import os
import textwrap
from pathlib import Path
from typing import Any, Optional

# ---------------------------------------------------------------------------
# SQL DDL generator  (approach 1 – PL/Python function + view)
# ---------------------------------------------------------------------------

def generate_plpython_function_ddl(
    index_db_path: str,
    index_module_path: str,
    function_name: str = "pruned_parquet_files",
    schema: str = "public",
) -> str:
    """
    Return DDL that creates a PL/Python3u function returning the list of
    Parquet file paths that might satisfy a WHERE clause string.

    Requirements
    ------------
    • PostgreSQL with `plpython3u` extension
    • The parquet_index package importable from the Postgres server's Python

    The generated function:
        public.pruned_parquet_files(where_clause text, index_db text)
        RETURNS text[]
    """
    return textwrap.dedent(f"""\
        -- Enable PL/Python (run once as superuser)
        CREATE EXTENSION IF NOT EXISTS plpython3u;

        CREATE OR REPLACE FUNCTION {schema}.{function_name}(
            where_clause text,
            index_db     text DEFAULT {_sql_str(index_db_path)}
        )
        RETURNS text[]
        LANGUAGE plpython3u
        VOLATILE STRICT
        AS $$
import sys
sys.path.insert(0, {_py_str(index_module_path)})
from index.parquet_index import IndexStore
from index.planner import QueryPlanner

store = IndexStore(index_db)
planner = QueryPlanner(store)
predicates = QueryPlanner.parse_where(where_clause)
candidate_files, _ = planner.prune(predicates)
return candidate_files
$$;

COMMENT ON FUNCTION {schema}.{function_name}(text, text) IS
'Returns Parquet file paths that might satisfy the WHERE clause '
'by consulting the secondary index.  Used for file pruning.';
""")


def generate_parquet_fdw_ddl(
    data_dir: str,
    server_name: str = "parquet_lake",
    table_name: str = "lake_data",
    schema: str = "public",
    columns: Optional[dict[str, str]] = None,  # {col_name: pg_type}
) -> str:
    """
    Generate DDL for the parquet_fdw foreign table.

    Assumes `parquet_fdw` extension is installed:
        CREATE EXTENSION parquet_fdw;

    The generated foreign table reads all Parquet files in *data_dir*.
    """
    col_defs = "\n    ".join(
        f"{col} {typ}," for col, typ in (columns or {}).items()
    ).rstrip(",")
    if not col_defs:
        col_defs = "-- add column definitions matching your Parquet schema"

    return textwrap.dedent(f"""\
        -- Install parquet_fdw (compile from https://github.com/adjust/parquet_fdw)
        CREATE EXTENSION IF NOT EXISTS parquet_fdw;

        CREATE SERVER IF NOT EXISTS {server_name}
            FOREIGN DATA WRAPPER parquet_fdw;

        -- Foreign table that reads all files in the data directory
        CREATE FOREIGN TABLE IF NOT EXISTS {schema}.{table_name} (
            {col_defs}
        )
        SERVER {server_name}
        OPTIONS (filename {_sql_str(os.path.join(data_dir, '*.parquet'))},
                 sorted 'false');
""")


def generate_pruned_query(
    base_table: str,
    candidate_files: list[str],
    original_where: str,
    schema: str = "public",
) -> str:
    """
    Rewrite a query to use only the candidate files returned by the index.

    For parquet_fdw this means generating a UNION ALL of per-file foreign
    tables (or using parquet_fdw's filename list option in a subquery).
    """
    if not candidate_files:
        return f"-- No candidate files found for: {original_where}\nSELECT * FROM {schema}.{base_table} WHERE FALSE;"

    # parquet_fdw supports a comma-separated filename list
    files_literal = ", ".join(f"'{f}'" for f in candidate_files)
    return textwrap.dedent(f"""\
        -- Pruned query: only {len(candidate_files)} file(s) need to be scanned
        -- (index eliminated other files for: {original_where})
        SELECT *
        FROM parquet_fdw_scan(
            ARRAY[{files_literal}]  -- pruned file list from secondary index
        )
        WHERE {original_where};
""")


# ---------------------------------------------------------------------------
# Multicorn FDW implementation  (approach 2)
# ---------------------------------------------------------------------------

MULTICORN_WRAPPER_CODE = '''"""
Multicorn FDW wrapper for indexed Parquet data lake.

Installation
------------
1.  pip install multicorn2 pyarrow
2.  In PostgreSQL:
        CREATE EXTENSION multicorn;
        CREATE SERVER parquet_index_srv
            FOREIGN DATA WRAPPER multicorn
            OPTIONS (wrapper 'fdw.multicorn_fdw.IndexedParquetFDW');
        CREATE FOREIGN TABLE lake (...)
            SERVER parquet_index_srv
            OPTIONS (data_dir '/data/lake', index_db '/data/lake/.index.db');
"""

import os
import sys
from typing import Any, Iterator

import pyarrow as pa
import pyarrow.dataset as ds

# Multicorn is only available inside PostgreSQL's Python interpreter
try:
    from multicorn import ForeignDataWrapper, TableDefinition, ColumnDefinition
    from multicorn.utils import log_to_postgres, ERROR, WARNING, DEBUG
except ImportError:
    ForeignDataWrapper = object  # type: ignore

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from index.parquet_index import IndexStore
from index.planner import QueryPlanner, Predicate, _build_arrow_filter


class IndexedParquetFDW(ForeignDataWrapper):
    """
    A Multicorn FDW that leverages the Parquet secondary index to prune
    files before scanning.

    Quals (WHERE conditions) that reference indexed columns are pushed
    down to the index; only candidate files are then scanned with PyArrow.
    """

    def __init__(self, options: dict, columns: dict):
        super().__init__(options, columns)
        self.data_dir = options["data_dir"]
        index_db = options.get("index_db", os.path.join(self.data_dir, ".index.db"))
        self.store = IndexStore(index_db)
        self.planner = QueryPlanner(self.store)
        self.columns = columns  # {name: ColumnDefinition}

    # -- Planner hooks -------------------------------------------------------

    def get_rel_size(self, quals, columns):
        """Estimate rows and average width; used by the query planner."""
        predicates = self._quals_to_predicates(quals)
        candidate_files, _ = self.planner.prune(predicates)
        # Rough estimate: sum row_counts from index
        total_rows = 0
        for fp in candidate_files:
            for col in list(self.columns.keys())[:1]:
                fs = self.store.get_stats(fp, col)
                if fs:
                    total_rows += fs.row_count
                    break
        avg_width = 100  # bytes per row (rough)
        return (max(1, total_rows), avg_width)

    def get_path_keys(self):
        """Advertise which columns can be used for index lookups."""
        indexed_cols = self._indexed_column_names()
        return [((col,), 1) for col in indexed_cols]

    # -- Execution -----------------------------------------------------------

    def execute(self, quals, columns):
        """
        Execute the scan: prune files with the index, then read with PyArrow.
        Each row is yielded as a dict.
        """
        predicates = self._quals_to_predicates(quals)
        candidate_files, _ = self.planner.prune(predicates)

        if not candidate_files:
            return

        arrow_filter = _build_arrow_filter(predicates)
        col_names = list(columns) if columns else None

        dataset = ds.dataset(candidate_files, format="parquet")
        try:
            table = dataset.to_table(columns=col_names, filter=arrow_filter)
        except Exception as exc:
            log_to_postgres(f"IndexedParquetFDW scan error: {exc}", ERROR)
            return

        for batch in table.to_batches():
            batch_dict = batch.to_pydict()
            keys = list(batch_dict.keys())
            for i in range(batch.num_rows):
                yield {k: batch_dict[k][i] for k in keys}

    # -- Helpers -------------------------------------------------------------

    def _indexed_column_names(self) -> list[str]:
        conn = self.store._conn()
        rows = conn.execute("SELECT DISTINCT column_name FROM column_stats").fetchall()
        return [r[0] for r in rows]

    def _quals_to_predicates(self, quals) -> list[Predicate]:
        """Convert Multicorn Qual objects to our Predicate objects."""
        predicates = []
        OP_MAP = {"=": "=", "<": "<", "<=": "<=", ">": ">", ">=": ">="}
        for qual in quals:
            op = OP_MAP.get(qual.operator)
            if op is None:
                continue
            predicates.append(Predicate(qual.field_name, op, qual.value))
        return predicates
'''


def write_multicorn_wrapper(output_path: str) -> None:
    """Write the Multicorn FDW wrapper to *output_path*."""
    Path(output_path).write_text(MULTICORN_WRAPPER_CODE)
    print(f"Wrote Multicorn FDW to {output_path}")


# ---------------------------------------------------------------------------
# Convenience: print all DDL for a given setup
# ---------------------------------------------------------------------------

def print_setup_ddl(
    data_dir: str,
    index_db: str,
    module_path: str,
    columns: Optional[dict[str, str]] = None,
) -> None:
    print("=" * 70)
    print("STEP 1 – PL/Python function (no extra extension needed beyond plpython3u)")
    print("=" * 70)
    print(generate_plpython_function_ddl(index_db, module_path))

    print("=" * 70)
    print("STEP 2 – parquet_fdw foreign table")
    print("=" * 70)
    print(generate_parquet_fdw_ddl(data_dir, columns=columns))

    print("=" * 70)
    print("STEP 3 – Multicorn FDW (alternative; requires multicorn extension)")
    print("=" * 70)
    print(textwrap.dedent("""\
        CREATE EXTENSION multicorn;

        CREATE SERVER parquet_index_srv
            FOREIGN DATA WRAPPER multicorn
            OPTIONS (wrapper 'fdw.multicorn_fdw.IndexedParquetFDW');

        CREATE FOREIGN TABLE lake_indexed (
            -- mirror your schema here
        )
        SERVER parquet_index_srv
        OPTIONS (
            data_dir  """ + _sql_str(data_dir) + """,
            index_db  """ + _sql_str(index_db) + """
        );

        -- Now queries like:
        --   SELECT * FROM lake_indexed WHERE user_id = 42;
        -- will automatically use the index for file pruning.
    """))


def _sql_str(s: str) -> str:
    return "'" + s.replace("'", "''") + "'"


def _py_str(s: str) -> str:
    return repr(s)
'''
'''
