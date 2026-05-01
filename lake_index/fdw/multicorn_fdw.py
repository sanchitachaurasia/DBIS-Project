"""
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

# Ensure project root is importable from anywhere
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pyarrow as pa
import pyarrow.dataset as ds

# Multicorn is only available inside PostgreSQL's Python interpreter
try:
    from multicorn import ForeignDataWrapper
    from multicorn.utils import log_to_postgres, ERROR, WARNING, DEBUG
except ImportError:
    ForeignDataWrapper = object  # type: ignore
    def log_to_postgres(msg, level=None): print(msg)
    ERROR = WARNING = DEBUG = None

from lake_index.parquet_index import IndexStore
from lake_index.planner import QueryPlanner, Predicate, _build_arrow_filter


class IndexedParquetFDW(ForeignDataWrapper):
    """Multicorn FDW that uses the local Parquet index for file pruning."""

    def __init__(self, options: dict, columns: dict):
        """Initialize the FDW, backing index store, and query planner."""
        super().__init__(options, columns)
        self.data_dir = options["data_dir"]
        index_db = options.get("index_db", os.path.join(self.data_dir, ".index.db"))
        self.store = IndexStore(index_db)
        self.planner = QueryPlanner(self.store)
        self.columns = columns

    def get_rel_size(self, quals, columns):
        """Estimate rows by summing indexed file sizes for the pruned file set."""
        predicates = self._quals_to_predicates(quals)
        candidate_files, _ = self.planner.prune(predicates)
        total_rows = 0
        # Read one indexed column per file as a cheap row-count proxy; that keeps the planner hint
        # fast while still giving PostgreSQL a better estimate than a hardcoded constant.
        for fp in candidate_files:
            for col in list(self.columns.keys())[:1]:
                fs = self.store.get_stats(fp, col)
                if fs:
                    total_rows += fs.row_count
                    break
        return (max(1, total_rows), 100)

    def get_path_keys(self):
        """Expose the columns that the planner can use for index-aware paths."""
        indexed_cols = self._indexed_column_names()
        return [((col,), 1) for col in indexed_cols]

    def execute(self, quals, columns):
        """Prune candidate files and stream the resulting rows from PyArrow."""
        predicates = self._quals_to_predicates(quals)
        candidate_files, _ = self.planner.prune(predicates)
        if not candidate_files:
            # If pruning removed every file, exit before constructing a dataset object or paying
            # the overhead of a scan path that we already know cannot produce rows.
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

    def _indexed_column_names(self) -> list[str]:
        """Return the set of distinct indexed column names from SQLite."""
        conn = self.store._conn()
        rows = conn.execute("SELECT DISTINCT column_name FROM column_stats").fetchall()
        return [r[0] for r in rows]

    def _quals_to_predicates(self, quals) -> list[Predicate]:
        """Convert Multicorn qual objects into planner predicates."""
        OP_MAP = {"=": "=", "<": "<", "<=": "<=", ">": ">", ">=": ">="}
        predicates = []
        for qual in (quals or []):
            op = OP_MAP.get(getattr(qual, "operator", None))
            if op is None:
                continue
            predicates.append(Predicate(qual.field_name, op, qual.value))
        return predicates