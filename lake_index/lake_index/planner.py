"""
Indexer, Query Planner, and Background File Watcher
====================================================
Continues parquet_index.py with the higher-level components.
"""

import os
import queue
import re
import threading
import time
from pathlib import Path
from typing import Any, Optional

import pyarrow as pa
import pyarrow.parquet as pq

from .parquet_index import BloomFilter, FileStats, IndexStore, RowGroupStats


# ---------------------------------------------------------------------------
# File statistics builder
# ---------------------------------------------------------------------------

BLOOM_ELIGIBLE_TYPES = {
    pa.types.is_integer,
    pa.types.is_floating,
    pa.types.is_string,
    pa.types.is_large_string,
    pa.types.is_date,
}

MAX_BLOOM_CARDINALITY = 1_000_000  # skip bloom for very high cardinality


def _is_bloom_eligible_type(dtype: pa.DataType) -> bool:
    return any(check(dtype) for check in BLOOM_ELIGIBLE_TYPES)


def _build_bloom_filter(column: pa.ChunkedArray) -> Optional[BloomFilter]:
    """Build a Bloom filter from a column if the type is suitable."""
    dtype = column.type
    if not _is_bloom_eligible_type(dtype):
        return None
    seen: set[Any] = set()
    for chunk in column.chunks:
        for val in chunk.to_pylist():
            if val is None:
                continue
            seen.add(val)
            if len(seen) > MAX_BLOOM_CARDINALITY:
                return None
    if not seen:
        return None
    bf = BloomFilter(capacity=len(seen), error_rate=0.01)
    for val in seen:
        bf.add(val)
    return bf


def build_file_stats(
    file_path: str, columns: Optional[list[str]] = None
) -> list[FileStats]:
    """
    Read a Parquet file and return FileStats for every indexed column.
    Uses Parquet row-group statistics where possible (fast path) and
    falls back to a full column scan only for the Bloom filter.
    """
    path = Path(file_path)
    stat = path.stat()
    pf = pq.ParquetFile(file_path)
    schema = pf.schema_arrow

    target_cols = columns if columns else [schema.field(i).name for i in range(len(schema))]
    target_cols = [c for c in target_cols if schema.get_field_index(c) >= 0]

    # --- Collect per-column min/max from row-group metadata (no I/O) --------
    col_min: dict[str, Any] = {}
    col_max: dict[str, Any] = {}
    col_nulls: dict[str, int] = {}

    for rg in range(pf.metadata.num_row_groups):
        rg_meta = pf.metadata.row_group(rg)
        for ci in range(rg_meta.num_columns):
            col_meta = rg_meta.column(ci)
            cname = col_meta.path_in_schema
            if cname not in target_cols:
                continue
            stats = col_meta.statistics
            if stats and stats.has_min_max:
                prev_min = col_min.get(cname)
                prev_max = col_max.get(cname)
                try:
                    col_min[cname] = stats.min if prev_min is None else min(prev_min, stats.min)
                    col_max[cname] = stats.max if prev_max is None else max(prev_max, stats.max)
                except TypeError:
                    col_min[cname] = stats.min
                    col_max[cname] = stats.max
            if stats:
                col_nulls[cname] = col_nulls.get(cname, 0) + (stats.null_count or 0)

    row_count = pf.metadata.num_rows

    # --- Build Bloom filters (requires reading only eligible column data) ---
    bloom_cols = [
        cname for cname in target_cols
        if _is_bloom_eligible_type(schema.field(cname).type)
    ]
    table = pq.read_table(file_path, columns=bloom_cols) if bloom_cols else None
    col_bloom: dict[str, Optional[BloomFilter]] = {}
    for cname in target_cols:
        if table is None or cname not in bloom_cols:
            col_bloom[cname] = None
            continue
        col_bloom[cname] = _build_bloom_filter(table.column(cname))

    # --- Assemble FileStats -------------------------------------------------
    results = []
    for cname in target_cols:
        results.append(FileStats(
            file_path=file_path,
            column_name=cname,
            row_count=row_count,
            null_count=col_nulls.get(cname, 0),
            min_value=col_min.get(cname),
            max_value=col_max.get(cname),
            bloom_filter=col_bloom.get(cname),
            file_mtime=stat.st_mtime,
            file_size=stat.st_size,
        ))
    return results


def build_row_group_stats(
    file_path: str, columns: Optional[list[str]] = None
) -> list[RowGroupStats]:
    """
    Read row-group min/max/null-count metadata for each indexed column.

    This is the granularity expected by the PostgreSQL parquet_gsi catalog:
    one row per (file, row_group, column).
    """
    path = Path(file_path)
    stat = path.stat()
    pf = pq.ParquetFile(file_path)
    schema = pf.schema_arrow

    target_cols = columns if columns else [schema.field(i).name for i in range(len(schema))]
    target_cols = [c for c in target_cols if schema.get_field_index(c) >= 0]

    results: list[RowGroupStats] = []

    for rg_idx in range(pf.metadata.num_row_groups):
        rg_meta = pf.metadata.row_group(rg_idx)
        row_count = rg_meta.num_rows

        for col_idx in range(rg_meta.num_columns):
            col_meta = rg_meta.column(col_idx)
            cname = col_meta.path_in_schema
            if cname not in target_cols:
                continue

            stats = col_meta.statistics
            min_value = None
            max_value = None
            null_count = 0

            if stats:
                null_count = stats.null_count or 0
                if stats.has_min_max:
                    min_value = stats.min
                    max_value = stats.max

            results.append(RowGroupStats(
                file_path=file_path,
                column_name=cname,
                row_group_id=rg_idx,
                row_count=row_count,
                null_count=null_count,
                min_value=min_value,
                max_value=max_value,
                file_mtime=stat.st_mtime,
                file_size=stat.st_size,
            ))

    return results


# ---------------------------------------------------------------------------
# Query planner – predicate pushdown via file pruning
# ---------------------------------------------------------------------------

class Predicate:
    """
    A simple predicate on a single column.

    Supported forms
    ---------------
    • EQ   column = value
    • LT   column < value
    • LE   column <= value
    • GT   column > value
    • GE   column >= value
    • RANGE  lo <= column <= hi  (convenience constructor)
    """

    EQ = "="
    LT = "<"
    LE = "<="
    GT = ">"
    GE = ">="

    def __init__(self, column: str, op: str, value: Any,
                 hi_value: Any = None, hi_inclusive: bool = True):
        self.column = column
        self.op = op
        self.value = value
        self.hi_value = hi_value          # only for RANGE
        self.hi_inclusive = hi_inclusive

    # -- factories -----------------------------------------------------------

    @classmethod
    def eq(cls, column: str, value: Any) -> "Predicate":
        return cls(column, cls.EQ, value)

    @classmethod
    def lt(cls, column: str, value: Any) -> "Predicate":
        return cls(column, cls.LT, value)

    @classmethod
    def le(cls, column: str, value: Any) -> "Predicate":
        return cls(column, cls.LE, value)

    @classmethod
    def gt(cls, column: str, value: Any) -> "Predicate":
        return cls(column, cls.GT, value)

    @classmethod
    def ge(cls, column: str, value: Any) -> "Predicate":
        return cls(column, cls.GE, value)

    @classmethod
    def range(cls, column: str, lo: Any, hi: Any,
              lo_inclusive: bool = True, hi_inclusive: bool = True) -> "Predicate":
        p = cls(column, "RANGE", lo, hi_value=hi, hi_inclusive=hi_inclusive)
        p.lo_inclusive = lo_inclusive
        return p

    # -- evaluation against FileStats ----------------------------------------

    def matches_stats(self, stats: FileStats) -> bool:
        """
        Return False only when the file is CERTAIN to contain no matching rows.
        Conservatively returns True when uncertain.
        """
        if self.op == self.EQ:
            return stats.might_satisfy_eq(self.value)
        if self.op == "RANGE":
            return stats.might_satisfy_range(
                lo=self.value, hi=self.hi_value,
                lo_inclusive=getattr(self, "lo_inclusive", True),
                hi_inclusive=self.hi_inclusive,
            )
        # Translate comparison operators to range checks
        if self.op == self.GT:
            return stats.might_satisfy_range(lo=self.value, lo_inclusive=False)
        if self.op == self.GE:
            return stats.might_satisfy_range(lo=self.value, lo_inclusive=True)
        if self.op == self.LT:
            return stats.might_satisfy_range(hi=self.value, hi_inclusive=False)
        if self.op == self.LE:
            return stats.might_satisfy_range(hi=self.value, hi_inclusive=True)
        return True  # unknown op – be conservative

    def __repr__(self) -> str:
        if self.op == "RANGE":
            lo_inc = getattr(self, "lo_inclusive", True)
            lo_b = "[" if lo_inc else "("
            hi_b = "]" if self.hi_inclusive else ")"
            return f"Predicate({lo_b}{self.value} <= {self.column} <= {self.hi_value}{hi_b})"
        return f"Predicate({self.column} {self.op} {self.value!r})"


class QueryPlanner:
    """
    Uses the IndexStore to determine which Parquet files *might* satisfy
    a conjunction of Predicates (AND semantics only for now).

    Files that are definitely outside the predicate range are pruned.
    """

    def __init__(self, store: IndexStore):
        self.store = store

    def prune(self, predicates: list[Predicate]) -> tuple[list[str], dict[str, Any]]:
        """
        Return (candidate_files, stats_dict) where:
          candidate_files – paths to files that might satisfy all predicates
          stats_dict      – pruning summary plus per-file/per-column stats
        """
        if not predicates:
            all_files = self.store.all_files()
            return all_files, {
                "total_files": len(all_files),
                "candidate_files": len(all_files),
                "files_pruned": 0,
                "pruning_ratio": 0.0,
                "file_stats": {},
            }

        # Build a map: file_path -> column -> FileStats
        file_stats: dict[str, dict[str, FileStats]] = {}

        for pred in predicates:
            col_stats = self.store.get_all_stats_for_column(pred.column)
            for fs in col_stats:
                file_stats.setdefault(fs.file_path, {})[pred.column] = fs

        # Files with no stats for a predicate column are included conservatively
        all_known = set(self.store.all_files())
        candidate_files = []
        pruned = []

        for fp in all_known:
            if self._file_satisfies_all(fp, predicates, file_stats):
                candidate_files.append(fp)
            else:
                pruned.append(fp)

        total_files = len(all_known)
        files_pruned = len(pruned)
        pruning_ratio = (files_pruned / total_files) if total_files else 0.0

        return candidate_files, {
            "total_files": total_files,
            "candidate_files": len(candidate_files),
            "files_pruned": files_pruned,
            "pruning_ratio": pruning_ratio,
            "file_stats": file_stats,
        }

    def _file_satisfies_all(
        self, file_path: str, predicates: list[Predicate],
        file_stats: dict[str, dict[str, FileStats]]
    ) -> bool:
        for pred in predicates:
            col_map = file_stats.get(file_path, {})
            if pred.column not in col_map:
                # No stats for this column → cannot prune
                continue
            if not pred.matches_stats(col_map[pred.column]):
                return False
        return True

    # -- SQL WHERE clause parser (simple subset) ----------------------------

    _TOKEN_RE = re.compile(
        r"(?P<col>[a-zA-Z_]\w*)"
        r"\s*(?P<op><=|>=|<>|!=|<|>|=)\s*"
        r"(?P<val>'[^']*'|-?\d+(?:\.\d+)?)",
        re.IGNORECASE,
    )

    @classmethod
    def parse_where(cls, where_clause: str) -> list[Predicate]:
        """
        Parse a simple SQL WHERE clause into Predicates.
        Supports: AND-connected col op value comparisons.
        String literals must be single-quoted.
        """
        predicates = []
        for part in re.split(r"\bAND\b", where_clause, flags=re.IGNORECASE):
            m = cls._TOKEN_RE.search(part.strip())
            if not m:
                continue
            col = m.group("col")
            op = m.group("op")
            raw = m.group("val")
            if raw.startswith("'"):
                val: Any = raw[1:-1]
            else:
                val = float(raw) if "." in raw else int(raw)
            if op == "<>":
                op = "!="
            if op == "!=":
                continue  # NOT EQUAL not supported by zone maps
            predicates.append(Predicate(col, op, val))
        return predicates


# ---------------------------------------------------------------------------
# Main DataLakeIndex facade
# ---------------------------------------------------------------------------

class DataLakeIndex:
    """
    High-level API for the Parquet secondary index.

    Usage
    -----
    idx = DataLakeIndex(data_dir="/data/lake", index_db="/data/lake/.index.db")
    idx.index_all()                          # initial scan
    idx.start_background_watcher()           # watch for new files
    files = idx.query(predicates=[Predicate.eq("user_id", 42)])
    table  = idx.read_matching(predicates=[...])
    """

    def __init__(
        self,
        data_dir: str,
        index_db: str,
        indexed_columns: Optional[list[str]] = None,
        file_pattern: str = "*.parquet",
    ):
        self.data_dir = Path(data_dir)
        self.store = IndexStore(index_db)
        self.indexed_columns = indexed_columns  # None = index all columns
        self.file_pattern = file_pattern
        self.planner = QueryPlanner(self.store)
        self._watcher: Optional["BackgroundIndexer"] = None

    # -- indexing ------------------------------------------------------------

    def index_file(self, file_path: str) -> None:
        """Index a single Parquet file (blocking)."""
        path = Path(file_path)
        mtime = path.stat().st_mtime
        if self.store.is_indexed(str(path), mtime):
            return
        stats = build_file_stats(str(path), self.indexed_columns)
        self.store.upsert_file(stats)

    def index_all(self, force: bool = False) -> int:
        """Scan the data directory and index any new/modified files. Returns count."""
        indexed = 0
        for fp in self.data_dir.rglob(self.file_pattern):
            if force or not self.store.is_indexed(str(fp), fp.stat().st_mtime):
                self.index_file(str(fp))
                indexed += 1
        return indexed

    # -- querying ------------------------------------------------------------

    def query(
        self,
        predicates: Optional[list[Predicate]] = None,
        where: Optional[str] = None,
    ) -> tuple[list[str], dict[str, Any]]:
        """
        Return the list of Parquet file paths that *might* satisfy the predicates.

        Parameters
        ----------
        predicates : list of Predicate objects  (OR pass where=)
        where      : simple SQL WHERE clause string

        Returns
        -------
        (candidate_files, pruning_stats)
        """
        if where is not None:
            predicates = QueryPlanner.parse_where(where)
        predicates = predicates or []
        return self.planner.prune(predicates)

    def read_matching(
        self,
        predicates: Optional[list[Predicate]] = None,
        where: Optional[str] = None,
        columns: Optional[list[str]] = None,
    ) -> "pa.Table":
        """
        Read only the Parquet files that might match the predicates and
        concatenate them into a single Arrow table.  Applies the predicates
        again at the Arrow layer for correctness (two-phase execution).
        """
        import pyarrow.dataset as ds

        if where is not None:
            predicates = QueryPlanner.parse_where(where)
        predicates = predicates or []
        candidate_files, _ = self.query(predicates=predicates, where=where)
        if not candidate_files:
            # Return empty table with correct schema
            return pa.table({})

        dataset = ds.dataset(candidate_files, format="parquet")
        # Build an Arrow filter expression for correctness
        arrow_filter = _build_arrow_filter(predicates)
        return dataset.to_table(columns=columns, filter=arrow_filter)

    # -- background watcher --------------------------------------------------

    def start_background_watcher(self, poll_interval: float = 5.0) -> None:
        """Start a background thread that watches for new Parquet files."""
        if self._watcher and self._watcher.is_alive():
            return
        self._watcher = BackgroundIndexer(
            data_dir=str(self.data_dir),
            store=self.store,
            indexed_columns=self.indexed_columns,
            file_pattern=self.file_pattern,
            poll_interval=poll_interval,
        )
        self._watcher.daemon = True
        self._watcher.start()

    def stop_background_watcher(self) -> None:
        if self._watcher:
            self._watcher.stop()
            self._watcher.join(timeout=10)

    def stats(self) -> dict:
        return self.store.stats_summary()

    def __repr__(self) -> str:
        s = self.stats()
        return (
            f"DataLakeIndex(dir={self.data_dir}, "
            f"files={s['indexed_files']}, stats={s['column_stats']})"
        )


# ---------------------------------------------------------------------------
# Background indexer thread
# ---------------------------------------------------------------------------

class BackgroundIndexer(threading.Thread):
    """
    Polls the data directory every *poll_interval* seconds and indexes any
    Parquet files whose mtime has changed since the last index pass.

    Design: a simple polling loop is chosen over inotify / FSEvents for
    portability (works on NFS, S3-fuse, container volumes, etc.).
    """

    def __init__(
        self,
        data_dir: str,
        store: IndexStore,
        indexed_columns: Optional[list[str]],
        file_pattern: str,
        poll_interval: float,
    ):
        super().__init__(name="ParquetBackgroundIndexer")
        self.data_dir = Path(data_dir)
        self.store = store
        self.indexed_columns = indexed_columns
        self.file_pattern = file_pattern
        self.poll_interval = poll_interval
        self._stop_event = threading.Event()
        self._task_queue: queue.Queue[str] = queue.Queue()

    def run(self) -> None:
        while not self._stop_event.is_set():
            try:
                self._scan_once()
            except Exception as exc:
                print(f"[BackgroundIndexer] error during scan: {exc}")
            self._stop_event.wait(self.poll_interval)

    def _scan_once(self) -> None:
        for fp in self.data_dir.rglob(self.file_pattern):
            try:
                mtime = fp.stat().st_mtime
            except OSError:
                continue
            if not self.store.is_indexed(str(fp), mtime):
                try:
                    stats = build_file_stats(str(fp), self.indexed_columns)
                    self.store.upsert_file(stats)
                    print(f"[BackgroundIndexer] indexed {fp.name}")
                except Exception as exc:
                    print(f"[BackgroundIndexer] failed to index {fp}: {exc}")

    def stop(self) -> None:
        self._stop_event.set()


# ---------------------------------------------------------------------------
# Arrow filter builder (for correctness pass)
# ---------------------------------------------------------------------------

def _build_arrow_filter(predicates: list[Predicate]):
    """Convert Predicates to a PyArrow compute expression."""
    import pyarrow.compute as pc

    if not predicates:
        return None

    exprs = []
    for p in predicates:
        field = pc.field(p.column)
        if p.op == Predicate.EQ:
            exprs.append(field == p.value)
        elif p.op == Predicate.LT:
            exprs.append(field < p.value)
        elif p.op == Predicate.LE:
            exprs.append(field <= p.value)
        elif p.op == Predicate.GT:
            exprs.append(field > p.value)
        elif p.op == Predicate.GE:
            exprs.append(field >= p.value)
        elif p.op == "RANGE":
            lo_inc = getattr(p, "lo_inclusive", True)
            hi_inc = p.hi_inclusive
            lo_expr = (field >= p.value) if lo_inc else (field > p.value)
            hi_expr = (field <= p.hi_value) if hi_inc else (field < p.hi_value)
            exprs.append(lo_expr & hi_expr)
        # Skip != – unsupported by zone maps, skip at filter layer too

    if not exprs:
        return None
    result = exprs[0]
    for e in exprs[1:]:
        result = result & e
    return result
