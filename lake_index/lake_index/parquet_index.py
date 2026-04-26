"""
Parquet Secondary Index
=======================
Maintains a secondary index over a collection of Parquet files (a data lake).
The index stores per-file statistics (min/max/bloom-filter) so that the query
planner can skip files that cannot contain qualifying rows (zone-map / file
pruning).

Architecture
------------
  DataLakeIndex          – main facade; builds & queries the index
  FileStats              – per-file statistics for one indexed column
  BloomFilter            – probabilistic membership test (for equality predicates)
  IndexStore             – SQLite persistence layer
  BackgroundIndexer      – watches a directory and indexes new files asynchronously
  QueryPlanner           – rewrites a simple predicate into a pruned file list
"""

import hashlib
import math
import os
import sqlite3
import struct
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterator, Optional

import pyarrow as pa
import pyarrow.parquet as pq


# ---------------------------------------------------------------------------
# Bloom filter (simple bit-array, MurmurHash-inspired double hashing)
# ---------------------------------------------------------------------------

class BloomFilter:
    """A simple Bloom filter for fast set-membership tests."""

    def __init__(self, capacity: int = 100_000, error_rate: float = 0.01):
        self.capacity = capacity
        self.error_rate = error_rate
        # Optimal number of bits and hash functions
        self.num_bits = self._optimal_bits(capacity, error_rate)
        self.num_hashes = self._optimal_hashes(self.num_bits, capacity)
        self.bit_array = bytearray(math.ceil(self.num_bits / 8))

    # -- construction helpers ------------------------------------------------

    @staticmethod
    def _optimal_bits(n: int, p: float) -> int:
        return max(1, int(-n * math.log(p) / (math.log(2) ** 2)))

    @staticmethod
    def _optimal_hashes(m: int, n: int) -> int:
        return max(1, int((m / n) * math.log(2)))

    def _hashes(self, item: Any) -> Iterator[int]:
        key = str(item).encode("utf-8")
        h1 = int(hashlib.md5(key).hexdigest(), 16)
        h2 = int(hashlib.sha1(key).hexdigest(), 16)
        for i in range(self.num_hashes):
            yield (h1 + i * h2) % self.num_bits

    # -- public API ----------------------------------------------------------

    def add(self, item: Any) -> None:
        for bit in self._hashes(item):
            self.bit_array[bit // 8] |= 1 << (bit % 8)

    def __contains__(self, item: Any) -> bool:
        return all(
            self.bit_array[bit // 8] & (1 << (bit % 8))
            for bit in self._hashes(item)
        )

    def to_bytes(self) -> bytes:
        header = struct.pack(">III", self.num_bits, self.num_hashes, self.capacity)
        return header + bytes(self.bit_array)

    @classmethod
    def from_bytes(cls, data: bytes) -> "BloomFilter":
        num_bits, num_hashes, capacity = struct.unpack(">III", data[:12])
        bf = cls.__new__(cls)
        bf.capacity = capacity
        bf.error_rate = 0.01
        bf.num_bits = num_bits
        bf.num_hashes = num_hashes
        bf.bit_array = bytearray(data[12:])
        return bf


# ---------------------------------------------------------------------------
# Per-file, per-column statistics
# ---------------------------------------------------------------------------

@dataclass
class FileStats:
    file_path: str
    column_name: str
    row_count: int
    null_count: int
    min_value: Any
    max_value: Any
    bloom_filter: Optional[BloomFilter] = None
    file_mtime: float = 0.0
    file_size: int = 0

    # -- zone-map predicate evaluation ---------------------------------------

    def might_satisfy_eq(self, value: Any) -> bool:
        """Return False only if the file definitely cannot contain *value*."""
        try:
            if self.min_value is not None and value < self.min_value:
                return False
            if self.max_value is not None and value > self.max_value:
                return False
        except TypeError:
            pass  # incomparable types – be conservative
        if self.bloom_filter is not None:
            return value in self.bloom_filter
        return True

    def might_satisfy_range(
        self, lo: Any = None, hi: Any = None,
        lo_inclusive: bool = True, hi_inclusive: bool = True
    ) -> bool:
        """Return False only if the file's [min,max] range is disjoint from [lo,hi]."""
        try:
            if lo is not None and self.max_value is not None:
                if lo_inclusive and lo > self.max_value:
                    return False
                if not lo_inclusive and lo >= self.max_value:
                    return False
            if hi is not None and self.min_value is not None:
                if hi_inclusive and hi < self.min_value:
                    return False
                if not hi_inclusive and hi <= self.min_value:
                    return False
        except TypeError:
            pass
        return True


# ---------------------------------------------------------------------------
# SQLite persistence
# ---------------------------------------------------------------------------

class IndexStore:
    """
    Stores file statistics in a local SQLite database.

    Schema
    ------
    file_registry  – one row per indexed Parquet file
    column_stats   – one row per (file, column)
    bloom_data     – raw bloom-filter bytes for each (file, column)
    """

    DDL = """
    CREATE TABLE IF NOT EXISTS file_registry (
        file_id   INTEGER PRIMARY KEY AUTOINCREMENT,
        file_path TEXT    NOT NULL UNIQUE,
        file_mtime REAL   NOT NULL,
        file_size  INTEGER NOT NULL,
        row_count  INTEGER NOT NULL,
        indexed_at REAL    NOT NULL
    );

    CREATE TABLE IF NOT EXISTS column_stats (
        stat_id     INTEGER PRIMARY KEY AUTOINCREMENT,
        file_id     INTEGER NOT NULL REFERENCES file_registry(file_id) ON DELETE CASCADE,
        column_name TEXT    NOT NULL,
        null_count  INTEGER NOT NULL,
        min_value   TEXT,
        max_value   TEXT,
        min_numeric REAL,
        max_numeric REAL,
        UNIQUE(file_id, column_name)
    );

    CREATE TABLE IF NOT EXISTS bloom_data (
        file_id     INTEGER NOT NULL REFERENCES file_registry(file_id) ON DELETE CASCADE,
        column_name TEXT    NOT NULL,
        bloom_bytes BLOB    NOT NULL,
        PRIMARY KEY (file_id, column_name)
    );

    CREATE INDEX IF NOT EXISTS idx_col_stats_file ON column_stats(file_id);
    CREATE INDEX IF NOT EXISTS idx_col_stats_col  ON column_stats(column_name);
    CREATE INDEX IF NOT EXISTS idx_registry_path  ON file_registry(file_path);
    """

    def __init__(self, db_path: str):
        self.db_path = db_path
        self._local = threading.local()
        self._init_db()

    def _conn(self) -> sqlite3.Connection:
        if not hasattr(self._local, "conn"):
            conn = sqlite3.connect(self.db_path, check_same_thread=False)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA foreign_keys=ON")
            conn.row_factory = sqlite3.Row
            self._local.conn = conn
        return self._local.conn

    def _init_db(self):
        conn = self._conn()
        conn.executescript(self.DDL)
        conn.commit()

    # -- write ---------------------------------------------------------------

    def upsert_file(self, stats_list: list[FileStats]) -> None:
        if not stats_list:
            return
        conn = self._conn()
        s = stats_list[0]
        conn.execute(
            """INSERT INTO file_registry(file_path, file_mtime, file_size, row_count, indexed_at)
               VALUES (?,?,?,?,?)
               ON CONFLICT(file_path) DO UPDATE SET
                 file_mtime=excluded.file_mtime,
                 file_size=excluded.file_size,
                 row_count=excluded.row_count,
                 indexed_at=excluded.indexed_at""",
            (s.file_path, s.file_mtime, s.file_size, s.row_count, time.time()),
        )
        file_id = conn.execute(
            "SELECT file_id FROM file_registry WHERE file_path=?", (s.file_path,)
        ).fetchone()["file_id"]

        for stat in stats_list:
            min_num = max_num = None
            try:
                min_num = float(stat.min_value) if stat.min_value is not None else None
                max_num = float(stat.max_value) if stat.max_value is not None else None
            except (TypeError, ValueError):
                pass

            conn.execute(
                """INSERT INTO column_stats
                   (file_id, column_name, null_count, min_value, max_value, min_numeric, max_numeric)
                   VALUES (?,?,?,?,?,?,?)
                   ON CONFLICT(file_id, column_name) DO UPDATE SET
                     null_count=excluded.null_count,
                     min_value=excluded.min_value, max_value=excluded.max_value,
                     min_numeric=excluded.min_numeric, max_numeric=excluded.max_numeric""",
                (file_id, stat.column_name, stat.null_count,
                 str(stat.min_value) if stat.min_value is not None else None,
                 str(stat.max_value) if stat.max_value is not None else None,
                 min_num, max_num),
            )

            if stat.bloom_filter is not None:
                conn.execute(
                    """INSERT INTO bloom_data(file_id, column_name, bloom_bytes)
                       VALUES (?,?,?)
                       ON CONFLICT(file_id, column_name) DO UPDATE SET
                         bloom_bytes=excluded.bloom_bytes""",
                    (file_id, stat.column_name, stat.bloom_filter.to_bytes()),
                )
        conn.commit()

    def remove_file(self, file_path: str) -> None:
        conn = self._conn()
        conn.execute("DELETE FROM file_registry WHERE file_path=?", (file_path,))
        conn.commit()

    # -- read ----------------------------------------------------------------

    def is_indexed(self, file_path: str, mtime: float) -> bool:
        row = self._conn().execute(
            "SELECT file_mtime FROM file_registry WHERE file_path=?", (file_path,)
        ).fetchone()
        return row is not None and abs(row["file_mtime"] - mtime) < 0.001

    def all_files(self) -> list[str]:
        rows = self._conn().execute("SELECT file_path FROM file_registry").fetchall()
        return [r["file_path"] for r in rows]

    def get_stats(self, file_path: str, column_name: str) -> Optional[FileStats]:
        conn = self._conn()
        reg = conn.execute(
            "SELECT * FROM file_registry WHERE file_path=?", (file_path,)
        ).fetchone()
        if reg is None:
            return None
        stat = conn.execute(
            "SELECT * FROM column_stats WHERE file_id=? AND column_name=?",
            (reg["file_id"], column_name),
        ).fetchone()
        if stat is None:
            return None
        bloom_row = conn.execute(
            "SELECT bloom_bytes FROM bloom_data WHERE file_id=? AND column_name=?",
            (reg["file_id"], column_name),
        ).fetchone()
        bloom = BloomFilter.from_bytes(bloom_row["bloom_bytes"]) if bloom_row else None

        def _parse(v, numeric):
            if numeric is not None:
                return numeric
            return v

        return FileStats(
            file_path=file_path,
            column_name=column_name,
            row_count=reg["row_count"],
            null_count=stat["null_count"],
            min_value=_parse(stat["min_value"], stat["min_numeric"]),
            max_value=_parse(stat["max_value"], stat["max_numeric"]),
            bloom_filter=bloom,
            file_mtime=reg["file_mtime"],
            file_size=reg["file_size"],
        )

    def get_all_stats_for_column(self, column_name: str) -> list[FileStats]:
        conn = self._conn()
        rows = conn.execute(
            """SELECT fr.file_path, fr.row_count, fr.file_mtime, fr.file_size,
                      cs.null_count, cs.min_value, cs.max_value,
                      cs.min_numeric, cs.max_numeric, cs.file_id
               FROM column_stats cs
               JOIN file_registry fr USING(file_id)
               WHERE cs.column_name = ?""",
            (column_name,),
        ).fetchall()

        result = []
        for r in rows:
            bloom_row = conn.execute(
                "SELECT bloom_bytes FROM bloom_data WHERE file_id=? AND column_name=?",
                (r["file_id"], column_name),
            ).fetchone()
            bloom = BloomFilter.from_bytes(bloom_row["bloom_bytes"]) if bloom_row else None

            def _pick(text, num):
                return num if num is not None else text

            result.append(FileStats(
                file_path=r["file_path"],
                column_name=column_name,
                row_count=r["row_count"],
                null_count=r["null_count"],
                min_value=_pick(r["min_value"], r["min_numeric"]),
                max_value=_pick(r["max_value"], r["max_numeric"]),
                bloom_filter=bloom,
                file_mtime=r["file_mtime"],
                file_size=r["file_size"],
            ))
        return result

    def stats_summary(self) -> dict:
        conn = self._conn()
        n_files = conn.execute("SELECT COUNT(*) FROM file_registry").fetchone()[0]
        n_stats = conn.execute("SELECT COUNT(*) FROM column_stats").fetchone()[0]
        n_bloom = conn.execute("SELECT COUNT(*) FROM bloom_data").fetchone()[0]
        return {"indexed_files": n_files, "column_stats": n_stats, "bloom_filters": n_bloom}
