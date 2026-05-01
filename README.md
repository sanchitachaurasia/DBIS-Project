# Global Secondary Indices in Data Lakes

This project implements a global secondary index for Parquet-based data lakes so PostgreSQL-style query execution can avoid scanning every file for selective predicates.

## Motivation

Data lakes typically store data as many Parquet files. A naive FDW-style query path scans every file even when a predicate like `user_id = 42` can only match a tiny subset of the lake. The goal here was to build an index that stores compact metadata outside the data files and uses that metadata to prune files before scanning.

## What Was Built

### 1. Python lake index

The `lake_index/` package builds and queries a file-level secondary index.

- `lake_index/lake_index/parquet_index.py`
  Stores indexed metadata in SQLite with WAL mode.
  Tracks per-file registry rows, per-column min/max statistics, and serialized Bloom filters.

- `lake_index/lake_index/planner.py`
  Builds file statistics from Parquet metadata.
  Builds Bloom filters for equality pruning.
  Prunes files with `Predicate` + `QueryPlanner`.
  Supports a correctness pass with `pyarrow.dataset`.
  Includes a background polling indexer.

- `run.py`
  Runs the Python indexer as a practical daemon/utility path.
  Optionally syncs normalized row-group zonemap metadata into PostgreSQL tables.

### 2. Native PostgreSQL extension work

The `postgres/contrib/parquet_gsi/` extension contains the native PostgreSQL-side implementation work.

- Catalog tables for indexed files and row-group zonemaps
- SQL helper functions for candidate row-group/file lookup
- Background-worker support
- FDW path hooks for cheaper index-aware scan paths

This path is designed for native PostgreSQL integration, while the Python path is useful both for experimentation and for benchmarking without requiring the full native stack to be installed.

### 3. Benchmarking and validation

- `benchmarks/benchmark_e2e.py`
  Single benchmark entrypoint for generating test data, rebuilding the index, and timing seqscan vs indexed lookup paths.

- `scripts/setup.sh`
  Convenience setup/build script for the end-to-end flow.

- `scripts/generate_git_diffs.sh`
  Helper for regenerating the archived diffs under `git_diffs/`.

## Functional Changes Completed

- Added real pruning statistics to `QueryPlanner.prune()` so callers get total files, candidates kept, files pruned, and pruning ratio.
- Fixed `read_matching(where=...)` so the Arrow correctness filter uses parsed predicates correctly.
- Reduced indexing overhead by reading only Bloom-eligible columns when building Bloom filters.
- Changed Bloom filter construction to use distinct-value cardinality instead of raw row count.
- Removed duplicate benchmark paths and kept one benchmark driver.
- Added a `--fresh-data` benchmark mode so the Parquet lake can be regenerated from scratch with smaller row groups, producing a much larger zonemap and a clearer measured speedup.
- Regenerated `git_diffs/` from the current vendored Postgres trees.
- Removed unnecessary generated files such as tracked cache/bytecode noise and extra benchmark markdown artifacts.

## How It Was Tested

### Code validation

- Python compile checks:
  `PYTHONDONTWRITEBYTECODE=1 python3 -m py_compile benchmarks/benchmark_e2e.py`

- Smoke generation run:
  `python3 benchmarks/benchmark_e2e.py --fresh-data --generate-only --data-dir /tmp/parquet_gsi_bench_smoke --num-files 3 --rows-per-file 1000 --parquet-row-group-size 64`

- Local pruning correctness:
  indexed a small subset of Parquet files with `DataLakeIndex`, pruned on `user_id = 42`, and verified the returned rows all matched the predicate.

### Benchmark run

Benchmark command used:

```bash
python3 benchmarks/benchmark_e2e.py --dsn "host=localhost port=55432 dbname=dbis_project user=sanchita password=sanchita" --data-dir benchmarks/testdata/parquet_gsi_bench --num-files 100 --rows-per-file 50000 --parquet-row-group-size 64 --target-user-id 42 --runs 5 --warmup 1 --index-via python --fresh-data --generate --output benchmarks/parquet_gsi_e2e_summary.md
```

Observed result from this run:

- `catalog_seqscan`: `4.236 ms`
- `catalog_indexed`: `0.114 ms`
- speedup: about `37.16x`

This benchmark used the Python indexing path to populate `indexed_files` and `row_group_zonemap` because `CREATE EXTENSION parquet_gsi` was not available in the target PostgreSQL instance during the run.

## Repository Layout

- `lake_index/`: Python indexing, planning, and optional FDW helpers
- `postgres/contrib/parquet_gsi/`: native PostgreSQL extension work
- `benchmarks/`: benchmark driver and local benchmark data folder
- `git_diffs/`: regenerated diffs for vendored Postgres-tree changes
- `scripts/`: setup helper scripts

## Setup

### Prerequisites

- PostgreSQL 14+ (with development headers)
- Python 3.8+
- Rust toolchain (for building `pg_parquet` and `parquet_gsi`)
- `pg_config` in `PATH`
- Development tools: `make`, `gcc`, `meson`, `ninja`

### Initial Setup

To set up the repository and run the full end-to-end pipeline (build extension, create schema, generate test data, build index, run benchmark):

```bash
./scripts/setup.sh --dsn "host=localhost dbname=postgres user=whoami" \
                   --pg-src /path/to/postgres \
                   --data-dir benchmarks/testdata/parquet_gsi_bench \
                   --runs 5
```

### Normal Run

After initial setup, to run the normal Python indexer path on a Parquet lake:

```bash
python3 run.py --data-dir benchmarks/testdata/parquet_gsi_bench --verbose
```

If you also want the indexer to sync row-group zonemap metadata into PostgreSQL, pass a libpq DSN:

```bash
python3 run.py \
  --data-dir benchmarks/testdata/parquet_gsi_bench \
  --pg-dsn "host=localhost dbname=postgres user=sanchita password=sanchita" \
  --verbose
```

After initial setup, to regenerate test data and rerun the benchmark (without rebuilding the extension):

```bash
python3 benchmarks/benchmark_e2e.py \
  --dsn "host=localhost dbname=postgres user=sanchita password=sanchita" \
  --data-dir benchmarks/testdata/parquet_gsi_bench \
  --num-files 100 \
  --rows-per-file 50000 \
  --parquet-row-group-size 64 \
  --target-user-id 42 \
  --runs 5 \
  --warmup 1 \
  --index-via python \
  --fresh-data \
  --generate \
  --output benchmarks/parquet_gsi_e2e_summary.md
```

**Key flags:**

- `--fresh-data`: Regenerates Parquet test data from scratch
- `--generate`: Generates the test data before running the benchmark
- `--index-via python`: Uses the Python indexing path (set to `native` for PostgreSQL extension, if available)
- `--runs 5`: Executes the benchmark 5 times and reports average/median
- `--output`: Writes the benchmark report to the specified markdown file

## Rerun Benchmark

Use:

```bash
python3 benchmarks/benchmark_e2e.py --dsn "host=localhost port=55432 dbname=dbis_project user=sanchita password=sanchita" --data-dir benchmarks/testdata/parquet_gsi_bench --num-files 100 --rows-per-file 50000 --parquet-row-group-size 64 --target-user-id 42 --runs 5 --warmup 1 --index-via python --fresh-data --generate --output benchmarks/parquet_gsi_e2e_summary.md
```
