# Parquet GSI / lake_index benchmark (E2E)

**Dataset:** 100 Parquet files × 50,000 rows = 5,000,000 total rows; **Parquet `row_group_size`:** 64

**Approx. `row_group_zonemap` rows (column `user_id` only):** ~78,200. With a tiny catalog, both seqscan and index paths finish too quickly from shared buffers, so **speedup ratios stay modest** even when the index path does much less work. This benchmark is meant to be rerun with **`--fresh-data --parquet-row-group-size 128`** or smaller so the zonemap grows and the baseline becomes visibly worse.

**Reindex mode:** `python` — **`python` = lake_index** reads Parquet metadata (`build_row_group_stats` / `build_file_stats` in `lake_index/`) and **writes those rows into Postgres** `indexed_files` + `row_group_zonemap`. **`c`** uses the parquet_gsi C extension (`parquet_gsi_reindex_all`, needs `pg_parquet`).

**parquet_gsi extension in database:** `no` (if no: install `pg_parquet`, then `CREATE EXTENSION parquet_gsi CASCADE`; otherwise this run used a minimal catalog + lake_index only).

**Rerun command:**

```bash
python3 benchmarks/benchmark_e2e.py --dsn 'host=localhost port=55432 dbname=dbis_project user=sanchita password=sanchita' --data-dir '/Users/sanchita/Sem-6/CS349/DBIS PROJECT/benchmarks/testdata/parquet_gsi_bench' --num-files 100 --rows-per-file 50000 --parquet-row-group-size 64 --target-user-id 42 --runs 5 --warmup 1 --index-via python --output benchmarks/parquet_gsi_e2e_summary.md --fresh-data --generate
```

## Summary

The **catalog_seqscan** path disables bitmap/index scans so the planner does a true **sequential scan** of `row_group_zonemap` with the same predicates as the GSI path. Other rows use default settings so the **B-tree on numeric bounds** can be used. Use `parquet_gsi_candidate_files` (not `parquet_gsi_query_files(..., true)`) to avoid an unrelated full scan of `parquet_gsi_discovered_files` in the “indexed” timing.

| Query path | avg ms | median ms | min ms | max ms | speedup vs seqscan baseline |
|------------|--------|-----------|--------|--------|-----------------------------|
| catalog_seqscan | 3.517 | 3.377 | 3.185 | 3.908 | 1× (seqscan baseline) |
| catalog_indexed | 0.113 | 0.111 | 0.107 | 0.120 | 31.24× |

---

## catalog_seqscan

Planner settings (seqscan-only baseline only):

```text
SET LOCAL enable_seqscan = on;
SET LOCAL enable_indexscan = off;
SET LOCAL enable_bitmapscan = off;
SET LOCAL enable_indexonlyscan = off;
SET LOCAL enable_tidscan = off;
SET LOCAL max_parallel_workers_per_gather = 0;
```

```sql
SELECT COUNT(DISTINCT file_path)
FROM row_group_zonemap
WHERE indexed_column = 'user_id'
  AND (min_value_numeric IS NULL OR min_value_numeric <= 42.0)
  AND (max_value_numeric IS NULL OR max_value_numeric >= 42.0)
```

- kind: sql
- warmup: 1
- runs: 5
- timings_ms: 3.185, 3.225, 3.908, 3.891, 3.377
- avg_ms: 3.517

### EXPLAIN ANALYZE (last run)
```text
Aggregate  (cost=3360.50..3360.51 rows=1 width=8) (actual time=3.368..3.369 rows=1.00 loops=1)
  Buffers: shared hit=1899
  ->  Sort  (cost=3357.34..3358.92 rows=631 width=98) (actual time=3.330..3.345 rows=782.00 loops=1)
        Sort Key: file_path
        Sort Method: quicksort  Memory: 25kB
        Buffers: shared hit=1899
        ->  Seq Scan on row_group_zonemap  (cost=0.00..3328.00 rows=631 width=98) (actual time=0.004..3.306 rows=782.00 loops=1)
              Filter: (((min_value_numeric IS NULL) OR (min_value_numeric <= '42'::double precision)) AND ((max_value_numeric IS NULL) OR (max_value_numeric >= '42'::double precision)) AND (indexed_column = 'user_id'::text))
              Rows Removed by Filter: 77418
              Buffers: shared hit=1899
Planning Time: 0.035 ms
Execution Time: 3.377 ms
```

## catalog_indexed

```sql
SELECT COUNT(DISTINCT file_path)
FROM row_group_zonemap
WHERE indexed_column = 'user_id'
  AND (min_value_numeric IS NULL OR min_value_numeric <= 42.0)
  AND (max_value_numeric IS NULL OR max_value_numeric >= 42.0)
```

- kind: sql
- warmup: 1
- runs: 5
- timings_ms: 0.111, 0.109, 0.120, 0.116, 0.107
- avg_ms: 0.113

### EXPLAIN ANALYZE (last run)
```text
Aggregate  (cost=1370.34..1370.35 rows=1 width=8) (actual time=0.104..0.104 rows=1.00 loops=1)
  Buffers: shared hit=29
  ->  Sort  (cost=1367.18..1368.76 rows=631 width=98) (actual time=0.073..0.087 rows=782.00 loops=1)
        Sort Key: file_path
        Sort Method: quicksort  Memory: 25kB
        Buffers: shared hit=29
        ->  Bitmap Heap Scan on row_group_zonemap  (cost=27.47..1337.84 rows=631 width=98) (actual time=0.014..0.052 rows=782.00 loops=1)
              Recheck Cond: (((indexed_column = 'user_id'::text) AND (min_value_numeric IS NULL)) OR ((indexed_column = 'user_id'::text) AND (min_value_numeric <= '42'::double precision)))
              Filter: ((max_value_numeric IS NULL) OR (max_value_numeric >= '42'::double precision))
              Heap Blocks: exact=19
              Buffers: shared hit=29
              ->  BitmapOr  (cost=27.47..27.47 rows=631 width=0) (actual time=0.012..0.012 rows=0.00 loops=1)
                    Buffers: shared hit=10
                    ->  Bitmap Index Scan on row_group_zonemap_column_numeric_bounds_idx  (cost=0.00..4.43 rows=1 width=0) (actual time=0.001..0.001 rows=0.00 loops=1)
                          Index Cond: ((indexed_column = 'user_id'::text) AND (min_value_numeric IS NULL))
                          Index Searches: 1
                          Buffers: shared hit=3
                    ->  Bitmap Index Scan on row_group_zonemap_column_numeric_bounds_idx  (cost=0.00..22.73 rows=631 width=0) (actual time=0.011..0.011 rows=782.00 loops=1)
                          Index Cond: ((indexed_column = 'user_id'::text) AND (min_value_numeric <= '42'::double precision))
                          Index Searches: 1
                          Buffers: shared hit=7
Planning Time: 0.017 ms
Execution Time: 0.107 ms
```
