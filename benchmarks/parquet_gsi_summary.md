# parquet_gsi EXPLAIN ANALYZE Benchmark Summary

These timings come from `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)` execution time.

## Baseline
- runs: 5
- timings_ms: 5.002, 1.603, 1.515, 1.582, 1.491
- avg_ms: 2.239
- median_ms: 1.582
- min_ms: 1.491
- max_ms: 5.002

### Raw EXPLAIN ANALYZE Output
#### Run 1
```text
Aggregate  (cost=27574.83..27574.84 rows=1 width=8) (actual time=4.950..4.950 rows=1.00 loops=1)
  Buffers: shared hit=3
  ->  Sort  (cost=27569.83..27572.33 rows=1000 width=32) (actual time=4.943..4.944 rows=1.00 loops=1)
        Sort Key: l.uri
        Sort Method: quicksort  Memory: 25kB
        Buffers: shared hit=3
        ->  Nested Loop  (cost=0.01..27520.00 rows=1000 width=32) (actual time=3.663..4.927 rows=1.00 loops=1)
              ->  Function Scan on list l  (cost=0.00..10.00 rows=1000 width=32) (actual time=0.415..0.416 rows=20.00 loops=1)
              ->  Function Scan on metadata m  (cost=0.00..27.50 rows=1 width=0) (actual time=0.225..0.225 rows=0.05 loops=20)
                    Filter: ((path_in_schema = 'user_id'::text) AND ((stats_min IS NULL) OR ((stats_min)::bigint <= 42)) AND ((stats_max IS NULL) OR ((stats_max)::bigint >= 42)))
                    Rows Removed by Filter: 30
Planning:
  Buffers: shared hit=26
Planning Time: 0.095 ms
Execution Time: 5.002 ms
```

#### Run 2
```text
Aggregate  (cost=27574.83..27574.84 rows=1 width=8) (actual time=1.596..1.596 rows=1.00 loops=1)
  ->  Sort  (cost=27569.83..27572.33 rows=1000 width=32) (actual time=1.594..1.594 rows=1.00 loops=1)
        Sort Key: l.uri
        Sort Method: quicksort  Memory: 25kB
        ->  Nested Loop  (cost=0.01..27520.00 rows=1000 width=32) (actual time=1.181..1.593 rows=1.00 loops=1)
              ->  Function Scan on list l  (cost=0.00..10.00 rows=1000 width=32) (actual time=0.107..0.107 rows=20.00 loops=1)
              ->  Function Scan on metadata m  (cost=0.00..27.50 rows=1 width=0) (actual time=0.074..0.074 rows=0.05 loops=20)
                    Filter: ((path_in_schema = 'user_id'::text) AND ((stats_min IS NULL) OR ((stats_min)::bigint <= 42)) AND ((stats_max IS NULL) OR ((stats_max)::bigint >= 42)))
                    Rows Removed by Filter: 30
Planning Time: 0.018 ms
Execution Time: 1.603 ms
```

#### Run 3
```text
Aggregate  (cost=27574.83..27574.84 rows=1 width=8) (actual time=1.510..1.510 rows=1.00 loops=1)
  ->  Sort  (cost=27569.83..27572.33 rows=1000 width=32) (actual time=1.509..1.509 rows=1.00 loops=1)
        Sort Key: l.uri
        Sort Method: quicksort  Memory: 25kB
        ->  Nested Loop  (cost=0.01..27520.00 rows=1000 width=32) (actual time=1.081..1.509 rows=1.00 loops=1)
              ->  Function Scan on list l  (cost=0.00..10.00 rows=1000 width=32) (actual time=0.104..0.105 rows=20.00 loops=1)
              ->  Function Scan on metadata m  (cost=0.00..27.50 rows=1 width=0) (actual time=0.070..0.070 rows=0.05 loops=20)
                    Filter: ((path_in_schema = 'user_id'::text) AND ((stats_min IS NULL) OR ((stats_min)::bigint <= 42)) AND ((stats_max IS NULL) OR ((stats_max)::bigint >= 42)))
                    Rows Removed by Filter: 30
Planning Time: 0.012 ms
Execution Time: 1.515 ms
```

#### Run 4
```text
Aggregate  (cost=27574.83..27574.84 rows=1 width=8) (actual time=1.575..1.575 rows=1.00 loops=1)
  ->  Sort  (cost=27569.83..27572.33 rows=1000 width=32) (actual time=1.574..1.574 rows=1.00 loops=1)
        Sort Key: l.uri
        Sort Method: quicksort  Memory: 25kB
        ->  Nested Loop  (cost=0.01..27520.00 rows=1000 width=32) (actual time=1.157..1.574 rows=1.00 loops=1)
              ->  Function Scan on list l  (cost=0.00..10.00 rows=1000 width=32) (actual time=0.117..0.117 rows=20.00 loops=1)
              ->  Function Scan on metadata m  (cost=0.00..27.50 rows=1 width=0) (actual time=0.073..0.073 rows=0.05 loops=20)
                    Filter: ((path_in_schema = 'user_id'::text) AND ((stats_min IS NULL) OR ((stats_min)::bigint <= 42)) AND ((stats_max IS NULL) OR ((stats_max)::bigint >= 42)))
                    Rows Removed by Filter: 30
Planning Time: 0.012 ms
Execution Time: 1.582 ms
```

#### Run 5
```text
Aggregate  (cost=27574.83..27574.84 rows=1 width=8) (actual time=1.486..1.486 rows=1.00 loops=1)
  ->  Sort  (cost=27569.83..27572.33 rows=1000 width=32) (actual time=1.484..1.485 rows=1.00 loops=1)
        Sort Key: l.uri
        Sort Method: quicksort  Memory: 25kB
        ->  Nested Loop  (cost=0.01..27520.00 rows=1000 width=32) (actual time=1.070..1.484 rows=1.00 loops=1)
              ->  Function Scan on list l  (cost=0.00..10.00 rows=1000 width=32) (actual time=0.100..0.101 rows=20.00 loops=1)
              ->  Function Scan on metadata m  (cost=0.00..27.50 rows=1 width=0) (actual time=0.069..0.069 rows=0.05 loops=20)
                    Filter: ((path_in_schema = 'user_id'::text) AND ((stats_min IS NULL) OR ((stats_min)::bigint <= 42)) AND ((stats_max IS NULL) OR ((stats_max)::bigint >= 42)))
                    Rows Removed by Filter: 30
Planning Time: 0.010 ms
Execution Time: 1.491 ms
```


## Indexed
- runs: 5
- timings_ms: 0.147, 0.109, 0.103, 0.102, 0.101
- avg_ms: 0.112
- median_ms: 0.103
- min_ms: 0.101
- max_ms: 0.147

### Raw EXPLAIN ANALYZE Output
#### Run 1
```text
Aggregate  (cost=116.25..116.26 rows=1 width=8) (actual time=0.134..0.134 rows=1.00 loops=1)
  Buffers: shared hit=10
  ->  Unique  (cost=98.63..105.24 rows=881 width=64) (actual time=0.131..0.134 rows=21.00 loops=1)
        Buffers: shared hit=10
        ->  Sort  (cost=98.63..100.83 rows=881 width=64) (actual time=0.131..0.132 rows=21.00 loops=1)
              Sort Key: c.file_path, ('indexed'::text)
              Sort Method: quicksort  Memory: 27kB
              Buffers: shared hit=10
              ->  Append  (cost=8.51..55.53 rows=881 width=64) (actual time=0.021..0.036 rows=21.00 loops=1)
                    Buffers: shared hit=10
                    ->  Subquery Scan on c  (cost=8.51..8.53 rows=1 width=128) (actual time=0.021..0.021 rows=1.00 loops=1)
                          Buffers: shared hit=8
                          ->  Unique  (cost=8.51..8.53 rows=1 width=96) (actual time=0.020..0.021 rows=1.00 loops=1)
                                Buffers: shared hit=8
                                ->  Subquery Scan on c_1  (cost=8.51..8.53 rows=1 width=96) (actual time=0.020..0.020 rows=1.00 loops=1)
                                      Buffers: shared hit=8
                                      ->  Sort  (cost=8.51..8.52 rows=1 width=100) (actual time=0.020..0.020 rows=1.00 loops=1)
                                            Sort Key: z.file_path, z.row_group_id
                                            Sort Method: quicksort  Memory: 25kB
                                            Buffers: shared hit=8
                                            ->  Seq Scan on row_group_zonemap z  (cost=0.00..8.50 rows=1 width=100) (actual time=0.012..0.015 rows=1.00 loops=1)
                                                  Filter: (((max_value_numeric IS NULL) OR (max_value_numeric >= '42'::double precision)) AND ((min_value_numeric IS NULL) OR (min_value_numeric <= '42'::double precision)) AND (indexed_column = 'user_id'::text))
                                                  Rows Removed by Filter: 199
                                                  Buffers: shared hit=5
                    ->  Hash Left Join  (cost=21.48..42.60 rows=880 width=64) (actual time=0.011..0.013 rows=20.00 loops=1)
                          Hash Cond: (d.file_path = i.file_path)
                          Filter: ((i.file_path IS NULL) OR (i.index_status <> 'indexed'::text) OR (i.file_mtime IS DISTINCT FROM d.file_mtime))
                          Buffers: shared hit=2
                          ->  Seq Scan on parquet_gsi_discovered_files d  (cost=0.00..18.80 rows=880 width=40) (actual time=0.001..0.001 rows=20.00 loops=1)
                                Buffers: shared hit=1
                          ->  Hash  (cost=15.10..15.10 rows=510 width=72) (actual time=0.006..0.006 rows=20.00 loops=1)
                                Buckets: 1024  Batches: 1  Memory Usage: 11kB
                                Buffers: shared hit=1
                                ->  Seq Scan on indexed_files i  (cost=0.00..15.10 rows=510 width=72) (actual time=0.002..0.003 rows=20.00 loops=1)
                                      Buffers: shared hit=1
Planning:
  Buffers: shared hit=539
Planning Time: 0.788 ms
Execution Time: 0.147 ms
```

#### Run 2
```text
Aggregate  (cost=116.25..116.26 rows=1 width=8) (actual time=0.102..0.103 rows=1.00 loops=1)
  Buffers: shared hit=7
  ->  Unique  (cost=98.63..105.24 rows=881 width=64) (actual time=0.100..0.102 rows=21.00 loops=1)
        Buffers: shared hit=7
        ->  Sort  (cost=98.63..100.83 rows=881 width=64) (actual time=0.100..0.100 rows=21.00 loops=1)
              Sort Key: c.file_path, ('indexed'::text)
              Sort Method: quicksort  Memory: 27kB
              Buffers: shared hit=7
              ->  Append  (cost=8.51..55.53 rows=881 width=64) (actual time=0.010..0.018 rows=21.00 loops=1)
                    Buffers: shared hit=7
                    ->  Subquery Scan on c  (cost=8.51..8.53 rows=1 width=128) (actual time=0.010..0.010 rows=1.00 loops=1)
                          Buffers: shared hit=5
                          ->  Unique  (cost=8.51..8.53 rows=1 width=96) (actual time=0.010..0.010 rows=1.00 loops=1)
                                Buffers: shared hit=5
                                ->  Subquery Scan on c_1  (cost=8.51..8.53 rows=1 width=96) (actual time=0.010..0.010 rows=1.00 loops=1)
                                      Buffers: shared hit=5
                                      ->  Sort  (cost=8.51..8.52 rows=1 width=100) (actual time=0.009..0.010 rows=1.00 loops=1)
                                            Sort Key: z.file_path, z.row_group_id
                                            Sort Method: quicksort  Memory: 25kB
                                            Buffers: shared hit=5
                                            ->  Seq Scan on row_group_zonemap z  (cost=0.00..8.50 rows=1 width=100) (actual time=0.007..0.009 rows=1.00 loops=1)
                                                  Filter: (((max_value_numeric IS NULL) OR (max_value_numeric >= '42'::double precision)) AND ((min_value_numeric IS NULL) OR (min_value_numeric <= '42'::double precision)) AND (indexed_column = 'user_id'::text))
                                                  Rows Removed by Filter: 199
                                                  Buffers: shared hit=5
                    ->  Hash Left Join  (cost=21.48..42.60 rows=880 width=64) (actual time=0.005..0.007 rows=20.00 loops=1)
                          Hash Cond: (d.file_path = i.file_path)
                          Filter: ((i.file_path IS NULL) OR (i.index_status <> 'indexed'::text) OR (i.file_mtime IS DISTINCT FROM d.file_mtime))
                          Buffers: shared hit=2
                          ->  Seq Scan on parquet_gsi_discovered_files d  (cost=0.00..18.80 rows=880 width=40) (actual time=0.001..0.001 rows=20.00 loops=1)
                                Buffers: shared hit=1
                          ->  Hash  (cost=15.10..15.10 rows=510 width=72) (actual time=0.003..0.003 rows=20.00 loops=1)
                                Buckets: 1024  Batches: 1  Memory Usage: 11kB
                                Buffers: shared hit=1
                                ->  Seq Scan on indexed_files i  (cost=0.00..15.10 rows=510 width=72) (actual time=0.001..0.002 rows=20.00 loops=1)
                                      Buffers: shared hit=1
Planning Time: 0.095 ms
Execution Time: 0.109 ms
```

#### Run 3
```text
Aggregate  (cost=116.25..116.26 rows=1 width=8) (actual time=0.097..0.098 rows=1.00 loops=1)
  Buffers: shared hit=7
  ->  Unique  (cost=98.63..105.24 rows=881 width=64) (actual time=0.095..0.097 rows=21.00 loops=1)
        Buffers: shared hit=7
        ->  Sort  (cost=98.63..100.83 rows=881 width=64) (actual time=0.095..0.096 rows=21.00 loops=1)
              Sort Key: c.file_path, ('indexed'::text)
              Sort Method: quicksort  Memory: 27kB
              Buffers: shared hit=7
              ->  Append  (cost=8.51..55.53 rows=881 width=64) (actual time=0.009..0.017 rows=21.00 loops=1)
                    Buffers: shared hit=7
                    ->  Subquery Scan on c  (cost=8.51..8.53 rows=1 width=128) (actual time=0.009..0.010 rows=1.00 loops=1)
                          Buffers: shared hit=5
                          ->  Unique  (cost=8.51..8.53 rows=1 width=96) (actual time=0.009..0.009 rows=1.00 loops=1)
                                Buffers: shared hit=5
                                ->  Subquery Scan on c_1  (cost=8.51..8.53 rows=1 width=96) (actual time=0.009..0.009 rows=1.00 loops=1)
                                      Buffers: shared hit=5
                                      ->  Sort  (cost=8.51..8.52 rows=1 width=100) (actual time=0.009..0.009 rows=1.00 loops=1)
                                            Sort Key: z.file_path, z.row_group_id
                                            Sort Method: quicksort  Memory: 25kB
                                            Buffers: shared hit=5
                                            ->  Seq Scan on row_group_zonemap z  (cost=0.00..8.50 rows=1 width=100) (actual time=0.006..0.009 rows=1.00 loops=1)
                                                  Filter: (((max_value_numeric IS NULL) OR (max_value_numeric >= '42'::double precision)) AND ((min_value_numeric IS NULL) OR (min_value_numeric <= '42'::double precision)) AND (indexed_column = 'user_id'::text))
                                                  Rows Removed by Filter: 199
                                                  Buffers: shared hit=5
                    ->  Hash Left Join  (cost=21.48..42.60 rows=880 width=64) (actual time=0.004..0.007 rows=20.00 loops=1)
                          Hash Cond: (d.file_path = i.file_path)
                          Filter: ((i.file_path IS NULL) OR (i.index_status <> 'indexed'::text) OR (i.file_mtime IS DISTINCT FROM d.file_mtime))
                          Buffers: shared hit=2
                          ->  Seq Scan on parquet_gsi_discovered_files d  (cost=0.00..18.80 rows=880 width=40) (actual time=0.001..0.001 rows=20.00 loops=1)
                                Buffers: shared hit=1
                          ->  Hash  (cost=15.10..15.10 rows=510 width=72) (actual time=0.003..0.003 rows=20.00 loops=1)
                                Buckets: 1024  Batches: 1  Memory Usage: 11kB
                                Buffers: shared hit=1
                                ->  Seq Scan on indexed_files i  (cost=0.00..15.10 rows=510 width=72) (actual time=0.001..0.002 rows=20.00 loops=1)
                                      Buffers: shared hit=1
Planning Time: 0.071 ms
Execution Time: 0.103 ms
```

#### Run 4
```text
Aggregate  (cost=116.25..116.26 rows=1 width=8) (actual time=0.097..0.097 rows=1.00 loops=1)
  Buffers: shared hit=7
  ->  Unique  (cost=98.63..105.24 rows=881 width=64) (actual time=0.094..0.096 rows=21.00 loops=1)
        Buffers: shared hit=7
        ->  Sort  (cost=98.63..100.83 rows=881 width=64) (actual time=0.094..0.095 rows=21.00 loops=1)
              Sort Key: c.file_path, ('indexed'::text)
              Sort Method: quicksort  Memory: 27kB
              Buffers: shared hit=7
              ->  Append  (cost=8.51..55.53 rows=881 width=64) (actual time=0.009..0.017 rows=21.00 loops=1)
                    Buffers: shared hit=7
                    ->  Subquery Scan on c  (cost=8.51..8.53 rows=1 width=128) (actual time=0.009..0.009 rows=1.00 loops=1)
                          Buffers: shared hit=5
                          ->  Unique  (cost=8.51..8.53 rows=1 width=96) (actual time=0.009..0.009 rows=1.00 loops=1)
                                Buffers: shared hit=5
                                ->  Subquery Scan on c_1  (cost=8.51..8.53 rows=1 width=96) (actual time=0.009..0.009 rows=1.00 loops=1)
                                      Buffers: shared hit=5
                                      ->  Sort  (cost=8.51..8.52 rows=1 width=100) (actual time=0.009..0.009 rows=1.00 loops=1)
                                            Sort Key: z.file_path, z.row_group_id
                                            Sort Method: quicksort  Memory: 25kB
                                            Buffers: shared hit=5
                                            ->  Seq Scan on row_group_zonemap z  (cost=0.00..8.50 rows=1 width=100) (actual time=0.006..0.009 rows=1.00 loops=1)
                                                  Filter: (((max_value_numeric IS NULL) OR (max_value_numeric >= '42'::double precision)) AND ((min_value_numeric IS NULL) OR (min_value_numeric <= '42'::double precision)) AND (indexed_column = 'user_id'::text))
                                                  Rows Removed by Filter: 199
                                                  Buffers: shared hit=5
                    ->  Hash Left Join  (cost=21.48..42.60 rows=880 width=64) (actual time=0.004..0.007 rows=20.00 loops=1)
                          Hash Cond: (d.file_path = i.file_path)
                          Filter: ((i.file_path IS NULL) OR (i.index_status <> 'indexed'::text) OR (i.file_mtime IS DISTINCT FROM d.file_mtime))
                          Buffers: shared hit=2
                          ->  Seq Scan on parquet_gsi_discovered_files d  (cost=0.00..18.80 rows=880 width=40) (actual time=0.001..0.001 rows=20.00 loops=1)
                                Buffers: shared hit=1
                          ->  Hash  (cost=15.10..15.10 rows=510 width=72) (actual time=0.003..0.003 rows=20.00 loops=1)
                                Buckets: 1024  Batches: 1  Memory Usage: 11kB
                                Buffers: shared hit=1
                                ->  Seq Scan on indexed_files i  (cost=0.00..15.10 rows=510 width=72) (actual time=0.001..0.001 rows=20.00 loops=1)
                                      Buffers: shared hit=1
Planning Time: 0.064 ms
Execution Time: 0.102 ms
```

#### Run 5
```text
Aggregate  (cost=116.25..116.26 rows=1 width=8) (actual time=0.096..0.096 rows=1.00 loops=1)
  Buffers: shared hit=7
  ->  Unique  (cost=98.63..105.24 rows=881 width=64) (actual time=0.094..0.096 rows=21.00 loops=1)
        Buffers: shared hit=7
        ->  Sort  (cost=98.63..100.83 rows=881 width=64) (actual time=0.094..0.094 rows=21.00 loops=1)
              Sort Key: c.file_path, ('indexed'::text)
              Sort Method: quicksort  Memory: 27kB
              Buffers: shared hit=7
              ->  Append  (cost=8.51..55.53 rows=881 width=64) (actual time=0.009..0.017 rows=21.00 loops=1)
                    Buffers: shared hit=7
                    ->  Subquery Scan on c  (cost=8.51..8.53 rows=1 width=128) (actual time=0.009..0.009 rows=1.00 loops=1)
                          Buffers: shared hit=5
                          ->  Unique  (cost=8.51..8.53 rows=1 width=96) (actual time=0.009..0.009 rows=1.00 loops=1)
                                Buffers: shared hit=5
                                ->  Subquery Scan on c_1  (cost=8.51..8.53 rows=1 width=96) (actual time=0.009..0.009 rows=1.00 loops=1)
                                      Buffers: shared hit=5
                                      ->  Sort  (cost=8.51..8.52 rows=1 width=100) (actual time=0.009..0.009 rows=1.00 loops=1)
                                            Sort Key: z.file_path, z.row_group_id
                                            Sort Method: quicksort  Memory: 25kB
                                            Buffers: shared hit=5
                                            ->  Seq Scan on row_group_zonemap z  (cost=0.00..8.50 rows=1 width=100) (actual time=0.006..0.009 rows=1.00 loops=1)
                                                  Filter: (((max_value_numeric IS NULL) OR (max_value_numeric >= '42'::double precision)) AND ((min_value_numeric IS NULL) OR (min_value_numeric <= '42'::double precision)) AND (indexed_column = 'user_id'::text))
                                                  Rows Removed by Filter: 199
                                                  Buffers: shared hit=5
                    ->  Hash Left Join  (cost=21.48..42.60 rows=880 width=64) (actual time=0.004..0.007 rows=20.00 loops=1)
                          Hash Cond: (d.file_path = i.file_path)
                          Filter: ((i.file_path IS NULL) OR (i.index_status <> 'indexed'::text) OR (i.file_mtime IS DISTINCT FROM d.file_mtime))
                          Buffers: shared hit=2
                          ->  Seq Scan on parquet_gsi_discovered_files d  (cost=0.00..18.80 rows=880 width=40) (actual time=0.001..0.001 rows=20.00 loops=1)
                                Buffers: shared hit=1
                          ->  Hash  (cost=15.10..15.10 rows=510 width=72) (actual time=0.003..0.003 rows=20.00 loops=1)
                                Buckets: 1024  Batches: 1  Memory Usage: 11kB
                                Buffers: shared hit=1
                                ->  Seq Scan on indexed_files i  (cost=0.00..15.10 rows=510 width=72) (actual time=0.001..0.002 rows=20.00 loops=1)
                                      Buffers: shared hit=1
Planning Time: 0.064 ms
Execution Time: 0.101 ms
```


## Difference
- avg improvement_ms = baseline_avg_ms - indexed_avg_ms = 2.239 - 0.112 = 2.126
- avg speedup_x = baseline_avg_ms / indexed_avg_ms = 2.239 / 0.112 = 19.916
