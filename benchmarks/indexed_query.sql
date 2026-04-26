-- Indexed: use parquet_gsi row-group indexes for the same predicate.
SELECT COUNT(*)
FROM parquet_gsi_query_files('user_id', 42, 42, NULL, NULL, true);
