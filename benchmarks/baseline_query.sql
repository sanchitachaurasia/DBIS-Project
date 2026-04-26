-- Baseline: scan parquet metadata directly across all generated files.
SELECT COUNT(DISTINCT l.uri)
FROM parquet.list('/tmp/parquet_gsi_bench/**/*.parquet') AS l
JOIN LATERAL parquet.metadata(l.uri) AS m ON true
WHERE m.path_in_schema = 'user_id'
	AND (m.stats_min IS NULL OR m.stats_min::bigint <= 42)
	AND (m.stats_max IS NULL OR m.stats_max::bigint >= 42);

