parquet_gsi = shared_module('parquet_gsi',
  files('parquet_gsi.c'),
  kwargs: contrib_mod_args,
)
contrib_targets += parquet_gsi

install_data(
  'parquet_gsi.control',
  'parquet_gsi--1.0.sql',
  kwargs: contrib_data_args,
)
