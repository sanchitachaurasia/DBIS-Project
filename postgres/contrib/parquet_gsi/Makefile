# contrib/parquet_gsi/Makefile

MODULE_big = parquet_gsi
OBJS = parquet_gsi.o

EXTENSION = parquet_gsi
DATA = parquet_gsi--1.0.sql
PGFILEDESC = "parquet_gsi - parquet zonemap background worker scaffold"

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/parquet_gsi
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
