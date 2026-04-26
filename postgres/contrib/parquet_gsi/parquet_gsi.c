/*-------------------------------------------------------------------------
 *
 * parquet_gsi.c
 *    Native PostgreSQL support code for the parquet_gsi extension.
 *
 * This first native implementation step adds:
 *   - a background worker that periodically scans a configured directory
 *   - a SQL-callable scan function for one-off runs
 *   - worker heartbeat/status bookkeeping in extension-owned tables
 *
 * Row-group metadata extraction and FDW/planner integration remain the next
 * implementation layers.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <errno.h>
#include <signal.h>
#include <sys/stat.h>
#include <unistd.h>

#include "access/xact.h"
#include "catalog/pg_type_d.h"
#include "commands/dbcommands.h"
#include "commands/extension.h"
#include "executor/spi.h"
#include "fmgr.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/bgworker.h"
#include "postmaster/interrupt.h"
#include "storage/fd.h"
#include "storage/latch.h"
#include "storage/procsignal.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"
#include "utils/wait_event.h"

PG_MODULE_MAGIC_EXT(
					.name = "parquet_gsi",
					.version = PG_VERSION
);

PG_FUNCTION_INFO_V1(parquet_gsi_scan_once);
PG_FUNCTION_INFO_V1(parquet_gsi_launch_worker);
PG_FUNCTION_INFO_V1(parquet_gsi_index_file);
PG_FUNCTION_INFO_V1(parquet_gsi_reindex_all);
PG_FUNCTION_INFO_V1(parquet_gsi_add_indexed_column);
PG_FUNCTION_INFO_V1(parquet_gsi_remove_indexed_column);
PG_FUNCTION_INFO_V1(parquet_gsi_reconcile_missing_files);

PGDLLEXPORT void parquet_gsi_worker_main(Datum main_arg);

static bool parquet_gsi_enable_worker = true;
static int	parquet_gsi_naptime = 10;
static char *parquet_gsi_database = NULL;
static char *parquet_gsi_directory = NULL;

static uint32 parquet_gsi_wait_event = 0;

static char *parquet_gsi_extension_schema(void);
static void parquet_gsi_register_worker(void);
static void parquet_gsi_update_worker_state(const char *worker_name,
											const char *database_name,
											const char *directory,
											int pid,
											int files_seen,
											int files_registered,
											const char *last_error,
											bool set_scan_time);
static int	parquet_gsi_scan_directory(const char *directory, int *files_seen);
static int	parquet_gsi_scan_directory_recursive(const char *directory, int *files_seen);
static bool parquet_gsi_has_parquet_suffix(const char *path);
static void parquet_gsi_register_discovered_file(const char *schema_name,
												 const char *path,
												 int64 file_size,
												 double file_mtime);
static bool parquet_gsi_index_file_internal(const char *path);
static int parquet_gsi_reindex_all_internal(const char *directory);
static void parquet_gsi_mark_pending(const char *schema_name,
									 const char *path,
									 int64 file_size,
									 double file_mtime);
static void parquet_gsi_mark_index_failure(const char *schema_name,
										   const char *path,
										   const char *message);
static int parquet_gsi_reconcile_missing_files_internal(void);


void
_PG_init(void)
{
	DefineCustomBoolVariable("parquet_gsi.enable_worker",
							 "Enable the parquet_gsi background worker.",
							 NULL,
							 &parquet_gsi_enable_worker,
							 true,
							 PGC_POSTMASTER,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomIntVariable("parquet_gsi.naptime",
							"Seconds to sleep between parquet_gsi directory scans.",
							NULL,
							&parquet_gsi_naptime,
							10,
							1,
							INT_MAX,
							PGC_SIGHUP,
							GUC_UNIT_S,
							NULL,
							NULL,
							NULL);

	DefineCustomStringVariable("parquet_gsi.database",
							   "Database used by the parquet_gsi static background worker.",
							   NULL,
							   &parquet_gsi_database,
							   "postgres",
							   PGC_POSTMASTER,
							   0,
							   NULL,
							   NULL,
							   NULL);

	DefineCustomStringVariable("parquet_gsi.directory",
							   "Directory scanned by parquet_gsi for Parquet files.",
							   NULL,
							   &parquet_gsi_directory,
							   NULL,
							   PGC_SIGHUP,
							   0,
							   NULL,
							   NULL,
							   NULL);

	MarkGUCPrefixReserved("parquet_gsi");

	if (!process_shared_preload_libraries_in_progress)
		return;

	if (parquet_gsi_enable_worker)
		parquet_gsi_register_worker();
}


static void
parquet_gsi_register_worker(void)
{
	BackgroundWorker worker;

	memset(&worker, 0, sizeof(worker));
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS |
		BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_restart_time = 5;
	snprintf(worker.bgw_library_name, MAXPGPATH, "parquet_gsi");
	snprintf(worker.bgw_function_name, BGW_MAXLEN, "parquet_gsi_worker_main");
	snprintf(worker.bgw_name, BGW_MAXLEN, "parquet_gsi worker");
	snprintf(worker.bgw_type, BGW_MAXLEN, "parquet_gsi");
	worker.bgw_main_arg = (Datum) 0;
	worker.bgw_notify_pid = 0;

	RegisterBackgroundWorker(&worker);
}


void
parquet_gsi_worker_main(Datum main_arg)
{
	Oid			dboid = InvalidOid;
	Oid			roleoid = InvalidOid;
	char	   *p;
	char	   *database_name;
	char	   *last_error = NULL;

	p = MyBgworkerEntry->bgw_extra;
	memcpy(&dboid, p, sizeof(Oid));
	p += sizeof(Oid);
	memcpy(&roleoid, p, sizeof(Oid));

	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	pqsignal(SIGTERM, SignalHandlerForShutdownRequest);
	pqsignal(SIGUSR1, procsignal_sigusr1_handler);
	BackgroundWorkerUnblockSignals();

	if (OidIsValid(dboid))
		BackgroundWorkerInitializeConnectionByOid(dboid, roleoid, 0);
	else
		BackgroundWorkerInitializeConnection(parquet_gsi_database, NULL, 0);

	database_name = pstrdup(get_database_name(MyDatabaseId));

	for (;;)
	{
		int			files_seen = 0;
		int			files_registered = 0;

		if (parquet_gsi_wait_event == 0)
			parquet_gsi_wait_event = WaitEventExtensionNew("ParquetGsiWorkerMain");

		(void) WaitLatch(MyLatch,
						 WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
						 parquet_gsi_naptime * 1000L,
						 parquet_gsi_wait_event);
		ResetLatch(MyLatch);

		CHECK_FOR_INTERRUPTS();

		if (ConfigReloadPending)
		{
			ConfigReloadPending = false;
			ProcessConfigFile(PGC_SIGHUP);
		}

		last_error = NULL;

		PG_TRY();
		{
			SetCurrentStatementStartTimestamp();
			StartTransactionCommand();
			SPI_connect();
			PushActiveSnapshot(GetTransactionSnapshot());
			pgstat_report_activity(STATE_RUNNING, "parquet_gsi scanning directory");

			if (parquet_gsi_directory == NULL || parquet_gsi_directory[0] == '\0')
			{
				last_error = pstrdup("parquet_gsi.directory is not set");
				parquet_gsi_update_worker_state(MyBgworkerEntry->bgw_name,
												database_name,
												parquet_gsi_directory,
												MyProcPid,
												0,
												0,
												last_error,
												false);
			}
			else
			{
				files_registered = parquet_gsi_scan_directory(parquet_gsi_directory, &files_seen);
				(void) parquet_gsi_reconcile_missing_files_internal();
				parquet_gsi_update_worker_state(MyBgworkerEntry->bgw_name,
												database_name,
												parquet_gsi_directory,
												MyProcPid,
											files_seen,
												files_registered,
												NULL,
												true);
			}

			SPI_finish();
			PopActiveSnapshot();
			CommitTransactionCommand();
			pgstat_report_stat(true);
			pgstat_report_activity(STATE_IDLE, NULL);
		}
		PG_CATCH();
		{
			ErrorData  *edata;

			MemoryContextSwitchTo(TopMemoryContext);
			edata = CopyErrorData();
			FlushErrorState();

			AbortCurrentTransaction();
			pgstat_report_activity(STATE_IDLE, NULL);

			PG_TRY();
			{
				SetCurrentStatementStartTimestamp();
				StartTransactionCommand();
				SPI_connect();
				PushActiveSnapshot(GetTransactionSnapshot());
				parquet_gsi_update_worker_state(MyBgworkerEntry->bgw_name,
												database_name,
												parquet_gsi_directory,
												MyProcPid,
												0,
												0,
												edata->message ? edata->message : "unknown parquet_gsi worker error",
												false);
				SPI_finish();
				PopActiveSnapshot();
				CommitTransactionCommand();
			}
			PG_CATCH();
			{
				FlushErrorState();
				AbortCurrentTransaction();
			}
			PG_END_TRY();

			FreeErrorData(edata);
		}
		PG_END_TRY();
	}
}


Datum
parquet_gsi_scan_once(PG_FUNCTION_ARGS)
{
	const char *directory = NULL;
	int			files_registered = 0;

	if (!PG_ARGISNULL(0))
		directory = text_to_cstring(PG_GETARG_TEXT_PP(0));
	else
		directory = parquet_gsi_directory;

	if (directory == NULL || directory[0] == '\0')
		ereport(ERROR,
				(errmsg("parquet_gsi directory is not set"),
				 errhint("Set parquet_gsi.directory or pass a directory argument.")));

	SPI_connect();
	files_registered = parquet_gsi_scan_directory(directory, NULL);
	(void) parquet_gsi_reconcile_missing_files_internal();
	SPI_finish();

	PG_RETURN_INT32(files_registered);
}

Datum
parquet_gsi_index_file(PG_FUNCTION_ARGS)
{
	const char *path = text_to_cstring(PG_GETARG_TEXT_PP(0));

	SPI_connect();
	if (!parquet_gsi_index_file_internal(path))
	{
		SPI_finish();
		PG_RETURN_INT32(0);
	}
	SPI_finish();

	PG_RETURN_INT32(1);
}

Datum
parquet_gsi_reindex_all(PG_FUNCTION_ARGS)
{
	const char *directory = NULL;
	int			indexed_count = 0;

	if (!PG_ARGISNULL(0))
		directory = text_to_cstring(PG_GETARG_TEXT_PP(0));
	else
		directory = parquet_gsi_directory;

	SPI_connect();
	indexed_count = parquet_gsi_reindex_all_internal(directory);
	SPI_finish();

	PG_RETURN_INT32(indexed_count);
}

Datum
parquet_gsi_add_indexed_column(PG_FUNCTION_ARGS)
{
	const char *column_name = text_to_cstring(PG_GETARG_TEXT_PP(0));
	char	   *schema_name;
	StringInfoData sql;
	Oid			argtypes[1];
	Datum		values[1];
	const char *nulls = NULL;

	schema_name = parquet_gsi_extension_schema();
	if (schema_name == NULL)
		elog(ERROR, "parquet_gsi extension is not installed in this database");

	initStringInfo(&sql);
	appendStringInfo(&sql,
					 "INSERT INTO %s (column_name) VALUES ($1) "
					 "ON CONFLICT (column_name) DO NOTHING",
					 quote_qualified_identifier(schema_name, "parquet_gsi_indexed_columns"));

	argtypes[0] = TEXTOID;
	values[0] = CStringGetTextDatum(column_name);

	SPI_connect();
	if (SPI_execute_with_args(sql.data, 1, argtypes, values, nulls, false, 0) < 0)
		elog(ERROR, "could not add indexed column \"%s\"", column_name);
	SPI_finish();

	PG_RETURN_INT32(1);
}

Datum
parquet_gsi_remove_indexed_column(PG_FUNCTION_ARGS)
{
	const char *column_name = text_to_cstring(PG_GETARG_TEXT_PP(0));
	char	   *schema_name;
	StringInfoData sql;
	Oid			argtypes[1];
	Datum		values[1];
	const char *nulls = NULL;

	schema_name = parquet_gsi_extension_schema();
	if (schema_name == NULL)
		elog(ERROR, "parquet_gsi extension is not installed in this database");

	initStringInfo(&sql);
	appendStringInfo(&sql,
					 "DELETE FROM %s WHERE column_name = $1",
					 quote_qualified_identifier(schema_name, "parquet_gsi_indexed_columns"));

	argtypes[0] = TEXTOID;
	values[0] = CStringGetTextDatum(column_name);

	SPI_connect();
	if (SPI_execute_with_args(sql.data, 1, argtypes, values, nulls, false, 0) < 0)
		elog(ERROR, "could not remove indexed column \"%s\"", column_name);
	SPI_finish();

	PG_RETURN_INT32(1);
}


Datum
parquet_gsi_reconcile_missing_files(PG_FUNCTION_ARGS)
{
	int removed;

	SPI_connect();
	removed = parquet_gsi_reconcile_missing_files_internal();
	SPI_finish();

	PG_RETURN_INT32(removed);
}


Datum
parquet_gsi_launch_worker(PG_FUNCTION_ARGS)
{
	BackgroundWorker worker;
	BackgroundWorkerHandle *handle;
	BgwHandleStatus status;
	pid_t		pid = 0;
	Oid			dboid = MyDatabaseId;
	Oid			roleoid = GetUserId();
	char	   *p;

	memset(&worker, 0, sizeof(worker));
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS |
		BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_restart_time = BGW_NEVER_RESTART;
	snprintf(worker.bgw_library_name, MAXPGPATH, "parquet_gsi");
	snprintf(worker.bgw_function_name, BGW_MAXLEN, "parquet_gsi_worker_main");
	snprintf(worker.bgw_name, BGW_MAXLEN, "parquet_gsi dynamic worker");
	snprintf(worker.bgw_type, BGW_MAXLEN, "parquet_gsi");
	worker.bgw_main_arg = (Datum) 0;
	worker.bgw_notify_pid = MyProcPid;

	p = worker.bgw_extra;
	memcpy(p, &dboid, sizeof(Oid));
	p += sizeof(Oid);
	memcpy(p, &roleoid, sizeof(Oid));

	if (!RegisterDynamicBackgroundWorker(&worker, &handle))
		ereport(ERROR,
				(errmsg("registering parquet_gsi background worker failed")));

	status = WaitForBackgroundWorkerStartup(handle, &pid);
	if (status != BGWH_STARTED)
		ereport(ERROR,
				(errmsg("parquet_gsi background worker failed to start")));

	PG_RETURN_INT32(pid);
}


static char *
parquet_gsi_extension_schema(void)
{
	Oid			ext_oid;
	Oid			nsp_oid;

	ext_oid = get_extension_oid("parquet_gsi", true);
	if (!OidIsValid(ext_oid))
		return NULL;

	nsp_oid = get_extension_schema(ext_oid);
	return get_namespace_name(nsp_oid);
}


static void
parquet_gsi_update_worker_state(const char *worker_name,
								const char *database_name,
								const char *directory,
								int pid,
								int files_seen,
								int files_registered,
								const char *last_error,
								bool set_scan_time)
{
	char	   *schema_name;
	Oid			argtypes[8];
	Datum		values[8];
	char		nulls[7] = {' ', ' ', ' ', ' ', ' ', ' ', ' '};
	StringInfoData sql;

	schema_name = parquet_gsi_extension_schema();
	if (schema_name == NULL)
		return;

	initStringInfo(&sql);
	appendStringInfo(&sql,
					 "INSERT INTO %s "
					 "(worker_name, pid, database_name, directory_path, last_start_at, "
					 "last_heartbeat_at, last_scan_at, files_seen, files_registered, last_error) "
					 "VALUES ($1, $2, $3, $4, now(), now(), %s, $5, $6, $7) "
					 "ON CONFLICT (worker_name) DO UPDATE SET "
					 "pid = EXCLUDED.pid, "
					 "database_name = EXCLUDED.database_name, "
					 "directory_path = EXCLUDED.directory_path, "
					 "last_heartbeat_at = EXCLUDED.last_heartbeat_at, "
					 "last_scan_at = EXCLUDED.last_scan_at, "
					 "files_seen = EXCLUDED.files_seen, "
					 "files_registered = EXCLUDED.files_registered, "
					 "last_error = EXCLUDED.last_error",
					 quote_qualified_identifier(schema_name, "parquet_gsi_worker_state"),
					 set_scan_time ? "now()" : "NULL");

	argtypes[0] = TEXTOID;
	argtypes[1] = INT4OID;
	argtypes[2] = TEXTOID;
	argtypes[3] = TEXTOID;
	argtypes[4] = INT4OID;
	argtypes[5] = INT4OID;
	argtypes[6] = TEXTOID;

	values[0] = CStringGetTextDatum(worker_name);
	values[1] = Int32GetDatum(pid);
	values[2] = CStringGetTextDatum(database_name ? database_name : "");
	if (directory != NULL)
		values[3] = CStringGetTextDatum(directory);
	else
	{
		values[3] = (Datum) 0;
			nulls[3] = 'n';
	}
	values[4] = Int32GetDatum(files_seen);
	values[5] = Int32GetDatum(files_registered);
	if (last_error != NULL)
		values[6] = CStringGetTextDatum(last_error);
	else
	{
		values[6] = (Datum) 0;
			nulls[6] = 'n';
	}

	if (SPI_execute_with_args(sql.data, 7, argtypes, values, nulls, false, 0) < 0)
		elog(ERROR, "could not update parquet_gsi worker state");
}


static int
parquet_gsi_scan_directory(const char *directory, int *files_seen)
{
	struct stat st;

	if (stat(directory, &st) != 0)
		ereport(ERROR,
				(errmsg("could not stat parquet_gsi directory \"%s\": %m", directory)));

	if (!S_ISDIR(st.st_mode))
		ereport(ERROR,
				(errmsg("parquet_gsi directory \"%s\" is not a directory", directory)));

	return parquet_gsi_scan_directory_recursive(directory, files_seen);
}


static int
parquet_gsi_scan_directory_recursive(const char *directory, int *files_seen)
{
	DIR		   *dir;
	struct dirent *de;
	int			files_registered = 0;
	char	   *schema_name;

	schema_name = parquet_gsi_extension_schema();
	if (schema_name == NULL)
		ereport(ERROR,
				(errmsg("parquet_gsi extension is not installed in this database")));

	dir = AllocateDir(directory);
	if (dir == NULL)
		ereport(ERROR,
				(errmsg("could not open parquet_gsi directory \"%s\": %m", directory)));

	while ((de = ReadDir(dir, directory)) != NULL)
	{
		char		path[MAXPGPATH];
		struct stat st;

		if (strcmp(de->d_name, ".") == 0 || strcmp(de->d_name, "..") == 0)
			continue;

		snprintf(path, MAXPGPATH, "%s/%s", directory, de->d_name);

		if (stat(path, &st) != 0)
			continue;

		if (S_ISDIR(st.st_mode))
		{
			files_registered += parquet_gsi_scan_directory_recursive(path, files_seen);
			continue;
		}

		if (!S_ISREG(st.st_mode) || !parquet_gsi_has_parquet_suffix(path))
			continue;

		if (files_seen != NULL)
			(*files_seen)++;

		parquet_gsi_register_discovered_file(schema_name,
											 path,
											 (int64) st.st_size,
											 (double) st.st_mtime);
		if (parquet_gsi_index_file_internal(path))
			files_registered++;
	}

	FreeDir(dir);
	return files_registered;
}


static bool
parquet_gsi_has_parquet_suffix(const char *path)
{
	size_t		len = strlen(path);

	return len >= 8 && strcmp(path + len - 8, ".parquet") == 0;
}


static void
parquet_gsi_register_discovered_file(const char *schema_name,
									 const char *path,
									 int64 file_size,
									 double file_mtime)
{
	Oid			argtypes[3];
	Datum		values[3];
	const char *nulls = NULL;
	StringInfoData sql;

	initStringInfo(&sql);
	appendStringInfo(&sql,
					 "INSERT INTO %s "
					 "(file_path, file_size, file_mtime, discovered_at, last_seen_at) "
					 "VALUES ($1, $2, $3, now(), now()) "
					 "ON CONFLICT (file_path) DO UPDATE SET "
					 "file_size = EXCLUDED.file_size, "
					 "file_mtime = EXCLUDED.file_mtime, "
					 "last_seen_at = EXCLUDED.last_seen_at",
					 quote_qualified_identifier(schema_name, "parquet_gsi_discovered_files"));

	argtypes[0] = TEXTOID;
	argtypes[1] = INT8OID;
	argtypes[2] = FLOAT8OID;

	values[0] = CStringGetTextDatum(path);
	values[1] = Int64GetDatum(file_size);
	values[2] = Float8GetDatum(file_mtime);

	if (SPI_execute_with_args(sql.data, 3, argtypes, values, nulls, false, 0) < 0)
		elog(ERROR, "could not register discovered parquet file \"%s\"", path);

	parquet_gsi_mark_pending(schema_name, path, file_size, file_mtime);
}

static bool
parquet_gsi_index_file_internal(const char *path)
{
	char	   *schema_name;
	StringInfoData delete_sql;
	StringInfoData insert_files_sql;
	StringInfoData insert_zonemap_sql;
	Oid			argtypes[1];
	Datum		values[1];
	const char *nulls = NULL;

	schema_name = parquet_gsi_extension_schema();
	if (schema_name == NULL)
		elog(ERROR, "parquet_gsi extension is not installed in this database");

	argtypes[0] = TEXTOID;
	values[0] = CStringGetTextDatum(path);

	initStringInfo(&delete_sql);
	appendStringInfo(&delete_sql,
					 "DELETE FROM %s WHERE file_path = $1",
					 quote_qualified_identifier(schema_name, "row_group_zonemap"));
	if (SPI_execute_with_args(delete_sql.data, 1, argtypes, values, nulls, false, 0) < 0)
		elog(ERROR, "could not clear existing zonemap rows for \"%s\"", path);

	initStringInfo(&insert_files_sql);
	appendStringInfo(&insert_files_sql,
					 "INSERT INTO %s (file_path, file_size, file_mtime, row_count, row_group_count, indexed_at, last_error, index_status) "
					 "SELECT f.uri, d.file_size, d.file_mtime, f.num_rows, f.num_row_groups, now(), NULL, 'indexed' "
					 "FROM parquet.file_metadata($1) AS f "
					 "JOIN %s AS d ON d.file_path = f.uri "
					 "ON CONFLICT (file_path) DO UPDATE SET "
					 "file_size = EXCLUDED.file_size, "
					 "file_mtime = EXCLUDED.file_mtime, "
					 "row_count = EXCLUDED.row_count, "
					 "row_group_count = EXCLUDED.row_group_count, "
					 "indexed_at = EXCLUDED.indexed_at, "
					 "last_error = EXCLUDED.last_error, "
					 "index_status = EXCLUDED.index_status",
					 quote_qualified_identifier(schema_name, "indexed_files"),
					 quote_qualified_identifier(schema_name, "parquet_gsi_discovered_files"));
	if (SPI_execute_with_args(insert_files_sql.data, 1, argtypes, values, nulls, false, 0) < 0)
	{
		parquet_gsi_mark_index_failure(schema_name, path, "could not upsert indexed_files row");
		elog(ERROR, "could not upsert indexed_files row for \"%s\"", path);
	}

	initStringInfo(&insert_zonemap_sql);
	appendStringInfo(&insert_zonemap_sql,
					 "INSERT INTO %s "
					 "(indexed_column, file_path, row_group_id, row_count, null_count, "
					 " min_value_text, max_value_text, min_value_numeric, max_value_numeric, indexed_at) "
					 "SELECT m.path_in_schema, m.uri, m.row_group_id, m.row_group_num_rows, "
					 "       COALESCE(m.stats_null_count, 0), "
					 "       m.stats_min, m.stats_max, "
					 "       CASE WHEN m.stats_min ~ '^[+-]?([0-9]+(\\.[0-9]+)?|\\.[0-9]+)$' "
					 "            THEN m.stats_min::double precision ELSE NULL END, "
					 "       CASE WHEN m.stats_max ~ '^[+-]?([0-9]+(\\.[0-9]+)?|\\.[0-9]+)$' "
					 "            THEN m.stats_max::double precision ELSE NULL END, "
					 "       now() "
					 "FROM parquet.metadata($1) AS m "
					 "WHERE (m.stats_min IS NOT NULL OR m.stats_max IS NOT NULL) "
					 "  AND (NOT EXISTS (SELECT 1 FROM %s) "
					 "       OR EXISTS (SELECT 1 FROM %s c WHERE c.column_name = m.path_in_schema))",
					 quote_qualified_identifier(schema_name, "row_group_zonemap"),
					 quote_qualified_identifier(schema_name, "parquet_gsi_indexed_columns"),
					 quote_qualified_identifier(schema_name, "parquet_gsi_indexed_columns"));
	if (SPI_execute_with_args(insert_zonemap_sql.data, 1, argtypes, values, nulls, false, 0) < 0)
	{
		parquet_gsi_mark_index_failure(schema_name, path, "could not populate zonemap rows");
		elog(ERROR, "could not populate zonemap rows for \"%s\"", path);
	}

	return true;
}

static int
parquet_gsi_reindex_all_internal(const char *directory)
{
	char	   *schema_name;
	StringInfoData sql;
	int			indexed_count = 0;

	schema_name = parquet_gsi_extension_schema();
	if (schema_name == NULL)
		elog(ERROR, "parquet_gsi extension is not installed in this database");

	if (directory != NULL && directory[0] != '\0')
		(void) parquet_gsi_scan_directory(directory, NULL);

	initStringInfo(&sql);
	appendStringInfo(&sql,
					 "SELECT file_path FROM %s ORDER BY file_path",
					 quote_qualified_identifier(schema_name, "parquet_gsi_discovered_files"));
	if (SPI_execute(sql.data, true, 0) != SPI_OK_SELECT)
		elog(ERROR, "could not list discovered parquet files");

	for (uint64 i = 0; i < SPI_processed; i++)
	{
		bool		isnull;
		Datum		datum;
		char	   *path;

		datum = SPI_getbinval(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 1, &isnull);
		if (isnull)
			continue;

		path = TextDatumGetCString(datum);
		if (parquet_gsi_index_file_internal(path))
			indexed_count++;
	}

	return indexed_count;
}

static void
parquet_gsi_mark_pending(const char *schema_name,
						 const char *path,
						 int64 file_size,
						 double file_mtime)
{
	StringInfoData sql;
	Oid			argtypes[3];
	Datum		values[3];
	const char *nulls = NULL;

	initStringInfo(&sql);
	appendStringInfo(&sql,
					 "INSERT INTO %s "
					 "(file_path, file_size, file_mtime, row_count, row_group_count, indexed_at, last_error, index_status) "
					 "VALUES ($1, $2, $3, 0, 0, now(), NULL, 'pending') "
					 "ON CONFLICT (file_path) DO UPDATE SET "
					 "file_size = EXCLUDED.file_size, "
					 "file_mtime = EXCLUDED.file_mtime, "
					 "indexed_at = EXCLUDED.indexed_at, "
					 "last_error = NULL, "
					 "index_status = CASE "
					 "  WHEN %s.file_mtime IS DISTINCT FROM EXCLUDED.file_mtime THEN 'pending' "
					 "  ELSE %s.index_status "
					 "END",
					 quote_qualified_identifier(schema_name, "indexed_files"),
					 quote_qualified_identifier(schema_name, "indexed_files"),
					 quote_qualified_identifier(schema_name, "indexed_files"));

	argtypes[0] = TEXTOID;
	argtypes[1] = INT8OID;
	argtypes[2] = FLOAT8OID;
	values[0] = CStringGetTextDatum(path);
	values[1] = Int64GetDatum(file_size);
	values[2] = Float8GetDatum(file_mtime);

	if (SPI_execute_with_args(sql.data, 3, argtypes, values, nulls, false, 0) < 0)
		elog(ERROR, "could not mark parquet file \"%s\" as pending", path);
}

static void
parquet_gsi_mark_index_failure(const char *schema_name,
							   const char *path,
							   const char *message)
{
	StringInfoData sql;
	Oid			argtypes[2];
	Datum		values[2];
	const char *nulls = NULL;

	initStringInfo(&sql);
	appendStringInfo(&sql,
					 "INSERT INTO %s (file_path, row_group_count, indexed_at, last_error, index_status, retry_count, last_error_at) "
					 "VALUES ($1, 0, now(), $2, 'failed', 1, now()) "
					 "ON CONFLICT (file_path) DO UPDATE SET "
					 "indexed_at = EXCLUDED.indexed_at, "
					 "last_error = EXCLUDED.last_error, "
					 "index_status = EXCLUDED.index_status, "
					 "retry_count = %s.retry_count + 1, "
					 "last_error_at = now()",
					 quote_qualified_identifier(schema_name, "indexed_files"),
					 quote_qualified_identifier(schema_name, "indexed_files"));

	argtypes[0] = TEXTOID;
	argtypes[1] = TEXTOID;
	values[0] = CStringGetTextDatum(path);
	values[1] = CStringGetTextDatum(message);

	(void) SPI_execute_with_args(sql.data, 2, argtypes, values, nulls, false, 0);
}


static int
parquet_gsi_reconcile_missing_files_internal(void)
{
	char		   *schema_name;
	StringInfoData query;
	int			removed = 0;

	schema_name = parquet_gsi_extension_schema();
	if (schema_name == NULL)
		elog(ERROR, "parquet_gsi extension is not installed in this database");

	initStringInfo(&query);
	appendStringInfo(&query,
					 "SELECT file_path FROM %s",
					 quote_qualified_identifier(schema_name, "parquet_gsi_discovered_files"));

	if (SPI_execute(query.data, true, 0) != SPI_OK_SELECT)
		elog(ERROR, "could not read discovered parquet files");

	for (uint64 i = 0; i < SPI_processed; i++)
	{
		bool		isnull;
		Datum		datum;
		char	   *path;
		struct stat st;

		datum = SPI_getbinval(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 1, &isnull);
		if (isnull)
			continue;

		path = TextDatumGetCString(datum);
		if (stat(path, &st) == 0)
			continue;

		if (errno != ENOENT)
			continue;

		{
			StringInfoData del;
			Oid		argtypes[1];
			Datum	values[1];
			const char *nulls = NULL;

			argtypes[0] = TEXTOID;
			values[0] = CStringGetTextDatum(path);

			initStringInfo(&del);
			appendStringInfo(&del,
						 "DELETE FROM %s WHERE file_path = $1",
						 quote_qualified_identifier(schema_name, "parquet_gsi_discovered_files"));
			if (SPI_execute_with_args(del.data, 1, argtypes, values, nulls, false, 0) < 0)
				elog(ERROR, "could not delete missing discovered file \"%s\"", path);

			resetStringInfo(&del);
			appendStringInfo(&del,
						 "DELETE FROM %s WHERE file_path = $1",
						 quote_qualified_identifier(schema_name, "indexed_files"));
			if (SPI_execute_with_args(del.data, 1, argtypes, values, nulls, false, 0) < 0)
				elog(ERROR, "could not delete missing indexed file \"%s\"", path);

			removed++;
		}
	}

	return removed;
}
