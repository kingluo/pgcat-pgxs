#include "postgres.h"

#include "replication/logical.h"
#include "replication/origin.h"

#include "utils/builtins.h"
#include "utils/pg_lsn.h"

#include "access/heapam.h"
#include "access/sysattr.h"
#include "catalog/namespace.h"
#include "catalog/pg_subscription_rel.h"
#include "executor/executor.h"
#include "nodes/makefuncs.h"
#include "replication/logicalrelation.h"
#include "replication/worker_internal.h"
#include "utils/builtins.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/syscache.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(pgcat_lock_and_check_table_insync);
PG_FUNCTION_INFO_V1(pgcat_set_table_insync);

static bool invalidate_cb_set = false;
static HTAB *LogicalRepRelMap = NULL;

static HTAB *
createHash(void)
{
	HASHCTL		ctl;

	ctl.keysize = sizeof(Oid);
	ctl.entrysize = sizeof(int);

	return hash_create("Remote Con hash", 128, &ctl, HASH_ELEM);
}

static void
logicalrep_relmap_invalidate_cb(Datum arg, Oid reloid) {
	if (LogicalRepRelMap == NULL)
		return;

	if (reloid != InvalidOid)
	{
		hash_search(LogicalRepRelMap, (void *) &reloid,
					HASH_REMOVE, NULL);
	}
	else
	{
		hash_destroy(LogicalRepRelMap);
		LogicalRepRelMap = NULL;
	}
}

Datum
pgcat_lock_and_check_table_insync(PG_FUNCTION_ARGS)
{
	char	   *nspname;
	char	   *relname;
	Oid			relid;
	bool		found;

	if (LogicalRepRelMap == NULL)
	{
		LogicalRepRelMap = createHash();
		if (!invalidate_cb_set)
		{
			CacheRegisterRelcacheCallback(logicalrep_relmap_invalidate_cb,
										  (Datum) 0);
			invalidate_cb_set = true;
		}
	}

	nspname = text_to_cstring((text *) DatumGetPointer(PG_GETARG_DATUM(0)));
	relname = text_to_cstring((text *) DatumGetPointer(PG_GETARG_DATUM(1)));

	relid = RangeVarGetRelid(makeRangeVar(nspname,
							 relname, -1),
							 AccessShareLock, true);

	pfree(nspname);
	pfree(relname);

	if (!OidIsValid(relid))
		elog(ERROR, "relation not found: %s.%s",
			 nspname, relname);

	hash_search(LogicalRepRelMap, (void *) &relid,
				HASH_FIND, &found);
	PG_RETURN_BOOL(found);
}

Datum
pgcat_set_table_insync(PG_FUNCTION_ARGS)
{
	Oid			relid;
	if (LogicalRepRelMap == NULL)
		PG_RETURN_VOID();
	relid = PG_GETARG_OID(0);
	hash_search(LogicalRepRelMap, (void *) &relid,
				HASH_ENTER, NULL);
	PG_RETURN_VOID();
}

/* replicate local changes only */
static bool
pgoutput_origin_filter(LogicalDecodingContext *ctx, RepOriginId origin_id)
{
	return (origin_id != 0);
}

extern void _PG_output_plugin_init(OutputPluginCallbacks *cb);

/* specify output plugin callbacks */
void
_PG_output_plugin_init(OutputPluginCallbacks *cb)
{
	LogicalOutputPluginInit plugin_init;

	plugin_init = (LogicalOutputPluginInit)
		load_external_function("$libdir/pgoutput", "_PG_output_plugin_init", false, NULL);

	if (plugin_init == NULL)
		elog(ERROR, "output plugins have to declare the _PG_output_plugin_init symbol");

	/* ask the output plugin to fill the callback struct */
	plugin_init(cb);

	cb->filter_by_origin_cb = pgoutput_origin_filter;
}
