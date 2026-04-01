/*
 * lance_fdw.cpp
 *
 * Foreign Data Wrapper for Lance columnar datasets in openGauss.
 *
 * Architecture:
 *   SQL Query -> openGauss Planner -> FdwRoutine callbacks (this file)
 *                                          |
 *                                  liblance_c (Rust staticlib)
 *                                          |
 *                                    Lance dataset files
 *
 * Supports:
 *   - Column projection (only requested columns are read)
 *   - Filter pushdown (=, <>, <, <=, >, >=, AND, OR, NOT, IS NULL, IN, LIKE)
 *   - INSERT (append rows to dataset)
 *   - Schema auto-discovery via lance_import()
 *   - EXPLAIN output with dataset URI
 */

#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/reloptions.h"
#include "catalog/pg_foreign_table.h"
#include "catalog/pg_foreign_server.h"
#include "commands/defrem.h"
#include "commands/explain.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "optimizer/cost.h"
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/lsyscache.h"
#include "utils/numeric.h"
#include "utils/datum.h"
#include "catalog/pg_type.h"
#include "catalog/pg_operator.h"
#include "nodes/nodeFuncs.h"
#include "parser/parsetree.h"

#include "lance_fdw.h"

PG_MODULE_MAGIC;

/*
 * Helper: get last error message from liblance_c and copy it into palloc'd memory.
 * Automatically frees the Rust-allocated string. Returns "unknown error" if none.
 */
static const char *
lance_get_error(void)
{
    const char *rust_err = lance_c_last_error_message();
    if (rust_err == NULL)
        return "unknown error";
    char *pg_err = pstrdup(rust_err);
    lance_c_free_string(rust_err);
    return pg_err;
}

/* Default batch size for Arrow record batches */
#define LANCE_DEFAULT_BATCH_SIZE    1024

/*
 * ============================================================
 * SQL function declarations
 * ============================================================
 */

extern "C" Datum lance_fdw_handler(PG_FUNCTION_ARGS);
extern "C" Datum lance_fdw_validator(PG_FUNCTION_ARGS);
extern "C" Datum lance_import(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(lance_fdw_handler);
PG_FUNCTION_INFO_V1(lance_fdw_validator);
PG_FUNCTION_INFO_V1(lance_import);

/*
 * ============================================================
 * FDW callback declarations
 * ============================================================
 */

static void lanceGetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid);
static void lanceGetForeignPaths(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid);
static ForeignScan *lanceGetForeignPlan(PlannerInfo *root, RelOptInfo *baserel,
    Oid foreigntableid, ForeignPath *best_path, List *tlist, List *scan_clauses, Plan *outer_plan);
static void lanceBeginForeignScan(ForeignScanState *node, int eflags);
static TupleTableSlot *lanceIterateForeignScan(ForeignScanState *node);
static void lanceReScanForeignScan(ForeignScanState *node);
static void lanceEndForeignScan(ForeignScanState *node);
static void lanceExplainForeignScan(ForeignScanState *node, ExplainState *es);

/* Write callbacks */
static int lanceIsForeignRelUpdatable(Relation rel);
static List *lancePlanForeignModify(PlannerInfo *root, ModifyTable *plan,
    Index resultRelation, int subplan_index);
static void lanceBeginForeignModify(ModifyTableState *mtstate,
    ResultRelInfo *rinfo, List *fdw_private, int subplan_index, int eflags);
static TupleTableSlot *lanceExecForeignInsert(EState *estate,
    ResultRelInfo *rinfo, TupleTableSlot *slot, TupleTableSlot *planSlot);
static void lanceEndForeignModify(EState *estate, ResultRelInfo *rinfo);

/*
 * State for INSERT operations.
 * We buffer inserted rows and flush them as a single Lance batch.
 */
typedef struct LanceFdwModifyState
{
    char       *uri;
    TupleDesc   tupdesc;
    int         nrows;
    int         max_rows;
    Datum     **row_values;     /* nrows x natts */
    bool      **row_nulls;     /* nrows x natts */
} LanceFdwModifyState;

/* External: Arrow-to-PG conversion from arrow_to_pg.cpp */
extern Datum arrow_value_to_datum(const struct ArrowArray *array,
                                  const struct ArrowSchema *schema,
                                  int64_t row, Oid pg_type_oid, bool *isnull);
extern void arrow_release_array(struct ArrowArray *array);
extern void arrow_release_schema(struct ArrowSchema *schema);

/*
 * ============================================================
 * Option handling
 * ============================================================
 */

typedef struct LanceFdwOption
{
    const char *optname;
    Oid         optcontext;     /* catalog OID where this option is valid */
} LanceFdwOption;

static const LanceFdwOption valid_options[] = {
    /* URI can be specified at table or server level */
    {"uri",        ForeignTableRelationId},
    {"uri",        ForeignServerRelationId},
    {"batch_size", ForeignTableRelationId},
    {"batch_size", ForeignServerRelationId},
    /* Sentinel */
    {NULL,         InvalidOid}
};

static bool
is_lance_option(const char *name, Oid context)
{
    const LanceFdwOption *opt;
    for (opt = valid_options; opt->optname; opt++)
    {
        if (context == opt->optcontext && strcmp(opt->optname, name) == 0)
            return true;
    }
    return false;
}

/*
 * Extract URI and batch_size from FDW options (table + server levels).
 */
static void
lanceGetOptions(Oid foreigntableid, char **uri, int *batch_size)
{
    ForeignTable *table;
    ForeignServer *server;
    List *options;
    ListCell *lc;

    table = GetForeignTable(foreigntableid);
    server = GetForeignServer(table->serverid);

    /* Merge options: table options override server options */
    options = NIL;
    options = list_concat(options, server->options);
    options = list_concat(options, table->options);

    *uri = NULL;
    *batch_size = LANCE_DEFAULT_BATCH_SIZE;

    foreach (lc, options)
    {
        DefElem *def = (DefElem *) lfirst(lc);

        if (strcmp(def->defname, "uri") == 0)
            *uri = defGetString(def);
        else if (strcmp(def->defname, "batch_size") == 0)
            *batch_size = atoi(defGetString(def));
    }

    if (*uri == NULL)
        elog(ERROR, "lance_fdw: 'uri' option is required");
}

/*
 * ============================================================
 * lance_import: schema auto-discovery
 * ============================================================
 */

/*
 * Map Arrow format string to PostgreSQL type name for DDL generation.
 */
static const char *
arrow_format_to_pg_type(const struct ArrowSchema *schema)
{
    const char *f = schema->format;
    if (!f) return "text";

    /* Dictionary: use value type */
    if (schema->dictionary)
        return arrow_format_to_pg_type(schema->dictionary);

    if (f[0] == 'b' && f[1] == '\0') return "boolean";
    if (f[0] == 'c' && f[1] == '\0') return "int2";
    if (f[0] == 'C' && f[1] == '\0') return "int2";
    if (f[0] == 's' && f[1] == '\0') return "int2";
    if (f[0] == 'S' && f[1] == '\0') return "int4";
    if (f[0] == 'i' && f[1] == '\0') return "int4";
    if (f[0] == 'I' && f[1] == '\0') return "int8";
    if (f[0] == 'l' && f[1] == '\0') return "int8";
    if (f[0] == 'L' && f[1] == '\0') return "int8";
    if (f[0] == 'e' && f[1] == '\0') return "float4";
    if (f[0] == 'f' && f[1] == '\0') return "float4";
    if (f[0] == 'g' && f[1] == '\0') return "float8";
    if (f[0] == 'u' && f[1] == '\0') return "text";
    if (f[0] == 'U' && f[1] == '\0') return "text";
    if (f[0] == 'z' && f[1] == '\0') return "bytea";
    if (f[0] == 'Z' && f[1] == '\0') return "bytea";
    if (f[0] == 'w' && f[1] == ':')  return "bytea";
    if (f[0] == 't' && f[1] == 'd')  return "date";
    if (f[0] == 't' && f[1] == 's')
    {
        if (f[3] == ':' && f[4] != '\0')
            return "timestamptz";
        return "timestamp";
    }
    if (f[0] == 'd' && f[1] == ':')  return "numeric";
    if (f[0] == '+' && f[1] == 's')  return "text"; /* struct → JSON text */

    /* List types → array of element type */
    if ((f[0] == '+' && f[1] == 'l') || (f[0] == '+' && f[1] == 'L') ||
        (f[0] == '+' && f[1] == 'w'))
    {
        if (schema->n_children > 0 && schema->children && schema->children[0])
        {
            const char *elem = arrow_format_to_pg_type(schema->children[0]);
            /* Return array type name */
            static char arr_type[64];
            snprintf(arr_type, sizeof(arr_type), "%s[]", elem);
            return arr_type;
        }
        return "text[]";
    }

    return "text"; /* fallback */
}

/*
 * lance_import(server_name, table_name, uri) → text
 *
 * Opens a Lance dataset, reads its schema, and returns a
 * CREATE FOREIGN TABLE DDL statement.
 */
Datum
lance_import(PG_FUNCTION_ARGS)
{
    char *server_name = text_to_cstring(PG_GETARG_TEXT_PP(0));
    char *table_name  = text_to_cstring(PG_GETARG_TEXT_PP(1));
    char *uri         = text_to_cstring(PG_GETARG_TEXT_PP(2));

    /* Open dataset */
    void *dataset = lance_c_open_dataset(uri);
    if (dataset == NULL)
        ereport(ERROR,
            (errcode(ERRCODE_FDW_ERROR),
             errmsg("lance_import: cannot open '%s': %s", uri, lance_get_error())));

    /* Get schema */
    void *schema_handle = lance_c_get_schema(dataset);
    if (schema_handle == NULL)
    {
        lance_c_close_dataset(dataset);
        ereport(ERROR,
            (errcode(ERRCODE_FDW_ERROR),
             errmsg("lance_import: cannot get schema: %s", lance_get_error())));
    }

    /* Export to Arrow C Data Interface */
    struct ArrowSchema arrow_schema;
    memset(&arrow_schema, 0, sizeof(arrow_schema));
    int32_t rc = lance_c_schema_to_arrow(schema_handle, &arrow_schema);
    lance_c_free_schema(schema_handle);
    lance_c_close_dataset(dataset);

    if (rc < 0)
        ereport(ERROR,
            (errcode(ERRCODE_FDW_ERROR),
             errmsg("lance_import: schema export failed: %s", lance_get_error())));

    /* Build DDL */
    StringInfoData ddl;
    initStringInfo(&ddl);
    appendStringInfo(&ddl, "CREATE FOREIGN TABLE %s (\n", table_name);

    for (int64_t i = 0; i < arrow_schema.n_children; i++)
    {
        struct ArrowSchema *field = arrow_schema.children[i];
        if (field == NULL || field->name == NULL)
            continue;

        if (i > 0)
            appendStringInfoString(&ddl, ",\n");

        const char *pg_type = arrow_format_to_pg_type(field);
        appendStringInfo(&ddl, "    %s %s", field->name, pg_type);
    }

    appendStringInfo(&ddl, "\n) SERVER %s OPTIONS (uri '%s');", server_name, uri);

    /* Release Arrow schema */
    if (arrow_schema.release)
        arrow_schema.release(&arrow_schema);

    PG_RETURN_TEXT_P(cstring_to_text(ddl.data));
}

/*
 * ============================================================
 * FDW handler - returns FdwRoutine with callback pointers
 * ============================================================
 */

Datum
lance_fdw_handler(PG_FUNCTION_ARGS)
{
    FdwRoutine *fdwroutine = makeNode(FdwRoutine);

    /* Required scan callbacks */
    fdwroutine->GetForeignRelSize = lanceGetForeignRelSize;
    fdwroutine->GetForeignPaths = lanceGetForeignPaths;
    fdwroutine->GetForeignPlan = lanceGetForeignPlan;
    fdwroutine->BeginForeignScan = lanceBeginForeignScan;
    fdwroutine->IterateForeignScan = lanceIterateForeignScan;
    fdwroutine->ReScanForeignScan = lanceReScanForeignScan;
    fdwroutine->EndForeignScan = lanceEndForeignScan;

    /* Optional */
    fdwroutine->ExplainForeignScan = lanceExplainForeignScan;

    /* Write support - INSERT only */
    fdwroutine->IsForeignRelUpdatable = lanceIsForeignRelUpdatable;
    fdwroutine->PlanForeignModify = lancePlanForeignModify;
    fdwroutine->BeginForeignModify = lanceBeginForeignModify;
    fdwroutine->ExecForeignInsert = lanceExecForeignInsert;
    fdwroutine->EndForeignModify = lanceEndForeignModify;

    /* Not implemented */
    fdwroutine->PartitionTblProcess = NULL;
    fdwroutine->BuildRuntimePredicate = NULL;

    PG_RETURN_POINTER(fdwroutine);
}

/*
 * Validator for CREATE FOREIGN TABLE / CREATE SERVER options.
 */
Datum
lance_fdw_validator(PG_FUNCTION_ARGS)
{
    List *options_list = untransformRelOptions(PG_GETARG_DATUM(0));
    Oid catalog = PG_GETARG_OID(1);
    ListCell *cell;

    foreach (cell, options_list)
    {
        DefElem *def = (DefElem *) lfirst(cell);

        if (!is_lance_option(def->defname, catalog))
        {
            ereport(ERROR,
                (errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
                 errmsg("lance_fdw: invalid option \"%s\"", def->defname),
                 errhint("Valid options are: uri, batch_size")));
        }

        if (strcmp(def->defname, "batch_size") == 0)
        {
            int bs = atoi(defGetString(def));
            if (bs <= 0)
                ereport(ERROR,
                    (errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
                     errmsg("lance_fdw: batch_size must be a positive integer")));
        }
    }

    PG_RETURN_VOID();
}

/*
 * ============================================================
 * Planning callbacks
 * ============================================================
 */

/*
 * GetForeignRelSize: estimate the size of the foreign table.
 * Opens the dataset and counts rows via liblance_c.
 */
static void
lanceGetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid)
{
    LanceFdwPlanState *fdw_private;
    char *uri = NULL;
    int batch_size = LANCE_DEFAULT_BATCH_SIZE;
    void *dataset;
    int64_t nrows;

    lanceGetOptions(foreigntableid, &uri, &batch_size);

    fdw_private = (LanceFdwPlanState *) palloc0(sizeof(LanceFdwPlanState));
    fdw_private->uri = pstrdup(uri);
    fdw_private->batch_size = batch_size;

    /* Open dataset to count rows */
    elog(DEBUG1, "lance_fdw: GetForeignRelSize opening '%s'", uri);
    dataset = lance_c_open_dataset(uri);
    elog(DEBUG1, "lance_fdw: GetForeignRelSize dataset=%p", dataset);
    if (dataset == NULL)
    {
        ereport(ERROR,
            (errcode(ERRCODE_FDW_ERROR),
             errmsg("lance_fdw: failed to open dataset '%s': %s",
                    uri, lance_get_error())));
    }

    elog(DEBUG1, "lance_fdw: counting rows...");
    nrows = lance_c_dataset_count_rows(dataset);
    elog(DEBUG1, "lance_fdw: count_rows=%ld, closing dataset", (long)nrows);
    lance_c_close_dataset(dataset);
    elog(DEBUG1, "lance_fdw: dataset closed");

    if (nrows < 0)
    {
        ereport(WARNING,
            (errmsg("lance_fdw: could not count rows: %s", lance_get_error())));
        fdw_private->nrows = 1000;  /* fallback estimate */
    }
    else
    {
        fdw_private->nrows = (double) nrows;
    }


    baserel->rows = fdw_private->nrows;
    baserel->fdw_private = (void *) fdw_private;
}

/*
 * GetForeignPaths: create a single scan path.
 */
static void
lanceGetForeignPaths(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid)
{
    LanceFdwPlanState *fdw_private = (LanceFdwPlanState *) baserel->fdw_private;
    Cost startup_cost = 0;
    Cost total_cost;

    /* Simple cost model: 0.01 per row for columnar scan */
    total_cost = startup_cost + fdw_private->nrows * 0.01;
    elog(DEBUG1, "lance_fdw: GetForeignPaths creating path, cost=%.2f", total_cost);

    add_path(root, baserel,
        (Path *) create_foreignscan_path(root, baserel,
            startup_cost, total_cost,
            NIL,    /* no pathkeys */
            NULL,   /* no outer rel */
            NULL,   /* no outer path */
            NIL));  /* no fdw_private in path */
    elog(DEBUG1, "lance_fdw: GetForeignPaths done");
}

/*
 * GetForeignPlan: create the ForeignScan plan node.
 * Options are re-fetched from catalog in BeginForeignScan (not via fdw_private).
 */
static ForeignScan *
lanceGetForeignPlan(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid,
    ForeignPath *best_path, List *tlist, List *scan_clauses, Plan *outer_plan)
{
    Index scan_relid = baserel->relid;

    /*
     * All scan_clauses are passed to the executor as post-filters.
     * Filter pushdown happens separately in BeginForeignScan by
     * deparsing quals to SQL strings for Lance.
     */
    scan_clauses = extract_actual_clauses(scan_clauses, false);

    elog(DEBUG1, "lance_fdw: GetForeignPlan creating plan");
    /*
     * Pass NIL for fdw_private. The executor re-fetches options from
     * catalog in BeginForeignScan, avoiding plan serialization issues.
     */
    ForeignScan *fsplan = make_foreignscan(tlist,
        scan_clauses,
        scan_relid,
        NIL,                    /* no expressions to evaluate */
        NIL,                    /* no fdw_private - re-fetch in Begin */
        NIL,                    /* no fdw_scan_tlist */
        NIL,                    /* no fdw_recheck_quals */
        NULL,                   /* no outer_plan */
        EXEC_ON_DATANODES);
    elog(DEBUG1, "lance_fdw: GetForeignPlan done");
    return fsplan;
}

/*
 * ============================================================
 * Filter pushdown helpers
 * ============================================================
 */

/*
 * Try to deparse a Const node to a SQL literal string.
 * Returns palloc'd string or NULL if unsupported type.
 */
static char *
deparse_const(Const *node)
{
    if (node->constisnull)
        return pstrdup("NULL");

    Oid typoutput;
    bool typIsVarlena;
    getTypeOutputInfo(node->consttype, &typoutput, &typIsVarlena);
    char *val = OidOutputFunctionCall(typoutput, node->constvalue);

    /* Quote strings, leave numbers unquoted */
    switch (node->consttype)
    {
        case BOOLOID:
        case INT2OID:
        case INT4OID:
        case INT8OID:
        case FLOAT4OID:
        case FLOAT8OID:
        case NUMERICOID:
            return val;
        default:
        {
            /* Escape single quotes */
            StringInfoData buf;
            initStringInfo(&buf);
            appendStringInfoChar(&buf, '\'');
            for (char *p = val; *p; p++)
            {
                if (*p == '\'')
                    appendStringInfoChar(&buf, '\'');
                appendStringInfoChar(&buf, *p);
            }
            appendStringInfoChar(&buf, '\'');
            pfree(val);
            return buf.data;
        }
    }
}

/*
 * Try to deparse a simple OpExpr (col op const) to a SQL string.
 * Returns palloc'd string or NULL if not a pushable expression.
 */
static char *
deparse_opexpr(OpExpr *node, TupleDesc tupdesc, Index relid)
{
    /* Must have exactly 2 args */
    if (list_length(node->args) != 2)
        return NULL;

    Node *left = (Node *) linitial(node->args);
    Node *right = (Node *) lsecond(node->args);

    /* We support: Var op Const, or Const op Var */
    Var *var = NULL;
    Const *con = NULL;
    bool reversed = false;

    if (IsA(left, Var) && IsA(right, Const))
    {
        var = (Var *) left;
        con = (Const *) right;
    }
    else if (IsA(left, Const) && IsA(right, Var))
    {
        var = (Var *) right;
        con = (Const *) left;
        reversed = true;
    }
    else
        return NULL;

    /* Var must refer to our foreign table */
    if (var->varno != relid)
        return NULL;

    /* Get column name */
    int attnum = var->varattno;
    if (attnum <= 0 || attnum > tupdesc->natts)
        return NULL;
    Form_pg_attribute attr = &tupdesc->attrs[attnum - 1];
    if (attr->attisdropped)
        return NULL;
    const char *colname = NameStr(attr->attname);

    /* Get operator name */
    char *opname = get_opname(node->opno);
    if (opname == NULL)
        return NULL;

    /* Only push simple comparison operators */
    if (strcmp(opname, "=") != 0 && strcmp(opname, "<>") != 0 &&
        strcmp(opname, "<") != 0 && strcmp(opname, "<=") != 0 &&
        strcmp(opname, ">") != 0 && strcmp(opname, ">=") != 0)
        return NULL;

    /*
     * Skip cross-type comparisons (e.g., int4 const vs int8 column).
     * The operator may be int48gt etc., and the deparsed SQL string
     * would lose the type context, potentially producing wrong results
     * in Lance's SQL parser. Let openGauss post-filter handle these.
     */
    if (!con->constisnull && var->vartype != con->consttype)
        return NULL;

    char *constval = deparse_const(con);
    if (constval == NULL)
        return NULL;

    StringInfoData buf;
    initStringInfo(&buf);
    if (reversed)
        appendStringInfo(&buf, "%s %s %s", constval, opname, colname);
    else
        appendStringInfo(&buf, "%s %s %s", colname, opname, constval);

    pfree(constval);
    return buf.data;
}

/*
 * Try to deparse a NullTest (col IS NULL / IS NOT NULL).
 */
static char *
deparse_nulltest(NullTest *node, TupleDesc tupdesc, Index relid)
{
    if (!IsA(node->arg, Var))
        return NULL;
    Var *var = (Var *) node->arg;
    if (var->varno != relid)
        return NULL;

    int attnum = var->varattno;
    if (attnum <= 0 || attnum > tupdesc->natts)
        return NULL;
    Form_pg_attribute attr = &tupdesc->attrs[attnum - 1];
    const char *colname = NameStr(attr->attname);

    StringInfoData buf;
    initStringInfo(&buf);
    appendStringInfo(&buf, "%s %s",
        colname,
        node->nulltesttype == IS_NULL ? "IS NULL" : "IS NOT NULL");
    return buf.data;
}

/*
 * Try to deparse a ScalarArrayOpExpr (col IN (v1, v2, ...)) to SQL.
 */
static char *
deparse_scalar_array_opexpr(ScalarArrayOpExpr *node, TupleDesc tupdesc, Index relid)
{
    if (list_length(node->args) != 2)
        return NULL;

    Node *left = (Node *) linitial(node->args);
    Node *right = (Node *) lsecond(node->args);

    /* Left must be Var, right must be Const (array literal) */
    if (!IsA(left, Var) || !IsA(right, Const))
        return NULL;

    Var *var = (Var *) left;
    Const *con = (Const *) right;

    if (var->varno != relid)
        return NULL;

    int attnum = var->varattno;
    if (attnum <= 0 || attnum > tupdesc->natts)
        return NULL;
    Form_pg_attribute attr = &tupdesc->attrs[attnum - 1];
    if (attr->attisdropped)
        return NULL;
    const char *colname = NameStr(attr->attname);

    /* Get operator name (= for IN, <> for NOT IN) */
    char *opname = get_opname(node->opno);
    if (opname == NULL)
        return NULL;

    bool is_eq = (strcmp(opname, "=") == 0);
    bool is_neq = (strcmp(opname, "<>") == 0);
    if (!is_eq && !is_neq)
        return NULL;

    /* Deparse the array constant to text */
    if (con->constisnull)
        return NULL;

    Oid typoutput;
    bool typIsVarlena;
    getTypeOutputInfo(con->consttype, &typoutput, &typIsVarlena);
    char *arr_text = OidOutputFunctionCall(typoutput, con->constvalue);

    /* arr_text looks like "{1,2,3}" - convert to SQL IN list "(1,2,3)" */
    if (arr_text[0] != '{')
    {
        pfree(arr_text);
        return NULL;
    }

    StringInfoData buf;
    initStringInfo(&buf);
    if (node->useOr && is_eq)
        appendStringInfo(&buf, "%s IN (", colname);
    else if (!node->useOr && is_neq)
        appendStringInfo(&buf, "%s NOT IN (", colname);
    else
    {
        pfree(arr_text);
        pfree(buf.data);
        return NULL;
    }

    /* Convert {a,b,c} to a,b,c - handle quoting for non-numeric types */
    Oid elem_type = get_element_type(con->consttype);
    bool need_quotes = (elem_type != INT2OID && elem_type != INT4OID &&
                        elem_type != INT8OID && elem_type != FLOAT4OID &&
                        elem_type != FLOAT8OID && elem_type != NUMERICOID);

    char *p = arr_text + 1;  /* skip '{' */
    bool first = true;
    while (*p && *p != '}')
    {
        if (!first)
            appendStringInfoString(&buf, ", ");
        first = false;

        /* Extract one element */
        if (need_quotes)
            appendStringInfoChar(&buf, '\'');
        while (*p && *p != ',' && *p != '}')
        {
            if (*p == '\\' && *(p + 1))
                p++;  /* skip escape */
            appendStringInfoChar(&buf, *p++);
        }
        if (need_quotes)
            appendStringInfoChar(&buf, '\'');
        if (*p == ',')
            p++;
    }

    appendStringInfoChar(&buf, ')');
    pfree(arr_text);
    return buf.data;
}

/*
 * Try to deparse a boolean expression for filter pushdown.
 * Handles: OpExpr, BoolExpr (AND/OR/NOT), NullTest, ScalarArrayOpExpr (IN).
 * Returns palloc'd SQL string or NULL if not pushable.
 */
static char *
deparse_expr_for_pushdown(Node *node, TupleDesc tupdesc, Index relid)
{
    if (node == NULL)
        return NULL;

    if (IsA(node, OpExpr))
    {
        OpExpr *opexpr = (OpExpr *) node;
        /* Check for LIKE operator by name */
        char *opname = get_opname(opexpr->opno);
        if (opname && (strcmp(opname, "~~") == 0 || strcmp(opname, "!~~") == 0))
        {
            /* LIKE / NOT LIKE: deparse as col LIKE 'pattern' */
            if (list_length(opexpr->args) == 2 &&
                IsA(linitial(opexpr->args), Var) &&
                IsA(lsecond(opexpr->args), Const))
            {
                Var *var = (Var *) linitial(opexpr->args);
                Const *con = (Const *) lsecond(opexpr->args);
                if (var->varno == relid && var->varattno > 0 &&
                    var->varattno <= tupdesc->natts && !con->constisnull)
                {
                    Form_pg_attribute attr = &tupdesc->attrs[var->varattno - 1];
                    char *constval = deparse_const(con);
                    StringInfoData buf;
                    initStringInfo(&buf);
                    appendStringInfo(&buf, "%s %s %s",
                        NameStr(attr->attname),
                        strcmp(opname, "~~") == 0 ? "LIKE" : "NOT LIKE",
                        constval);
                    pfree(constval);
                    return buf.data;
                }
            }
        }
        return deparse_opexpr(opexpr, tupdesc, relid);
    }

    if (IsA(node, ScalarArrayOpExpr))
        return deparse_scalar_array_opexpr((ScalarArrayOpExpr *) node, tupdesc, relid);

    if (IsA(node, NullTest))
        return deparse_nulltest((NullTest *) node, tupdesc, relid);

    if (IsA(node, BoolExpr))
    {
        BoolExpr *bexpr = (BoolExpr *) node;
        if (bexpr->boolop == AND_EXPR || bexpr->boolop == OR_EXPR)
        {
            StringInfoData buf;
            initStringInfo(&buf);
            const char *op = (bexpr->boolop == AND_EXPR) ? " AND " : " OR ";
            bool first = true;
            ListCell *lc;

            appendStringInfoChar(&buf, '(');
            foreach (lc, bexpr->args)
            {
                char *sub = deparse_expr_for_pushdown((Node *) lfirst(lc),
                                                       tupdesc, relid);
                if (sub == NULL)
                {
                    pfree(buf.data);
                    return NULL;  /* If any part not pushable, don't push any */
                }
                if (!first)
                    appendStringInfoString(&buf, op);
                appendStringInfoString(&buf, sub);
                pfree(sub);
                first = false;
            }
            appendStringInfoChar(&buf, ')');
            return buf.data;
        }
        else if (bexpr->boolop == NOT_EXPR)
        {
            char *sub = deparse_expr_for_pushdown((Node *) linitial(bexpr->args),
                                                   tupdesc, relid);
            if (sub == NULL)
                return NULL;
            StringInfoData buf;
            initStringInfo(&buf);
            appendStringInfo(&buf, "NOT (%s)", sub);
            pfree(sub);
            return buf.data;
        }
    }

    return NULL;  /* Unsupported expression type */
}

/*
 * Build a filter SQL string from the plan's qual list.
 * Only pushes down clauses that we can fully handle in Lance.
 * Non-pushable clauses are left for openGauss to evaluate post-scan.
 *
 * Returns palloc'd SQL string or NULL if no pushable filters.
 */
static char *
lance_build_filter_sql(List *quals, TupleDesc tupdesc, Index relid)
{
    StringInfoData buf;
    bool has_filter = false;
    ListCell *lc;

    initStringInfo(&buf);

    foreach (lc, quals)
    {
        Node *qual = (Node *) lfirst(lc);
        char *sql = deparse_expr_for_pushdown(qual, tupdesc, relid);
        if (sql != NULL)
        {
            if (has_filter)
                appendStringInfoString(&buf, " AND ");
            appendStringInfoString(&buf, sql);
            pfree(sql);
            has_filter = true;
        }
        /* Non-pushable quals are silently left for post-filtering */
    }

    if (!has_filter)
    {
        pfree(buf.data);
        return NULL;
    }
    return buf.data;
}

/*
 * ============================================================
 * Execution callbacks
 * ============================================================
 */

/*
 * Helper: create a scan stream with filter pushdown and fallback.
 * If filter pushdown fails, retries without the filter.
 */
static void *
lance_open_stream(void *dataset, const char **col_names, int ncols,
                  const char *filter_sql)
{
    void *stream = lance_c_create_scan_stream(dataset, col_names, ncols,
                                               filter_sql, -1, 0);
    if (stream == NULL && filter_sql != NULL)
    {
        elog(DEBUG1, "lance_fdw: filter pushdown failed, retrying without filter");
        stream = lance_c_create_scan_stream(dataset, col_names, ncols,
                                             NULL, -1, 0);
    }
    return stream;
}

/*
 * Helper: release current Arrow batch resources.
 */
static void
lance_release_batch(LanceFdwScanState *festate)
{
    if (festate->has_batch)
    {
        arrow_release_array(&festate->current_array);
        arrow_release_schema(&festate->current_schema);
        festate->has_batch = 0;
        festate->batch_rows = 0;
        festate->current_row = 0;
    }
}

/*
 * Helper: fetch the next Arrow batch from the stream.
 * Returns true if a batch was fetched, false if end-of-stream.
 */
static bool
lance_fetch_next_batch(LanceFdwScanState *festate)
{
    void *batch_handle = NULL;
    int32_t rc;

    /* Release previous batch if any */
    lance_release_batch(festate);

    elog(DEBUG1, "lance_fdw: stream_next calling...");
    rc = lance_c_stream_next(festate->stream, &batch_handle);
    elog(DEBUG1, "lance_fdw: stream_next returned %d, batch=%p", rc, batch_handle);
    if (rc == 1)
        return false;   /* end of stream */
    if (rc < 0)
    {
        ereport(ERROR,
            (errcode(ERRCODE_FDW_ERROR),
             errmsg("lance_fdw: stream_next failed: %s", lance_get_error())));
    }

    /* Export to Arrow C Data Interface */
    memset(&festate->current_array, 0, sizeof(festate->current_array));
    memset(&festate->current_schema, 0, sizeof(festate->current_schema));

    elog(DEBUG1, "lance_fdw: batch_to_arrow calling...");
    rc = lance_c_batch_to_arrow(batch_handle,
                                &festate->current_array,
                                &festate->current_schema);
    elog(DEBUG1, "lance_fdw: batch_to_arrow returned %d", rc);
    /* Free the Rust batch handle - the data is now owned by the Arrow C structs */
    lance_c_free_batch(batch_handle);

    if (rc < 0)
    {
        ereport(ERROR,
            (errcode(ERRCODE_FDW_ERROR),
             errmsg("lance_fdw: batch_to_arrow failed: %s", lance_get_error())));
    }

    festate->has_batch = 1;
    festate->batch_rows = festate->current_array.length;
    festate->current_row = 0;
    festate->batch_count++;
    return true;
}

/*
 * Build the mapping from PG attribute index to Arrow child index.
 * Matches by column name (case-sensitive).
 */
static void
lance_build_column_mapping(LanceFdwScanState *festate, TupleDesc tupdesc)
{
    int natts = tupdesc->natts;
    int i;

    festate->natts = natts;
    festate->att_to_arrow = (int *) palloc(sizeof(int) * natts);

    for (i = 0; i < natts; i++)
    {
        Form_pg_attribute attr = &tupdesc->attrs[i];
        int j;

        festate->att_to_arrow[i] = -1;     /* default: not mapped */

        if (attr->attisdropped)
            continue;

        /* Search Arrow schema children for matching name */
        if (festate->current_schema.children == NULL)
            continue;
        for (j = 0; j < festate->current_schema.n_children; j++)
        {
            if (festate->current_schema.children[j] != NULL &&
                festate->current_schema.children[j]->name != NULL &&
                strcmp(NameStr(attr->attname),
                       festate->current_schema.children[j]->name) == 0)
            {
                festate->att_to_arrow[i] = j;
                break;
            }
        }
    }
}

/*
 * BeginForeignScan: open the dataset and create a scan stream.
 */
static void
lanceBeginForeignScan(ForeignScanState *node, int eflags)
{
    elog(DEBUG1, "lance_fdw: BeginForeignScan ENTER eflags=%d", eflags);
    LanceFdwScanState *festate;
    char *uri = NULL;
    int batch_size = LANCE_DEFAULT_BATCH_SIZE;
    TupleDesc tupdesc;
    int natts, i;

    /* Do nothing for EXPLAIN without ANALYZE */
    if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
        return;

    /* Deserialize options from fdw_private */
    /*
     * Re-fetch options directly from catalog instead of fdw_private.
     * This is simpler and avoids plan serialization issues with openGauss.
     */
    elog(DEBUG1, "lance_fdw: BeginForeignScan fetching options from catalog");
    {
        Oid foreigntableid = RelationGetRelid(node->ss.ss_currentRelation);
        lanceGetOptions(foreigntableid, &uri, &batch_size);
    }
    elog(DEBUG1, "lance_fdw: BeginForeignScan uri='%s' batch_size=%d", uri, batch_size);

    /* Allocate execution state */
    festate = (LanceFdwScanState *) palloc0(sizeof(LanceFdwScanState));
    festate->uri = pstrdup(uri);
    festate->batch_size = batch_size;

    /* Open dataset */
    elog(DEBUG1, "lance_fdw: BeginForeignScan opening dataset '%s'", uri);
    festate->dataset = lance_c_open_dataset(uri);
    elog(DEBUG1, "lance_fdw: BeginForeignScan dataset=%p", festate->dataset);
    if (festate->dataset == NULL)
    {
        ereport(ERROR,
            (errcode(ERRCODE_FDW_ERROR),
             errmsg("lance_fdw: failed to open dataset '%s': %s",
                    uri, lance_get_error())));
    }

    /*
     * Build column projection from the target list.
     * We only request columns that the query actually needs.
     */
    tupdesc = node->ss.ss_currentRelation
              ? RelationGetDescr(node->ss.ss_currentRelation)
              : node->ss.ss_ScanTupleSlot->tts_tupleDescriptor;
    natts = tupdesc->natts;

    {
        const char **col_names = NULL;
        int ncols = 0;

        col_names = (const char **) palloc(sizeof(const char *) * natts);
        for (i = 0; i < natts; i++)
        {
            Form_pg_attribute attr = &tupdesc->attrs[i];
            if (!attr->attisdropped)
                col_names[ncols++] = NameStr(attr->attname);
        }

        /*
         * Build filter SQL from plan quals for pushdown.
         * Only simple predicates (col op const, IS NULL, AND/OR/NOT)
         * are pushed down. Complex expressions remain as post-filters.
         */
        ForeignScan *fsplan = (ForeignScan *) node->ss.ps.plan;
        TupleDesc rel_tupdesc = RelationGetDescr(node->ss.ss_currentRelation);
        char *filter_sql = lance_build_filter_sql(
            fsplan->scan.plan.qual, rel_tupdesc, fsplan->scan.scanrelid);
        /* Save filter for possible rescan */
        festate->filter_sql = filter_sql ? pstrdup(filter_sql) : NULL;

        elog(DEBUG1, "lance_fdw: creating scan stream with %d columns, filter=%s",
             ncols, filter_sql ? filter_sql : "(none)");

        festate->stream = lance_open_stream(
            festate->dataset, col_names, ncols, filter_sql);

        if (filter_sql)
            pfree(filter_sql);
        pfree(col_names);
    }

    if (festate->stream == NULL)
    {
        lance_c_close_dataset(festate->dataset);
        festate->dataset = NULL;
        ereport(ERROR,
            (errcode(ERRCODE_FDW_ERROR),
             errmsg("lance_fdw: failed to create scan stream: %s",
                    lance_get_error())));
    }

    node->fdw_state = (void *) festate;

    /* Fetch the first batch to build column mapping */
    if (lance_fetch_next_batch(festate))
    {
        lance_build_column_mapping(festate, tupdesc);
    }
}

/*
 * IterateForeignScan: return the next tuple from the Lance dataset.
 *
 * Reads Arrow batches and converts them row-by-row to PG tuples.
 */
static TupleTableSlot *
lanceIterateForeignScan(ForeignScanState *node)
{
    LanceFdwScanState *festate = (LanceFdwScanState *) node->fdw_state;
    TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;
    TupleDesc tupdesc = slot->tts_tupleDescriptor;
    int i;

    ExecClearTuple(slot);

    /* Check if we need a new batch */
    while (festate->has_batch && festate->current_row >= festate->batch_rows)
    {
        if (!lance_fetch_next_batch(festate))
            return slot;    /* end of data - return empty slot */

        /* Rebuild column mapping for new batch (schema may differ) */
        lance_build_column_mapping(festate, tupdesc);
    }

    if (!festate->has_batch)
        return slot;    /* no data at all */

    /* Validate Arrow data structure */
    if (festate->current_array.children == NULL && festate->natts > 0)
    {
        elog(DEBUG1, "lance_fdw: Arrow batch has no children arrays");
        return slot;
    }

    /* Convert current row from Arrow to PG Datums */
    for (i = 0; i < festate->natts; i++)
    {
        int arrow_idx = festate->att_to_arrow[i];
        Form_pg_attribute attr = &tupdesc->attrs[i];

        if (attr->attisdropped || arrow_idx < 0)
        {
            slot->tts_isnull[i] = true;
            slot->tts_values[i] = (Datum) 0;
            continue;
        }

        if (arrow_idx >= festate->current_array.n_children ||
            festate->current_array.children[arrow_idx] == NULL)
        {
            slot->tts_isnull[i] = true;
            slot->tts_values[i] = (Datum) 0;
            continue;
        }

        slot->tts_values[i] = arrow_value_to_datum(
            festate->current_array.children[arrow_idx],
            festate->current_schema.children[arrow_idx],
            festate->current_row,
            attr->atttypid,
            &slot->tts_isnull[i]);
    }

    festate->current_row++;
    festate->total_rows_returned++;
    ExecStoreVirtualTuple(slot);
    return slot;
}

/*
 * ReScanForeignScan: restart the scan from the beginning.
 */
static void
lanceReScanForeignScan(ForeignScanState *node)
{
    LanceFdwScanState *festate = (LanceFdwScanState *) node->fdw_state;
    TupleDesc tupdesc;
    int natts, i;

    if (festate == NULL)
        return;

    /* Release current batch */
    lance_release_batch(festate);

    /* Close old stream */
    if (festate->stream)
    {
        lance_c_close_stream(festate->stream);
        festate->stream = NULL;
    }

    /* Create a new stream */
    tupdesc = node->ss.ss_ScanTupleSlot->tts_tupleDescriptor;
    natts = tupdesc->natts;

    {
        const char **col_names = (const char **) palloc(sizeof(const char *) * natts);
        int ncols = 0;

        for (i = 0; i < natts; i++)
        {
            Form_pg_attribute attr = &tupdesc->attrs[i];
            if (!attr->attisdropped)
                col_names[ncols++] = NameStr(attr->attname);
        }

        festate->stream = lance_open_stream(
            festate->dataset, col_names, ncols, festate->filter_sql);

        pfree(col_names);
    }

    if (festate->stream == NULL)
    {
        ereport(ERROR,
            (errcode(ERRCODE_FDW_ERROR),
             errmsg("lance_fdw: rescan failed to create stream: %s",
                    lance_get_error())));
    }

    /* Fetch first batch */
    if (lance_fetch_next_batch(festate))
    {
        lance_build_column_mapping(festate, tupdesc);
    }
}

/*
 * EndForeignScan: cleanup all resources.
 */
static void
lanceEndForeignScan(ForeignScanState *node)
{
    LanceFdwScanState *festate = (LanceFdwScanState *) node->fdw_state;

    if (festate == NULL)
        return;

    lance_release_batch(festate);

    if (festate->stream)
    {
        lance_c_close_stream(festate->stream);
        festate->stream = NULL;
    }

    if (festate->dataset)
    {
        lance_c_close_dataset(festate->dataset);
        festate->dataset = NULL;
    }
}

/*
 * ============================================================
 * Write path callbacks (INSERT)
 * ============================================================
 */

static int
lanceIsForeignRelUpdatable(Relation rel)
{
    return (1 << CMD_INSERT);
}

static List *
lancePlanForeignModify(PlannerInfo *root, ModifyTable *plan,
    Index resultRelation, int subplan_index)
{
    /*
     * openGauss requires non-NULL fdw_private for DML on foreign tables.
     * Return a dummy list; actual options are re-fetched in BeginForeignModify.
     */
    return list_make1(makeInteger(0));
}

static void
lanceBeginForeignModify(ModifyTableState *mtstate,
    ResultRelInfo *rinfo, List *fdw_private, int subplan_index, int eflags)
{
    if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
        return;

    Relation rel = rinfo->ri_RelationDesc;
    TupleDesc tupdesc = RelationGetDescr(rel);
    char *uri = NULL;
    int batch_size;
    lanceGetOptions(RelationGetRelid(rel), &uri, &batch_size);

    LanceFdwModifyState *fmstate = (LanceFdwModifyState *) palloc0(sizeof(LanceFdwModifyState));
    fmstate->uri = pstrdup(uri);
    fmstate->tupdesc = tupdesc;
    fmstate->nrows = 0;
    fmstate->max_rows = 10000;  /* initial buffer */
    fmstate->row_values = (Datum **) palloc(sizeof(Datum *) * fmstate->max_rows);
    fmstate->row_nulls = (bool **) palloc(sizeof(bool *) * fmstate->max_rows);

    rinfo->ri_FdwState = fmstate;
}

static TupleTableSlot *
lanceExecForeignInsert(EState *estate, ResultRelInfo *rinfo,
    TupleTableSlot *slot, TupleTableSlot *planSlot)
{
    LanceFdwModifyState *fmstate = (LanceFdwModifyState *) rinfo->ri_FdwState;
    TupleDesc tupdesc = fmstate->tupdesc;
    int natts = tupdesc->natts;

    /* Grow buffer if needed */
    if (fmstate->nrows >= fmstate->max_rows)
    {
        fmstate->max_rows *= 2;
        fmstate->row_values = (Datum **) repalloc(fmstate->row_values,
            sizeof(Datum *) * fmstate->max_rows);
        fmstate->row_nulls = (bool **) repalloc(fmstate->row_nulls,
            sizeof(bool *) * fmstate->max_rows);
    }

    /* Copy slot values */
    /* Ensure all attrs are available. The executor should have already
     * materialized the slot, but call heap_slot_getallattrs to be safe. */
    heap_slot_getallattrs(slot);
    Datum *vals = (Datum *) palloc(sizeof(Datum) * natts);
    bool *nulls = (bool *) palloc(sizeof(bool) * natts);
    for (int i = 0; i < natts; i++)
    {
        if (slot->tts_isnull[i])
        {
            vals[i] = (Datum) 0;
            nulls[i] = true;
        }
        else
        {
            /* Deep copy pass-by-reference types */
            Form_pg_attribute attr = &tupdesc->attrs[i];
            if (attr->attlen == -1)  /* varlena */
                vals[i] = PointerGetDatum(PG_DETOAST_DATUM_COPY(slot->tts_values[i]));
            else
                vals[i] = slot->tts_values[i];
            nulls[i] = false;
        }
    }

    fmstate->row_values[fmstate->nrows] = vals;
    fmstate->row_nulls[fmstate->nrows] = nulls;
    fmstate->nrows++;

    return slot;
}

/*
 * Helper: build an Arrow C Data Interface schema from PG TupleDesc.
 * Only supports basic types (int8, text, float8, etc.).
 */
static const char *
pg_type_to_arrow_format(Oid typid)
{
    switch (typid)
    {
        case BOOLOID:       return "b";
        case INT2OID:       return "s";
        case INT4OID:       return "i";
        case INT8OID:       return "l";
        case FLOAT4OID:     return "f";
        case FLOAT8OID:     return "g";
        case TEXTOID:       return "u";
        case VARCHAROID:    return "u";
        case BYTEAOID:      return "z";
        case DATEOID:       return "tdD";
        case TIMESTAMPOID:  return "tsu:";
        case NUMERICOID:    return "g";  /* convert numeric to float8 for simplicity */
        default:            return "u";  /* fallback to utf8 text */
    }
}

/*
 * Map PG type OID to our simplified type char for lance_c_append_columns.
 */
static char
pg_type_to_col_type_char(Oid typid)
{
    switch (typid)
    {
        case INT8OID:       return 'l';
        case INT4OID:       return 'i';
        case INT2OID:       return 'i';  /* promote to int32 */
        case FLOAT8OID:     return 'g';
        case FLOAT4OID:     return 'f';
        case BOOLOID:       return 't';
        case TEXTOID:
        case VARCHAROID:
        case NAMEOID:       return 'u';
        case NUMERICOID:    return 'g';  /* convert to float64 */
        default:            return 'u';  /* fallback: text output */
    }
}

static void
lanceEndForeignModify(EState *estate, ResultRelInfo *rinfo)
{
    LanceFdwModifyState *fmstate = (LanceFdwModifyState *) rinfo->ri_FdwState;
    if (fmstate == NULL || fmstate->nrows == 0)
        return;

    TupleDesc tupdesc = fmstate->tupdesc;
    int natts = tupdesc->natts;
    int nrows = fmstate->nrows;

    /* Build column-oriented arrays for lance_c_append_columns */
    const char **col_names = (const char **) palloc(sizeof(char *) * natts);
    char *col_types = (char *) palloc(natts + 1);
    void **col_data = (void **) palloc(sizeof(void *) * natts);
    bool **col_nulls = (bool **) palloc(sizeof(bool *) * natts);

    for (int c = 0; c < natts; c++)
    {
        Form_pg_attribute attr = &tupdesc->attrs[c];
        col_names[c] = NameStr(attr->attname);
        char tc = pg_type_to_col_type_char(attr->atttypid);
        col_types[c] = tc;

        /* Allocate column data array */
        bool *nulls = (bool *) palloc0(sizeof(bool) * nrows);
        col_nulls[c] = nulls;

        switch (tc)
        {
            case 'l':
            {
                int64_t *vals = (int64_t *) palloc(sizeof(int64_t) * nrows);
                for (int r = 0; r < nrows; r++)
                {
                    nulls[r] = fmstate->row_nulls[r][c];
                    if (!nulls[r])
                    {
                        if (attr->atttypid == INT2OID)
                            vals[r] = (int64_t) DatumGetInt16(fmstate->row_values[r][c]);
                        else
                            vals[r] = DatumGetInt64(fmstate->row_values[r][c]);
                    }
                    else
                        vals[r] = 0;
                }
                col_data[c] = vals;
                break;
            }
            case 'i':
            {
                int32_t *vals = (int32_t *) palloc(sizeof(int32_t) * nrows);
                for (int r = 0; r < nrows; r++)
                {
                    nulls[r] = fmstate->row_nulls[r][c];
                    if (!nulls[r])
                    {
                        if (attr->atttypid == INT2OID)
                            vals[r] = (int32_t) DatumGetInt16(fmstate->row_values[r][c]);
                        else
                            vals[r] = DatumGetInt32(fmstate->row_values[r][c]);
                    }
                    else
                        vals[r] = 0;
                }
                col_data[c] = vals;
                break;
            }
            case 'g':
            {
                double *vals = (double *) palloc(sizeof(double) * nrows);
                for (int r = 0; r < nrows; r++)
                {
                    nulls[r] = fmstate->row_nulls[r][c];
                    if (!nulls[r])
                    {
                        if (attr->atttypid == NUMERICOID)
                        {
                            Datum d = DirectFunctionCall1(numeric_float8, fmstate->row_values[r][c]);
                            vals[r] = DatumGetFloat8(d);
                        }
                        else
                            vals[r] = DatumGetFloat8(fmstate->row_values[r][c]);
                    }
                    else
                        vals[r] = 0.0;
                }
                col_data[c] = vals;
                break;
            }
            case 'f':
            {
                float *vals = (float *) palloc(sizeof(float) * nrows);
                for (int r = 0; r < nrows; r++)
                {
                    nulls[r] = fmstate->row_nulls[r][c];
                    vals[r] = nulls[r] ? 0.0f : DatumGetFloat4(fmstate->row_values[r][c]);
                }
                col_data[c] = vals;
                break;
            }
            case 't':
            {
                bool *vals = (bool *) palloc(sizeof(bool) * nrows);
                for (int r = 0; r < nrows; r++)
                {
                    nulls[r] = fmstate->row_nulls[r][c];
                    vals[r] = nulls[r] ? false : DatumGetBool(fmstate->row_values[r][c]);
                }
                col_data[c] = vals;
                break;
            }
            case 'u':
            default:
            {
                /* Text: convert datum to C string */
                const char **vals = (const char **) palloc(sizeof(char *) * nrows);
                for (int r = 0; r < nrows; r++)
                {
                    nulls[r] = fmstate->row_nulls[r][c];
                    if (!nulls[r])
                    {
                        Oid typoutput;
                        bool typIsVarlena;
                        getTypeOutputInfo(attr->atttypid, &typoutput, &typIsVarlena);
                        vals[r] = OidOutputFunctionCall(typoutput, fmstate->row_values[r][c]);
                    }
                    else
                        vals[r] = NULL;
                }
                col_data[c] = vals;
                break;
            }
        }
    }
    col_types[natts] = '\0';

    /* Call Rust to write */
    int32_t rc = lance_c_append_columns(
        fmstate->uri,
        col_names,
        col_types,
        natts,
        nrows,
        (const void **) col_data,
        (const bool **) col_nulls);

    if (rc < 0)
        ereport(ERROR,
            (errcode(ERRCODE_FDW_ERROR),
             errmsg("lance_fdw: write failed: %s", lance_get_error())));

    elog(NOTICE, "lance_fdw: inserted %d rows into '%s'", nrows, fmstate->uri);
}

/*
 * ExplainForeignScan: add extra info to EXPLAIN output.
 */
static void
lanceExplainForeignScan(ForeignScanState *node, ExplainState *es)
{
    char *uri = NULL;
    int batch_size;

    lanceGetOptions(RelationGetRelid(node->ss.ss_currentRelation), &uri, &batch_size);
    ExplainPropertyText("Lance URI", uri, es);
}
