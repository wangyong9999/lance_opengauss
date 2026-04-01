/*
 * lance_fdw.h
 *
 * Internal header for lance_fdw - Lance Foreign Data Wrapper for openGauss.
 */

#ifndef LANCE_FDW_H
#define LANCE_FDW_H

#include "arrow_c_data.h"
#include "lance_c_api.h"

/*
 * FDW plan state - stored in baserel->fdw_private during planning.
 */
typedef struct LanceFdwPlanState
{
    char       *uri;            /* Lance dataset URI */
    int         batch_size;     /* Arrow batch size (default 1024) */
    double      nrows;          /* Estimated row count */
} LanceFdwPlanState;

/*
 * FDW execution state - stored in node->fdw_state during scan.
 */
typedef struct LanceFdwScanState
{
    /* Dataset and stream handles from liblance_c */
    void       *dataset;        /* Dataset handle */
    void       *stream;         /* Stream handle */

    /* Current Arrow batch */
    struct ArrowArray    current_array;      /* Arrow C Data array */
    struct ArrowSchema   current_schema;     /* Arrow C Data schema */
    int                  has_batch;          /* 1 if current batch is valid */
    int64_t              batch_rows;         /* Number of rows in current batch */
    int64_t              current_row;        /* Current row index in batch */

    /* Column mapping: PG attribute index -> Arrow child index */
    int        *att_to_arrow;   /* Array of size natts, -1 if not mapped */
    int         natts;          /* Number of PG attributes */

    /* Debug counters */
    int64_t     total_rows_returned;
    int         batch_count;

    /* Options */
    char       *uri;
    char       *filter_sql;     /* Pushed-down filter SQL (palloc'd, may be NULL) */
    int         batch_size;
} LanceFdwScanState;

/*
 * Arrow-to-PG type conversion functions (arrow_to_pg.c)
 */

/*
 * Extract one value from an Arrow column at the given row and convert it
 * to an openGauss Datum.
 *
 * arrow_array:  the Arrow child array for the column
 * arrow_schema: the Arrow schema for the column
 * row:          row index within the batch
 * isnull:       output - set to true if the value is NULL
 * pg_type_oid:  the expected PG type OID for validation
 *
 * Returns the Datum value. If *isnull is true, the returned Datum is undefined.
 */

#endif /* LANCE_FDW_H */
