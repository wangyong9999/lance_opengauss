/*
 * lance_c_api.h
 *
 * C API declarations for liblance_c - a database-agnostic C interface
 * to the Lance columnar data format.
 *
 * All functions use opaque void* handles for resource management.
 * Errors are reported via thread-local state (lance_c_last_error_*).
 */

#ifndef LANCE_C_API_H
#define LANCE_C_API_H

#include <stddef.h>
#include <stdint.h>

#include "arrow_c_data.h"

#ifdef __cplusplus
extern "C" {
#endif

/*
 * ============================================================
 * Dataset lifecycle
 * ============================================================
 */

/* Open a Lance dataset at the given URI (local path or remote). */
void *lance_c_open_dataset(const char *uri);

/* Open a dataset with storage options (e.g., S3 credentials). */
void *lance_c_open_dataset_with_options(
    const char *uri,
    const char **option_keys,
    const char **option_values,
    size_t options_len);

/* Close and free a dataset handle. */
void lance_c_close_dataset(void *dataset);

/* Count total rows in the dataset. Returns -1 on error. */
int64_t lance_c_dataset_count_rows(void *dataset);

/*
 * ============================================================
 * Schema
 * ============================================================
 */

/* Get the schema handle for a dataset. Returns NULL on error. */
void *lance_c_get_schema(void *dataset);

/* Export a schema handle to an Arrow C Data Interface ArrowSchema. */
int32_t lance_c_schema_to_arrow(void *schema, struct ArrowSchema *out_schema);

/* Free a schema handle. */
void lance_c_free_schema(void *schema);

/*
 * ============================================================
 * Scan / Stream
 * ============================================================
 */

/*
 * Create a scan stream over the dataset.
 *
 * columns/columns_len: column names for projection (NULL/0 = all columns)
 * filter_sql:          SQL predicate string for pushdown (NULL = no filter)
 * limit:               max rows to return (-1 = unlimited)
 * offset:              rows to skip (0 = none)
 *
 * Returns a stream handle, or NULL on error.
 */
void *lance_c_create_scan_stream(
    void *dataset,
    const char **columns,
    size_t columns_len,
    const char *filter_sql,
    int64_t limit,
    int64_t offset);

/*
 * Get the next RecordBatch from a stream.
 *
 * Returns:
 *   0  - batch available (*out_batch is set)
 *   1  - end of stream
 *  -1  - error (check lance_c_last_error_message)
 */
int32_t lance_c_stream_next(void *stream, void **out_batch);

/* Close and free a stream handle. */
void lance_c_close_stream(void *stream);

/*
 * ============================================================
 * Batch export (Arrow C Data Interface)
 * ============================================================
 */

/*
 * Export a RecordBatch to Arrow C Data Interface structs.
 * Returns 0 on success, -1 on error.
 */
int32_t lance_c_batch_to_arrow(
    void *batch,
    struct ArrowArray *out_array,
    struct ArrowSchema *out_schema);

/* Free a batch handle. */
void lance_c_free_batch(void *batch);

/*
 * ============================================================
 * Vector search
 * ============================================================
 */

/*
 * Create a KNN search stream.
 * Returns a stream handle (use lance_c_stream_next to iterate results).
 * Results include a "_distance" column.
 */
void *lance_c_knn_search(
    const char *uri,
    const char *vector_column,
    const float *query,
    size_t query_len,
    size_t k,
    const char *metric);  /* "l2", "cosine", "dot" */

/*
 * ============================================================
 * Write operations
 * ============================================================
 */

/*
 * Append a batch of rows to a Lance dataset.
 * Creates the dataset if it doesn't exist.
 * Takes ownership of the ArrowArray/ArrowSchema (releases them).
 * Returns 0 on success, -1 on error.
 */
int32_t lance_c_append_batch(
    const char *uri,
    struct ArrowArray *array,
    struct ArrowSchema *schema);

/*
 * Append rows given as column-oriented typed arrays.
 * col_types: 'l'=int64, 'g'=float64, 'u'=utf8, 'f'=float32, 'i'=int32, 't'=bool
 */
int32_t lance_c_append_columns(
    const char *uri,
    const char **col_names,
    const char *col_types,
    size_t ncols,
    size_t nrows,
    const void **col_data,
    const bool **col_nulls);

/*
 * ============================================================
 * Error handling (thread-local)
 * ============================================================
 */

/* Get the last error code (0 = no error). */
int32_t lance_c_last_error_code(void);

/*
 * Get the last error message. Returns NULL if no error.
 * The returned string must be freed with lance_c_free_string().
 */
const char *lance_c_last_error_message(void);

/* Free a string returned by lance_c_last_error_message(). */
void lance_c_free_string(const char *s);

#ifdef __cplusplus
}
#endif

#endif /* LANCE_C_API_H */
