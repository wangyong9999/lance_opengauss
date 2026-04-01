# C++ FDW Extension (lance_fdw)

openGauss Foreign Data Wrapper implementation. Translates between openGauss's executor callbacks and the Rust FFI layer (`liblance_c`).

## File Overview

| File | Lines | Purpose |
|------|-------|---------|
| `lance_fdw.cpp` | ~1600 | FDW callbacks, filter deparse, write path, `lance_import()` |
| `arrow_to_pg.cpp` | ~760 | Arrow C Data Interface → openGauss Datum conversion |
| `lance_fdw.h` | ~70 | State structs (`LanceFdwPlanState`, `LanceFdwScanState`) |
| `lance_c_api.h` | ~130 | C declarations for all `liblance_c` FFI functions |
| `arrow_c_data.h` | ~60 | Vendored Arrow C Data Interface struct definitions |

## lance_fdw.cpp — FDW Callbacks

### Execution Flow

```
SELECT * FROM lance_table WHERE id > 10 LIMIT 5;

┌─ Planning Phase ──────────────────────────────────────────┐
│                                                           │
│  GetForeignRelSize()                                      │
│    → lance_c_open_dataset(uri)                            │
│    → lance_c_dataset_count_rows()  →  baserel->rows       │
│    → lance_c_close_dataset()                              │
│                                                           │
│  GetForeignPaths()                                        │
│    → create_foreignscan_path(cost = rows * 0.01)          │
│                                                           │
│  GetForeignPlan()                                         │
│    → extract_actual_clauses(scan_clauses)                 │
│    → make_foreignscan(quals as post-filter)               │
│                                                           │
└───────────────────────────────────────────────────────────┘

┌─ Execution Phase ─────────────────────────────────────────┐
│                                                           │
│  BeginForeignScan()                                       │
│    → lanceGetOptions(foreigntableid)   [from catalog]     │
│    → lance_c_open_dataset(uri)                            │
│    → lance_build_filter_sql(quals)     [deparse WHERE]    │
│    → lance_open_stream(ds, cols, filter)                  │
│    → lance_fetch_next_batch()          [prefetch batch 1] │
│    → lance_build_column_mapping()                         │
│                                                           │
│  IterateForeignScan()  ← called per row                   │
│    → if current_row >= batch_rows:                        │
│        lance_fetch_next_batch()                           │
│    → for each column:                                     │
│        arrow_value_to_datum(children[i], row, type_oid)   │
│    → ExecStoreVirtualTuple(slot)                          │
│                                                           │
│  EndForeignScan()                                         │
│    → lance_release_batch()                                │
│    → lance_c_close_stream()                               │
│    → lance_c_close_dataset()                              │
│                                                           │
└───────────────────────────────────────────────────────────┘
```

### Filter Pushdown Pipeline

```
openGauss qual list (Node*)
    │
    ▼
deparse_expr_for_pushdown()
    ├── OpExpr (col op const)   → deparse_opexpr()       → "id > 10"
    ├── NullTest                → deparse_nulltest()      → "name IS NOT NULL"
    ├── BoolExpr (AND/OR/NOT)   → recurse + combine       → "(a > 1 AND b < 5)"
    ├── ScalarArrayOpExpr (IN)  → deparse_scalar_array()  → "id IN (1, 2, 3)"
    └── OpExpr (LIKE ~~)        → special case             → "name LIKE 'al%'"
    │
    ▼
lance_build_filter_sql()  →  "id > 10 AND name IS NOT NULL"
    │
    ▼
lance_c_create_scan_stream(ds, cols, filter_sql, limit, offset)
    │
    ▼  if fails
lance_open_stream() retries without filter  →  post-filter by openGauss
```

**Safety rules:**
- Cross-type comparisons (e.g., int4 constant vs int8 column) are never pushed down
- Only `=, <>, <, <=, >, >=` operators are pushed; others are left for post-filtering
- String constants are single-quote escaped to prevent SQL injection

### Write Path

```
INSERT INTO lance_table SELECT * FROM normal_table;

BeginForeignModify()
    → lanceGetOptions() from catalog
    → allocate row buffer (LanceFdwModifyState)

ExecForeignInsert()  ← called per row
    → slot_getallattrs → copy Datum values to buffer
    → deep-copy varlena types (text, bytea)

EndForeignModify()
    → for each column: convert Datum[] to typed array
        int8 → int64_t[], float8 → double[], text → const char*[]
    → lance_c_append_columns(uri, names, types, ncols, nrows, data, nulls)
    → Rust builds Arrow RecordBatch → InsertBuilder::execute()
```

### lance_import() — Schema Discovery

```sql
SELECT lance_import('lance_srv', 'my_table', '/data/ds.lance');
```

```
lance_c_open_dataset(uri)
    → lance_c_get_schema()
    → lance_c_schema_to_arrow()  → ArrowSchema with children
    → for each child:
        arrow_format_to_pg_type(format)  → PG type name
    → build "CREATE FOREIGN TABLE ... " DDL string
    → return as text Datum
```

### Error Handling

All FFI errors go through `lance_get_error()`:

```cpp
static const char *lance_get_error(void) {
    const char *rust_err = lance_c_last_error_message();  // Rust heap alloc
    if (rust_err == NULL) return "unknown error";
    char *pg_err = pstrdup(rust_err);   // Copy to PG palloc
    lance_c_free_string(rust_err);      // Free Rust memory
    return pg_err;
}
```

This prevents memory leaks from Rust-allocated error strings.

## arrow_to_pg.cpp — Type Conversion

### Dispatch Flow

```
arrow_value_to_datum(array, schema, row, pg_type_oid)
    │
    ├── arrow_is_null(array, row)?  →  return NULL Datum
    │
    ├── schema->dictionary != NULL?  →  (handled in Rust: dict unpacked before FFI)
    │
    ├── buffers == NULL?  →  return NULL Datum (safety guard)
    │
    └── switch on schema->format:
        "b"     → BoolGetDatum(bit from buffers[1])
        "c/C"   → Int16GetDatum(int8/uint8)
        "s/S"   → Int16/Int32GetDatum
        "i/I"   → Int32/Int64GetDatum
        "l/L"   → Int64GetDatum
        "e"     → Float4GetDatum(half-float conversion)
        "f/g"   → Float4/Float8GetDatum
        "u/U"   → cstring_to_text_with_len(offsets + data)
        "z/Z"   → bytea via palloc + memcpy
        "w:N"   → FixedSizeBinary → bytea
        "tdD"   → DateADTGetDatum(days - UNIX_TO_PG_EPOCH_DAYS)
        "tdm"   → DateADTGetDatum(ms / 86400000 - epoch)
        "tsu:"  → TimestampGetDatum(usec - UNIX_TO_PG_EPOCH_USEC)
        "d:P,S" → Decimal128: 128-bit LE → digit string → numeric_in
        "+l/+L" → List: iterate offsets, recurse, construct_md_array
        "+w:N"  → FixedSizeList: same approach
        "+s"    → Struct: serialize fields to JSON text
```

### Epoch Conversion Constants

```
Unix epoch:  1970-01-01 00:00:00 UTC
PG epoch:    2000-01-01 00:00:00 UTC
Difference:  10957 days = 946684800 seconds = 946684800000000 microseconds
```

### Null Detection

Arrow validity bitmaps use LSB (least-significant-bit) ordering:

```
Byte 0:  bit0=row0, bit1=row1, ..., bit7=row7
Byte 1:  bit0=row8, bit1=row9, ...

Check:   validity[row / 8] & (1 << (row % 8))
         0 = NULL,  1 = valid
```

### Safety Guards

- `n_buffers < 2` for scalar types → return NULL
- `children == NULL` for List/Struct → return NULL
- List length capped at 1,000,000 elements
- FixedSizeList size validated > 0 and <= 1,000,000
- Decimal128 buffer sized at 128 bytes (worst case: 81 bytes for Decimal(38,38))

## openGauss-Specific Adaptations

Code patterns that differ from standard PostgreSQL FDW:

| Pattern | Standard PG | openGauss | Our approach |
|---------|------------|-----------|--------------|
| Pass data planner→executor | `fdw_private` in plan node | Serialization crashes | Re-fetch from catalog |
| Get slot attributes | `slot_getallattrs(slot)` | `heap_slot_getallattrs(slot)` | openGauss variant |
| Allow INSERT | Automatic if callbacks set | Requires `IsForeignRelUpdatable` + `PlanForeignModify` | Return `(1 << CMD_INSERT)` + dummy list |
| INSERT syntax | `INSERT ... VALUES` works | Only `INSERT ... SELECT` allowed | Document as limitation |
| Extension files | `.c` with C99 | `.cpp` with C++14, specific defines | Match openGauss contrib flags |
| Compilation defines | Standard PG | `-DPGXC -DSTREAMPLAN -D_GLIBCXX_USE_CXX11_ABI=0` | Copy from `file_fdw` build flags |
