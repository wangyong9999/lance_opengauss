# Rust FFI Layer (liblance_c)

Database-agnostic C API for reading and writing Lance datasets. Compiled as a Rust staticlib (`liblance_c.a`, ~280MB with S3 support) that is linked into the openGauss FDW extension.

## Design Principles

1. **Opaque handles** — Dataset, Stream, Schema, Batch are all `void*` pointers to Rust heap objects. The C side never touches Rust internals.
2. **Thread-local errors** — Errors propagate via `lance_c_last_error_message()` instead of return structs. The C++ wrapper `lance_get_error()` copies and frees in one call.
3. **Arrow C Data Interface** — Zero-copy data transfer between Rust and C via the standard `ArrowArray`/`ArrowSchema` structs.
4. **Lazy Tokio runtime** — A global multi-thread runtime (2 workers) is created on first FFI call via `OnceLock`. This is fork-safe because openGauss backends initialize it after fork.

## Module Structure

```
rust/
├── lib.rs              # Crate root — declares all modules
├── runtime.rs          # Tokio runtime management
├── error.rs            # Thread-local error state
├── scanner.rs          # Async→sync stream bridge
├── ffi/
│   ├── mod.rs          # FFI module declarations
│   ├── types.rs        # Handle types (DatasetHandle, StreamHandle, SchemaHandle)
│   ├── util.rs         # C string conversion, handle casting, Arrow schema helpers
│   ├── dataset.rs      # Dataset lifecycle: open, close, count_rows, get_schema
│   ├── scan.rs         # Scan stream creation with projection + SQL filter + limit
│   ├── stream.rs       # Stream iteration: next batch, close
│   ├── arrow_export.rs # RecordBatch → Arrow C Data Interface + dictionary unpacking
│   ├── write.rs        # Columnar write: append_columns, append_batch
│   └── search.rs       # KNN vector search
└── bin/
    └── create_test_data.rs  # Test dataset generator (6 modes)
```

## Key Files

### runtime.rs — Async Bridge

```
OnceLock<Runtime>  ──→  block_on(future)  ──→  Sync result
```

Lance is fully async (tokio). Every FFI function that calls Lance wraps it in `runtime::block_on()` which blocks the calling thread until the async operation completes. The runtime uses `new_multi_thread().worker_threads(2)` because Lance internally uses `tokio::spawn()` for parallel I/O — a single-thread runtime would deadlock.

### error.rs — Error Propagation

```
Rust error  ──→  set_last_error(code, msg)  ──→  thread_local RefCell
                                                       │
C caller    ──→  lance_c_last_error_message()  ←───────┘
                 (returns CString, caller must free)
```

The C++ layer wraps this in `lance_get_error()` which copies via `pstrdup` and immediately frees the Rust string.

### scanner.rs — Stream Wrapper

Wraps Lance's async `DatasetRecordBatchStream` into a sync iterator:

```rust
pub struct LanceStream {
    handle: Handle,           // Tokio handle for block_on
    stream: Pin<Box<...>>,    // The async stream
}

impl LanceStream {
    fn next(&mut self) -> Result<Option<RecordBatch>> {
        self.handle.block_on(async { self.stream.next().await })
    }
}
```

### ffi/arrow_export.rs — Dictionary Unpacking

Lance datasets may store Dictionary-encoded columns (e.g., `Dictionary<Int32, Utf8>` for categorical strings). Before exporting via Arrow C Data Interface, these are unpacked to plain arrays using `arrow::compute::cast`. This simplifies the C++ conversion layer which doesn't need dictionary index resolution.

### ffi/write.rs — Columnar Write Interface

`lance_c_append_columns()` accepts column-oriented raw data from C:

```
C side:  int64_t ids[] = {1, 2, 3};
         const char* names[] = {"a", "b", NULL};
         bool nulls[] = {false, false, true};
              │
              ▼
Rust:    Build Arrow arrays (Int64Array, StringArray)
         → RecordBatch → InsertBuilder::new(uri).execute(batches)
```

Supported column types: `l`=int64, `i`=int32, `g`=float64, `f`=float32, `u`=utf8, `t`=bool.

### ffi/search.rs — Vector Search

```
lance_c_knn_search(uri, vector_col, query_vec, k, metric)
    │
    ▼
Dataset::open(uri) → scan.nearest(col, query, k) → LanceStream
```

Returns a standard stream handle — results include all columns plus `_distance`. Supports L2, Cosine, Dot metrics via `lance_linalg::DistanceType`.

## FFI Surface (~15 exported functions)

| Function | Purpose |
|----------|---------|
| `lance_c_open_dataset` | Open dataset by URI |
| `lance_c_open_dataset_with_options` | Open with storage options (S3 creds) |
| `lance_c_close_dataset` | Free dataset handle |
| `lance_c_dataset_count_rows` | Row count from metadata |
| `lance_c_get_schema` | Get schema handle |
| `lance_c_schema_to_arrow` | Export schema to ArrowSchema |
| `lance_c_free_schema` | Free schema handle |
| `lance_c_create_scan_stream` | Create scan with projection + filter + limit |
| `lance_c_stream_next` | Get next RecordBatch (0=ok, 1=end, -1=error) |
| `lance_c_close_stream` | Free stream handle |
| `lance_c_batch_to_arrow` | Export batch to ArrowArray + ArrowSchema |
| `lance_c_free_batch` | Free batch handle |
| `lance_c_append_columns` | Write columnar data to dataset |
| `lance_c_append_batch` | Write Arrow batch to dataset |
| `lance_c_knn_search` | Create KNN search stream |
| `lance_c_last_error_code` | Get error code |
| `lance_c_last_error_message` | Get error message (must free) |
| `lance_c_free_string` | Free Rust-allocated string |

## Test Data Generator

`create_test_data.rs` produces 6 dataset variants:

| Mode | Schema | Rows | Purpose |
|------|--------|------|---------|
| `basic` | id(int64), name(utf8), score(float64) | 5 | Core scan tests |
| `types` | 19 columns covering all types | 4 | Type mapping validation |
| `empty` | id, name, value | 0 | Empty dataset edge case |
| `large` | id, name, score | 10,000 | Multi-batch, performance |
| `nested` | id, category(dict), meta(struct) | 4 | Dictionary + Struct types |
| `vector` | id, label, vec(fixed_list<f32,3>) | 5 | KNN search tests |
