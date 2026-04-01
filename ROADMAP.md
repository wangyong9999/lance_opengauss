# lance_fdw Roadmap

## Completed

- **Read path**: Full scan, column projection, 21 Arrow types, Dictionary/Struct, empty/large datasets
- **Filter pushdown**: =, <>, <, <=, >, >=, AND, OR, NOT, IS NULL, IN, LIKE, BETWEEN with fallback
- **Schema discovery**: `lance_import()` SQL function
- **Write path**: INSERT via `INSERT SELECT FROM normal_table` (openGauss limitation)
- **KNN search**: Rust FFI implemented (`lance_c_knn_search`)
- **Cloud storage**: S3/GCS/Azure Cargo features enabled
- **Code quality**: Two audit rounds, all critical bugs fixed, 77 tests

## Next Steps

### Short Term

1. **KNN SQL function** — Wrap `lance_c_knn_search` as openGauss set-returning function (SRF)
2. **S3 end-to-end test** — Run MinIO in Docker, test read/write via S3 URIs
3. **Struct composite type** — Map Arrow Struct to openGauss composite types via `heap_form_tuple`
4. **INSERT read-back fix** — Investigate why newly written datasets fail to open in same session

### Medium Term

5. **DELETE / UPDATE** — Add FDW modify callbacks for row deletion and update
6. **EXPLAIN ANALYZE** — Show actual rows scanned, filter pushdown stats, Lance scan plan
7. **Statistics** — Implement `AnalyzeForeignTable` for better query planning
8. **Batch size control** — Pass `batch_size` option to Lance scan

### Long Term

9. **Namespace / catalog** — Multi-dataset management via Lance namespaces
10. **Index management** — CREATE/DROP INDEX for vector (IVF) and scalar (BTree) indices
11. **Full-text search** — `lance_fts()` SQL function
12. **Parallel scan** — Multi-thread scan for large datasets (requires openGauss parallel worker support)
