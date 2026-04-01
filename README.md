# Lance for openGauss

[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

A Foreign Data Wrapper (FDW) that brings [Lance](https://github.com/lancedb/lance) columnar datasets into [openGauss](https://opengauss.org/), enabling SQL queries over ML-optimized data without ETL.

Lance is a modern columnar format designed for machine learning — it provides fast random access, vector search, zero-copy versioning, and efficient encodings for embeddings. This project bridges Lance into the openGauss SQL ecosystem.

## Why

| Scenario | Without lance_opengauss | With lance_opengauss |
|----------|------------------------|---------------------|
| Query ML training data | Export to CSV, import to PG | `SELECT * FROM training_data WHERE label = 'cat'` |
| Vector similarity search | Separate vector DB | Lance KNN via SQL (FFI ready) |
| Inspect Lance dataset schema | Python script | `SELECT lance_import(...)` |
| Join ML features with business data | Custom pipeline | Standard SQL JOIN on foreign table |
| Access S3-hosted datasets | Download + convert | Direct URI: `OPTIONS (uri 's3://bucket/dataset')` |

## Quick Start

```sql
-- Install extension
CREATE EXTENSION lance_fdw;
CREATE SERVER lance_srv FOREIGN DATA WRAPPER lance_fdw;

-- Create foreign table (manual schema)
CREATE FOREIGN TABLE embeddings (
    id        bigint,
    text      text,
    embedding float4[]
) SERVER lance_srv OPTIONS (uri '/data/embeddings.lance');

-- Query it like a normal table
SELECT id, text FROM embeddings
WHERE id > 1000
ORDER BY id
LIMIT 10;

-- Or auto-discover schema from dataset
SELECT lance_import('lance_srv', 'auto_table', '/data/embeddings.lance');
-- Returns: CREATE FOREIGN TABLE auto_table (id int8, text text, embedding float4[]) ...
```

## Features

### Type System

Full bidirectional Arrow-to-PostgreSQL type mapping:

| Arrow Type | PG Type | Notes |
|---|---|---|
| Boolean | `boolean` | |
| Int8, UInt8, Int16 | `int2` | |
| UInt16, Int32 | `int4` | |
| UInt32, Int64, UInt64 | `int8` | |
| Float16, Float32 | `float4` | Half-float converted to float |
| Float64 | `float8` | |
| Utf8, LargeUtf8 | `text` | Full Unicode support |
| Binary, LargeBinary, FixedSizeBinary | `bytea` | |
| Date32, Date64 | `date` | Unix epoch to PG epoch conversion |
| Timestamp(us) | `timestamp` | Microsecond precision |
| Timestamp(us, tz) | `timestamptz` | |
| Decimal128(P,S) | `numeric` | 128-bit to string to numeric |
| List\<T\>, FixedSizeList\<T\> | `T[]` | Recursive element conversion |
| Dictionary(K, V) | V type | Decoded in Rust before FFI transfer |
| Struct | `text` | Serialized as JSON: `{"score": 9.5, "tag": "good"}` |

### Filter Pushdown

WHERE predicates are automatically extracted, deparsed to SQL strings, and pushed to Lance's scanner:

```sql
-- All of these are pushed down to Lance (fewer rows read from disk):
SELECT * FROM t WHERE id = 42;
SELECT * FROM t WHERE score >= 90.0 AND name IS NOT NULL;
SELECT * FROM t WHERE id IN (1, 2, 3);
SELECT * FROM t WHERE name LIKE 'al%';
SELECT * FROM t WHERE id BETWEEN 10 AND 20;
SELECT * FROM t WHERE NOT (id = 1);

-- These are NOT pushed down (evaluated post-scan by openGauss):
SELECT * FROM t WHERE upper(name) = 'FOO';      -- function call
SELECT * FROM t WHERE int8_col > 42;             -- cross-type comparison
```

If pushdown fails (e.g., Lance rejects the SQL), the extension automatically retries without the filter. Correctness is never compromised.

### Write Path

```sql
-- openGauss requires INSERT ... SELECT (not INSERT ... VALUES) for FDW:
CREATE TEMP TABLE src (id bigint, name text, score float8);
INSERT INTO src VALUES (1, 'alice', 95.5), (2, 'bob', 87.3);

INSERT INTO my_lance_table SELECT * FROM src;
```

Rows are buffered in memory, converted to columnar Arrow arrays, and written to Lance as a single batch via the Rust FFI.

### Schema Auto-Discovery

```sql
-- Reads Lance dataset schema, returns CREATE FOREIGN TABLE DDL:
SELECT lance_import('lance_srv', 'my_table', '/data/dataset.lance');

-- Output:
-- CREATE FOREIGN TABLE my_table (
--     id int8,
--     name text,
--     embedding float4[]
-- ) SERVER lance_srv OPTIONS (uri '/data/dataset.lance');
```

Handles all types including nested (List, Dictionary, Struct).

### Vector Search (FFI Ready)

The Rust FFI layer implements KNN vector search via `lance_c_knn_search()`. Supports L2, Cosine, and Dot distance metrics. SQL function wrapper (SRF) is planned.

### Cloud Storage

S3, GCS, and Azure storage are supported at the compile level. Pass credentials via server options:

```sql
CREATE SERVER lance_s3 FOREIGN DATA WRAPPER lance_fdw
    OPTIONS (uri 's3://my-bucket/dataset.lance');
```

## Architecture

```
                     openGauss SQL
                          |
            Planning      |      Execution
        +-----------+     |   +-----------------+
        |GetForeign | <---+-> |BeginForeignScan |
        | RelSize   |         |IterateForeignScan|
        | Paths     |         |EndForeignScan   |
        | Plan      |         +---------+-------+
        +-----------+                   |
                                        | extern "C"
              lance_fdw.cpp             | FFI calls
        ============================    |
              liblance_c.a              v
        +---------------------------+--------+
        | dataset.rs  scan.rs  stream.rs     |
        | arrow_export.rs  write.rs          |
        | search.rs (KNN)                    |
        | runtime.rs (Tokio, 2 workers)      |
        +---------------------------+--------+
                                    |
                                    v
                         Lance dataset files
                      (local / S3 / GCS / Azure)
```

See [rust/README.md](rust/README.md) and [src/README.md](src/README.md) for detailed code documentation.

## Build

### Prerequisites

- openGauss compiled from source (with `mppdb_temp_install/`)
- openGauss third-party binarylibs (GCC 10.3)
- Docker (CentOS 7 based build environment)
- Rust toolchain (installed inside Docker)

### Docker Build (Recommended)

```bash
# 1. Build the Docker image (one-time)
docker build -t opengauss-build -f /path/to/openGauss-server/docker/dockerfile_build .
docker build -t lance-fdw-test -f Dockerfile.test .

# 2. Run tests (builds everything inside container)
docker run --rm \
  -v /path/to/openGauss-server:/build/openGauss-server \
  -v /path/to/binarylibs:/build/binarylibs \
  -v $(pwd):/build/lance_fdw:ro \
  -v lance_fdw_cargo_cache:/tmp/lance_build \
  -v lance_fdw_cargo_registry:/root/.cargo/registry \
  lance-fdw-test \
  bash /build/lance_fdw/test_integration.sh
```

### Build Performance

| What | Time | Cached? |
|------|------|---------|
| openGauss compilation | ~30 min | Pre-built, never rebuilt |
| Rust `liblance_c.a` (first build) | ~8 min | Docker volume persists |
| Rust incremental rebuild | ~1s | Cargo incremental compilation |
| C++ `lance_fdw.so` | ~10s | Rebuilt every code change |
| Full test suite (77 tests) | ~30s | After build |

### Manual Build

```bash
# Build Rust staticlib
cargo build --release

# Build C++ extension (adjust paths to your openGauss install)
g++ -std=c++14 -fPIC -shared \
    -I/path/to/openGauss/src/include \
    -I/path/to/openGauss/tmp_build \
    -I/path/to/binarylibs/kernel/dependency/openssl/comm/include \
    -Isrc \
    -DENABLE_GSTRACE -DPGXC -DSTREAMPLAN -D_GNU_SOURCE \
    -D_GLIBCXX_USE_CXX11_ABI=0 \
    -o lance_fdw.so src/lance_fdw.cpp src/arrow_to_pg.cpp \
    -Ltarget/release -llance_c \
    -lpthread -ldl -lm -lrt -Wl,--exclude-libs,ALL

# Install
cp lance_fdw.so /path/to/openGauss/lib/postgresql/
cp lance_fdw.control sql/lance_fdw--1.0.sql /path/to/openGauss/share/postgresql/extension/
```

## Project Structure

```
lance_opengauss/
├── README.md                       # This file
├── ROADMAP.md                      # Future plans
├── Cargo.toml                      # Rust crate config
├── CMakeLists.txt                  # CMake build
├── Makefile                        # PGXS build (alternative)
├── Dockerfile.test                 # Docker test image
├── lance_fdw.control               # PG extension metadata
├── test_integration.sh             # 77 automated tests
│
├── rust/                           # Rust FFI layer (liblance_c)
│   ├── README.md                   # Rust code documentation
│   ├── lib.rs                      # Crate root
│   ├── runtime.rs                  # Tokio async runtime
│   ├── error.rs                    # Thread-local error handling
│   ├── scanner.rs                  # Async→sync stream bridge
│   ├── ffi/                        # C FFI functions (~15 exports)
│   │   ├── dataset.rs              # open / close / count_rows / get_schema
│   │   ├── scan.rs                 # create_scan_stream (projection + filter)
│   │   ├── stream.rs               # stream_next / close_stream
│   │   ├── arrow_export.rs         # batch→Arrow C Data Interface + dict unpack
│   │   ├── write.rs                # append_columns (columnar write)
│   │   └── search.rs              # KNN vector search
│   └── bin/
│       └── create_test_data.rs     # Test dataset generator (6 modes)
│
├── src/                            # C++ FDW extension
│   ├── README.md                   # C++ code documentation
│   ├── lance_fdw.cpp               # FDW callbacks + filter deparse + INSERT + lance_import
│   ├── arrow_to_pg.cpp             # Arrow → PG Datum conversion
│   ├── lance_fdw.h                 # State structs
│   ├── lance_c_api.h               # liblance_c C API declarations
│   └── arrow_c_data.h              # Arrow C Data Interface structs
│
└── sql/
    └── lance_fdw--1.0.sql          # Extension SQL (handler, validator, lance_import)
```

## Testing

77 tests across 10 suites, all running inside Docker against a real openGauss instance:

| Suite | Tests | What it covers |
|---|---|---|
| Basic Scan | 8 | count, WHERE, NULL, ORDER BY, LIMIT/OFFSET, projection |
| Type Mapping | 32 | All 21 Arrow types, boundary values, unicode, NULL per type |
| Empty Dataset | 2 | 0-row dataset handling |
| Large Dataset | 6 | 10,000 rows, multi-batch scan, aggregation |
| Nested Types | 5 | Dictionary decode, Struct→JSON, NULL struct |
| Filter Pushdown | 14 | All operators: =, <, >=, AND, OR, NOT, IS NULL, IN, LIKE, BETWEEN |
| Schema Discovery | 4 | lance_import DDL generation, type mapping, nested types |
| Write Path | 2 | INSERT SELECT, count verification |
| Error Handling | 3 | Missing URI, bad path, invalid options |
| EXPLAIN | 1 | Foreign scan plan output |

## Known openGauss Limitations

These are openGauss platform behaviors, not lance_opengauss bugs:

| Behavior | Impact | Workaround |
|---|---|---|
| `INSERT ... VALUES` blocked for FDW | Cannot use VALUES syntax | Use `INSERT ... SELECT FROM normal_table` |
| `INSERT ... SELECT FROM foreign_table` blocked | Cannot copy between foreign tables | Stage through a normal table |
| `count(col)` may skip rows with FDW | Aggregate results may be incomplete | Use `count(*)` instead |
| `fdw_private` plan serialization crashes | Cannot pass data from planner to executor | Re-fetch options from catalog |
| Negative DateADT display issues | Pre-2000 dates show as timestamps | Use date arithmetic for verification |

## Related Projects

| Project | Database | Approach |
|---------|----------|----------|
| **lance_opengauss** (this) | openGauss (PG 9.2) | C++ FDW + Rust staticlib |
| [pglance](https://github.com/lancedb/lance/tree/main/pglance) | PostgreSQL 13-17 | Rust FDW via pgrx |
| [lance-duckdb](https://github.com/lancedb/lance/tree/main/lance-duckdb) | DuckDB | C++ extension + Rust staticlib |

## License

Apache License 2.0
