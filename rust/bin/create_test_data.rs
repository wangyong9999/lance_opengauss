//! Creates Lance test datasets for integration testing.
//!
//! Subcommands:
//!   create_test_data basic <path>    — 5-row dataset (id, name, score)
//!   create_test_data types <path>    — all-types dataset for type mapping tests
//!   create_test_data empty <path>    — empty dataset (0 rows, 3 columns)
//!   create_test_data large <path>    — 10000-row dataset for batch/perf tests

use std::sync::Arc;

use arrow::array::*;
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::*;
use arrow::record_batch::RecordBatch;
use lance::dataset::InsertBuilder;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let cmd = args.get(1).map(|s| s.as_str()).unwrap_or("basic");
    let output_path = args
        .get(2)
        .expect("Usage: create_test_data <cmd> <output_path>");

    let rt = tokio::runtime::Runtime::new().expect("Failed to create tokio runtime");

    match cmd {
        "basic" => rt.block_on(create_basic(output_path)),
        "types" => rt.block_on(create_types(output_path)),
        "empty" => rt.block_on(create_empty(output_path)),
        "large" => rt.block_on(create_large(output_path)),
        "nested" => rt.block_on(create_nested(output_path)),
        "vector" => rt.block_on(create_vector(output_path)),
        _ => panic!("Unknown command: {cmd}. Use: basic, types, empty, large, nested, vector"),
    }
}

async fn write_dataset(path: &str, batch: RecordBatch) {
    let ds = InsertBuilder::new(path)
        .execute(vec![batch])
        .await
        .expect("Failed to write dataset");
    let rows = ds.count_rows(None).await.unwrap();
    let cols = ds.schema().fields.len();
    println!("  {path}: {rows} rows, {cols} columns");
}

/// Basic 5-row dataset (used by existing tests)
async fn create_basic(path: &str) {
    println!("Creating basic dataset");
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("score", DataType::Float64, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
            Arc::new(StringArray::from(vec![
                Some("alice"),
                Some("bob"),
                Some("charlie"),
                None,
                Some("eve"),
            ])),
            Arc::new(Float64Array::from(vec![
                Some(95.5),
                Some(87.3),
                Some(92.1),
                Some(78.9),
                None,
            ])),
        ],
    )
    .unwrap();
    write_dataset(path, batch).await;
}

/// All-types dataset for comprehensive type mapping tests
async fn create_types(path: &str) {
    println!("Creating all-types dataset");

    // 4 rows: normal values, edge values, NULLs, boundary values
    let schema = Arc::new(Schema::new(vec![
        Field::new("col_bool", DataType::Boolean, true),
        Field::new("col_int8", DataType::Int8, true),
        Field::new("col_int16", DataType::Int16, true),
        Field::new("col_int32", DataType::Int32, true),
        Field::new("col_int64", DataType::Int64, true),
        Field::new("col_uint16", DataType::UInt16, true),
        Field::new("col_uint32", DataType::UInt32, true),
        Field::new("col_float32", DataType::Float32, true),
        Field::new("col_float64", DataType::Float64, true),
        Field::new("col_utf8", DataType::Utf8, true),
        Field::new("col_binary", DataType::Binary, true),
        Field::new("col_date32", DataType::Date32, true),
        Field::new(
            "col_timestamp",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
        Field::new(
            "col_timestamp_tz",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        ),
        Field::new(
            "col_decimal",
            DataType::Decimal128(10, 2),
            true,
        ),
        Field::new(
            "col_list_int",
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
            true,
        ),
        Field::new(
            "col_list_float",
            DataType::List(Arc::new(Field::new("item", DataType::Float32, true))),
            true,
        ),
        Field::new("col_large_utf8", DataType::LargeUtf8, true),
        Field::new(
            "col_fixed_binary",
            DataType::FixedSizeBinary(4),
            true,
        ),
    ]));

    // Row 0: normal values
    // Row 1: different normal values
    // Row 2: all NULLs
    // Row 3: boundary/edge values

    let batch = RecordBatch::try_new(
        schema,
        vec![
            // col_bool
            Arc::new(BooleanArray::from(vec![
                Some(true),
                Some(false),
                None,
                Some(true),
            ])),
            // col_int8
            Arc::new(Int8Array::from(vec![
                Some(42i8),
                Some(-1),
                None,
                Some(127),   // INT8 MAX
            ])),
            // col_int16
            Arc::new(Int16Array::from(vec![
                Some(1000i16),
                Some(-32768),   // INT16 MIN
                None,
                Some(32767),    // INT16 MAX
            ])),
            // col_int32
            Arc::new(Int32Array::from(vec![
                Some(100000i32),
                Some(-1),
                None,
                Some(i32::MAX),
            ])),
            // col_int64
            Arc::new(Int64Array::from(vec![
                Some(1_000_000_000i64),
                Some(-1),
                None,
                Some(i64::MAX),
            ])),
            // col_uint16
            Arc::new(UInt16Array::from(vec![
                Some(1000u16),
                Some(0),
                None,
                Some(65535),    // UINT16 MAX
            ])),
            // col_uint32
            Arc::new(UInt32Array::from(vec![
                Some(100000u32),
                Some(0),
                None,
                Some(u32::MAX), // 4294967295
            ])),
            // col_float32
            Arc::new(Float32Array::from(vec![
                Some(3.14f32),
                Some(-0.0),
                None,
                Some(f32::MAX),
            ])),
            // col_float64
            Arc::new(Float64Array::from(vec![
                Some(2.718281828459045f64),
                Some(-1.0e100),
                None,
                Some(f64::INFINITY),
            ])),
            // col_utf8
            Arc::new(StringArray::from(vec![
                Some("hello"),
                Some(""),           // empty string
                None,
                Some("日本語テスト"),  // unicode
            ])),
            // col_binary
            Arc::new(BinaryArray::from(vec![
                Some(b"\x00\x01\x02\x03".as_ref()),
                Some(b"".as_ref()),
                None,
                Some(b"\xff\xfe".as_ref()),
            ])),
            // col_date32 (days since Unix epoch)
            Arc::new(Date32Array::from(vec![
                Some(19723),    // 2023-12-25
                Some(0),        // 1970-01-01
                None,
                Some(10957),    // 2000-01-01 (PG epoch)
            ])),
            // col_timestamp (microseconds since Unix epoch, no TZ)
            Arc::new(TimestampMicrosecondArray::from(vec![
                Some(1703462400_000_000i64),  // 2023-12-25 00:00:00
                Some(0),                      // 1970-01-01 00:00:00
                None,
                Some(946684800_000_000),      // 2000-01-01 00:00:00 (PG epoch)
            ])),
            // col_timestamp_tz (microseconds since Unix epoch, with TZ)
            Arc::new(
                TimestampMicrosecondArray::from(vec![
                    Some(1703462400_000_000i64),
                    Some(0),
                    None,
                    Some(946684800_000_000),
                ])
                .with_timezone("UTC"),
            ),
            // col_decimal (Decimal128(10,2))
            Arc::new(
                Decimal128Array::from(vec![
                    Some(12345i128),    // 123.45
                    Some(-100i128),     // -1.00
                    None,
                    Some(99999999_99i128), // 999999999.99 (max for (10,2) roughly)
                ])
                .with_precision_and_scale(10, 2)
                .unwrap(),
            ),
            // col_list_int (List<Int32>)
            {
                let values = Int32Array::from(vec![
                    Some(1), Some(2), Some(3),      // row 0: [1,2,3]
                    Some(10),                        // row 1: [10]
                                                     // row 2: NULL (no values)
                    Some(100), Some(200),            // row 3: [100,200]
                ]);
                let offsets = OffsetBuffer::new(vec![0, 3, 4, 4, 6].into());
                let nulls = Some(arrow::buffer::NullBuffer::from(vec![true, true, false, true]));
                Arc::new(ListArray::new(
                    Arc::new(Field::new("item", DataType::Int32, true)),
                    offsets,
                    Arc::new(values),
                    nulls,
                ))
            },
            // col_list_float (List<Float32>) — embedding-like
            {
                let values = Float32Array::from(vec![
                    0.1f32, 0.2, 0.3,   // row 0
                    0.4, 0.5, 0.6,       // row 1
                                          // row 2: NULL
                    0.7, 0.8, 0.9,       // row 3
                ]);
                let offsets = OffsetBuffer::new(vec![0, 3, 6, 6, 9].into());
                let nulls = Some(arrow::buffer::NullBuffer::from(vec![true, true, false, true]));
                Arc::new(ListArray::new(
                    Arc::new(Field::new("item", DataType::Float32, true)),
                    offsets,
                    Arc::new(values),
                    nulls,
                ))
            },
            // col_large_utf8
            Arc::new(LargeStringArray::from(vec![
                Some("large_hello"),
                Some(""),
                None,
                Some("large_unicode_日本"),
            ])),
            // col_fixed_binary (FixedSizeBinary(4))
            Arc::new(
                FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                    vec![
                        Some(b"\x01\x02\x03\x04".to_vec()),
                        Some(b"\x00\x00\x00\x00".to_vec()),
                        None,
                        Some(b"\xff\xff\xff\xff".to_vec()),
                    ]
                    .into_iter(),
                    4,
                )
                .unwrap(),
            ),
        ],
    )
    .unwrap();

    write_dataset(path, batch).await;
}

/// Empty dataset (0 rows)
async fn create_empty(path: &str) {
    println!("Creating empty dataset");
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("value", DataType::Float64, true),
    ]));
    let batch = RecordBatch::new_empty(schema);
    write_dataset(path, batch).await;
}

/// Large dataset (10000 rows) for batch boundary and performance tests
async fn create_large(path: &str) {
    println!("Creating large dataset (10000 rows)");
    let n = 10000i64;
    let ids: Vec<i64> = (1..=n).collect();
    let names: Vec<Option<&str>> = (0..n)
        .map(|i| {
            if i % 100 == 0 {
                None
            } else {
                Some("name")
            }
        })
        .collect();
    let scores: Vec<Option<f64>> = (0..n)
        .map(|i| {
            if i % 50 == 0 {
                None
            } else {
                Some((i as f64) * 0.1)
            }
        })
        .collect();

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("score", DataType::Float64, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(names)),
            Arc::new(Float64Array::from(scores)),
        ],
    )
    .unwrap();
    write_dataset(path, batch).await;
}

/// Nested types dataset: dictionary-encoded strings + struct
async fn create_nested(path: &str) {
    use arrow::array::{DictionaryArray, StructArray};
    use arrow::datatypes::Int32Type;

    println!("Creating nested types dataset");

    let dict_values: DictionaryArray<Int32Type> =
        vec![Some("cat"), Some("dog"), None, Some("cat")]
            .into_iter()
            .collect();

    let struct_array = StructArray::from(vec![
        (
            Arc::new(Field::new("score", DataType::Float32, true)),
            Arc::new(Float32Array::from(vec![Some(9.5f32), Some(8.0), None, Some(7.2)])) as _,
        ),
        (
            Arc::new(Field::new("tag", DataType::Utf8, true)),
            Arc::new(StringArray::from(vec![Some("good"), Some("ok"), None, Some("fair")])) as _,
        ),
    ]);

    let struct_fields = vec![
        Field::new("score", DataType::Float32, true),
        Field::new("tag", DataType::Utf8, true),
    ];
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new(
            "category",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            true,
        ),
        Field::new("meta", DataType::Struct(struct_fields.into()), true),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
            Arc::new(dict_values),
            Arc::new(struct_array),
        ],
    )
    .unwrap();
    write_dataset(path, batch).await;
}

/// Vector dataset for KNN search testing
async fn create_vector(path: &str) {
    println!("Creating vector dataset");
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("label", DataType::Utf8, false),
        Field::new(
            "vec",
            DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Float32, true)),
                3,
            ),
            false,
        ),
    ]));

    // 5 vectors in 3D space
    let float_values = Float32Array::from(vec![
        1.0f32, 0.0, 0.0,   // id=1: x-axis
        0.0, 1.0, 0.0,      // id=2: y-axis
        0.0, 0.0, 1.0,      // id=3: z-axis
        1.0, 1.0, 0.0,      // id=4: xy-plane
        0.0, 1.0, 1.0,      // id=5: yz-plane
    ]);
    let fsl = FixedSizeListArray::new(
        Arc::new(Field::new("item", DataType::Float32, true)),
        3,
        Arc::new(float_values),
        None,
    );

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
            Arc::new(StringArray::from(vec!["x-axis", "y-axis", "z-axis", "xy-plane", "yz-plane"])),
            Arc::new(fsl),
        ],
    )
    .unwrap();
    write_dataset(path, batch).await;
}
