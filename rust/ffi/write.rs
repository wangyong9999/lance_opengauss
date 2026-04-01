use std::ffi::c_char;
use std::sync::Arc;

use arrow::array::*;
use arrow::datatypes::*;
use arrow::ffi::{from_ffi, FFI_ArrowArray, FFI_ArrowSchema};

use crate::error::{clear_last_error, set_last_error, ErrorCode};
use crate::runtime;

use super::util::{cstr_to_str, FfiError, FfiResult};

use lance::dataset::InsertBuilder;

/// Append an Arrow batch (via C Data Interface) to a Lance dataset.
/// Creates the dataset if it doesn't exist.
/// Returns 0 on success, -1 on error.
#[no_mangle]
pub unsafe extern "C" fn lance_c_append_batch(
    uri: *const c_char,
    array: *mut FFI_ArrowArray,
    schema: *mut FFI_ArrowSchema,
) -> i32 {
    match append_batch_inner(uri, array, schema) {
        Ok(()) => {
            clear_last_error();
            0
        }
        Err(err) => {
            set_last_error(err.code, err.message);
            -1
        }
    }
}

fn append_batch_inner(
    uri: *const c_char,
    array: *mut FFI_ArrowArray,
    schema: *mut FFI_ArrowSchema,
) -> FfiResult<()> {
    let uri_str = unsafe { cstr_to_str(uri, "uri")? };

    if array.is_null() || schema.is_null() {
        return Err(FfiError::new(
            ErrorCode::InvalidArgument,
            "array/schema is null",
        ));
    }

    let ffi_array = unsafe { std::ptr::read(array) };
    let ffi_schema = unsafe { std::ptr::read(schema) };

    let data = unsafe { from_ffi(ffi_array, &ffi_schema) }.map_err(|e| {
        FfiError::new(ErrorCode::BatchExport, format!("arrow import: {e}"))
    })?;

    let struct_array = StructArray::from(data);
    let batch = RecordBatch::from(struct_array);

    match runtime::block_on(async {
        InsertBuilder::new(uri_str).execute(vec![batch]).await
    }) {
        Ok(Ok(_ds)) => Ok(()),
        Ok(Err(err)) => Err(FfiError::new(
            ErrorCode::BatchExport,
            format!("dataset write: {err}"),
        )),
        Err(err) => Err(FfiError::new(ErrorCode::Runtime, format!("runtime: {err}"))),
    }
}

/// Append rows given as column-oriented typed arrays.
/// This is a simplified write interface for the FDW INSERT path.
///
/// col_types: array of Arrow format chars ('l'=int64, 'g'=float64, 'u'=utf8)
/// col_names: array of column name C-strings
/// ncols: number of columns
/// nrows: number of rows
/// col_data: array of ncols pointers, each pointing to nrows values:
///   - For 'l': int64_t[nrows]
///   - For 'g': double[nrows]
///   - For 'u': const char*[nrows] (NULL for null values)
/// col_nulls: array of ncols pointers to bool[nrows] null bitmaps (true=null)
///
/// Returns 0 on success, -1 on error.
#[no_mangle]
pub unsafe extern "C" fn lance_c_append_columns(
    uri: *const c_char,
    col_names: *const *const c_char,
    col_types: *const c_char,
    ncols: usize,
    nrows: usize,
    col_data: *const *const std::ffi::c_void,
    col_nulls: *const *const bool,
) -> i32 {
    match append_columns_inner(uri, col_names, col_types, ncols, nrows, col_data, col_nulls) {
        Ok(()) => {
            clear_last_error();
            0
        }
        Err(err) => {
            set_last_error(err.code, err.message);
            -1
        }
    }
}

fn append_columns_inner(
    uri: *const c_char,
    col_names: *const *const c_char,
    col_types: *const c_char,
    ncols: usize,
    nrows: usize,
    col_data: *const *const std::ffi::c_void,
    col_nulls: *const *const bool,
) -> FfiResult<()> {
    use super::util::optional_cstr_array;

    let uri_str = unsafe { cstr_to_str(uri, "uri")? };
    let names = unsafe { optional_cstr_array(col_names, ncols, "col_names")? };
    let types = unsafe { std::slice::from_raw_parts(col_types as *const u8, ncols) };
    let data_ptrs = unsafe { std::slice::from_raw_parts(col_data, ncols) };
    let null_ptrs = unsafe { std::slice::from_raw_parts(col_nulls, ncols) };

    let mut fields: Vec<Field> = Vec::with_capacity(ncols);
    let mut columns: Vec<Arc<dyn Array>> = Vec::with_capacity(ncols);

    for i in 0..ncols {
        let nulls_slice = if null_ptrs[i].is_null() {
            None
        } else {
            Some(unsafe { std::slice::from_raw_parts(null_ptrs[i], nrows) })
        };

        match types[i] as char {
            'l' => {
                let vals = unsafe { std::slice::from_raw_parts(data_ptrs[i] as *const i64, nrows) };
                let arr: Int64Array = vals.iter().enumerate().map(|(j, &v)| {
                    if nulls_slice.map_or(false, |n| n[j]) { None } else { Some(v) }
                }).collect();
                fields.push(Field::new(&names[i], DataType::Int64, true));
                columns.push(Arc::new(arr));
            }
            'g' => {
                let vals = unsafe { std::slice::from_raw_parts(data_ptrs[i] as *const f64, nrows) };
                let arr: Float64Array = vals.iter().enumerate().map(|(j, &v)| {
                    if nulls_slice.map_or(false, |n| n[j]) { None } else { Some(v) }
                }).collect();
                fields.push(Field::new(&names[i], DataType::Float64, true));
                columns.push(Arc::new(arr));
            }
            'u' => {
                let ptrs = unsafe { std::slice::from_raw_parts(data_ptrs[i] as *const *const c_char, nrows) };
                let arr: StringArray = (0..nrows).map(|j| {
                    if nulls_slice.map_or(false, |n| n[j]) || ptrs[j].is_null() {
                        None
                    } else {
                        let s = unsafe { std::ffi::CStr::from_ptr(ptrs[j]) };
                        Some(s.to_str().unwrap_or(""))
                    }
                }).collect();
                fields.push(Field::new(&names[i], DataType::Utf8, true));
                columns.push(Arc::new(arr));
            }
            'f' => {
                let vals = unsafe { std::slice::from_raw_parts(data_ptrs[i] as *const f32, nrows) };
                let arr: Float32Array = vals.iter().enumerate().map(|(j, &v)| {
                    if nulls_slice.map_or(false, |n| n[j]) { None } else { Some(v) }
                }).collect();
                fields.push(Field::new(&names[i], DataType::Float32, true));
                columns.push(Arc::new(arr));
            }
            'i' => {
                let vals = unsafe { std::slice::from_raw_parts(data_ptrs[i] as *const i32, nrows) };
                let arr: Int32Array = vals.iter().enumerate().map(|(j, &v)| {
                    if nulls_slice.map_or(false, |n| n[j]) { None } else { Some(v) }
                }).collect();
                fields.push(Field::new(&names[i], DataType::Int32, true));
                columns.push(Arc::new(arr));
            }
            't' => {
                // boolean
                let vals = unsafe { std::slice::from_raw_parts(data_ptrs[i] as *const bool, nrows) };
                let arr: BooleanArray = vals.iter().enumerate().map(|(j, &v)| {
                    if nulls_slice.map_or(false, |n| n[j]) { None } else { Some(v) }
                }).collect();
                fields.push(Field::new(&names[i], DataType::Boolean, true));
                columns.push(Arc::new(arr));
            }
            _ => {
                return Err(FfiError::new(
                    ErrorCode::InvalidArgument,
                    format!("unsupported column type '{}' for write", types[i] as char),
                ));
            }
        }
    }

    let schema = Arc::new(Schema::new(fields));
    let batch = RecordBatch::try_new(schema, columns)
        .map_err(|e| FfiError::new(ErrorCode::BatchExport, format!("build batch: {e}")))?;

    match runtime::block_on(async {
        InsertBuilder::new(uri_str).execute(vec![batch]).await
    }) {
        Ok(Ok(_ds)) => Ok(()),
        Ok(Err(err)) => Err(FfiError::new(
            ErrorCode::BatchExport,
            format!("dataset write: {err}"),
        )),
        Err(err) => Err(FfiError::new(ErrorCode::Runtime, format!("runtime: {err}"))),
    }
}
