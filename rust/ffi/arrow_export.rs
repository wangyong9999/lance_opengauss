use std::ffi::c_void;
use std::sync::Arc;

use arrow::array::{Array, StructArray};
use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};

use crate::error::{clear_last_error, set_last_error, ErrorCode};

use super::util::{batch_handle, schema_handle, schema_to_ffi_arrow_schema, FfiError, FfiResult};

#[no_mangle]
pub unsafe extern "C" fn lance_c_free_schema(schema: *mut c_void) {
    if !schema.is_null() {
        unsafe {
            let _ = Box::from_raw(schema as *mut super::types::SchemaHandle);
        }
    }
}

/// Export a schema handle to the Arrow C Data Interface format.
#[no_mangle]
pub unsafe extern "C" fn lance_c_schema_to_arrow(
    schema: *mut c_void,
    out_schema: *mut FFI_ArrowSchema,
) -> i32 {
    match schema_to_arrow_inner(schema, out_schema) {
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

fn schema_to_arrow_inner(schema: *mut c_void, out_schema: *mut FFI_ArrowSchema) -> FfiResult<()> {
    if out_schema.is_null() {
        return Err(FfiError::new(
            ErrorCode::InvalidArgument,
            "out_schema is null",
        ));
    }
    let schema = unsafe { schema_handle(schema)? };
    let ffi_schema = schema_to_ffi_arrow_schema(schema)?;
    unsafe {
        std::ptr::write_unaligned(out_schema, ffi_schema);
    }
    Ok(())
}

#[no_mangle]
pub unsafe extern "C" fn lance_c_free_batch(batch: *mut c_void) {
    if !batch.is_null() {
        unsafe {
            let _ = Box::from_raw(batch as *mut arrow::array::RecordBatch);
        }
    }
}

/// Export a RecordBatch to Arrow C Data Interface structs (ArrowArray + ArrowSchema).
#[no_mangle]
pub unsafe extern "C" fn lance_c_batch_to_arrow(
    batch: *mut c_void,
    out_array: *mut FFI_ArrowArray,
    out_schema: *mut FFI_ArrowSchema,
) -> i32 {
    match batch_to_arrow_inner(batch, out_array, out_schema) {
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

/// Unpack dictionary-encoded columns in a RecordBatch to plain arrays.
/// This simplifies the C-side conversion since it doesn't need to handle
/// dictionary index lookup.
fn unpack_dictionaries(batch: &arrow::array::RecordBatch) -> FfiResult<arrow::array::RecordBatch> {
    use arrow::compute::cast;

    let schema = batch.schema();
    let mut new_columns: Vec<Arc<dyn Array>> = Vec::with_capacity(batch.num_columns());
    let mut new_fields: Vec<arrow::datatypes::FieldRef> = Vec::with_capacity(batch.num_columns());

    for i in 0..batch.num_columns() {
        let col = batch.column(i);
        let field = schema.field(i);

        if let arrow::datatypes::DataType::Dictionary(_, value_type) = col.data_type() {
            // Cast dictionary → value type (e.g., Dict<Int32, Utf8> → Utf8)
            let unpacked = cast(col.as_ref(), value_type.as_ref())
                .map_err(|e| FfiError::new(ErrorCode::BatchExport, format!("dict unpack: {e}")))?;
            new_fields.push(Arc::new(arrow::datatypes::Field::new(
                field.name(),
                value_type.as_ref().clone(),
                field.is_nullable(),
            )));
            new_columns.push(unpacked);
        } else {
            new_fields.push(schema.field(i).clone().into());
            new_columns.push(col.clone());
        }
    }

    let new_schema = Arc::new(arrow::datatypes::Schema::new(new_fields));
    arrow::array::RecordBatch::try_new(new_schema, new_columns)
        .map_err(|e| FfiError::new(ErrorCode::BatchExport, format!("rebuild batch: {e}")))
}

fn batch_to_arrow_inner(
    batch: *mut c_void,
    out_array: *mut FFI_ArrowArray,
    out_schema: *mut FFI_ArrowSchema,
) -> FfiResult<()> {
    if out_array.is_null() || out_schema.is_null() {
        return Err(FfiError::new(
            ErrorCode::InvalidArgument,
            "out_array/out_schema is null",
        ));
    }
    let batch = unsafe { batch_handle(batch)? };

    // Unpack dictionary columns before FFI export
    let batch = unpack_dictionaries(batch)?;

    // Use try_from to avoid panic on edge cases (e.g., struct columns)
    let struct_array: Arc<dyn Array> = match StructArray::try_from(batch) {
        Ok(sa) => Arc::new(sa),
        Err(e) => return Err(FfiError::new(ErrorCode::BatchExport, format!("struct conversion: {e}"))),
    };
    let data = struct_array.to_data();
    let array = FFI_ArrowArray::new(&data);
    let schema = FFI_ArrowSchema::try_from(data.data_type())
        .map_err(|err| FfiError::new(ErrorCode::BatchExport, format!("batch export: {err}")))?;

    unsafe {
        std::ptr::write_unaligned(out_array, array);
        std::ptr::write_unaligned(out_schema, schema);
    }
    Ok(())
}
