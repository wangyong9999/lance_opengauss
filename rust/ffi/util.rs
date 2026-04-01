use std::ffi::{c_char, c_void, CStr, CString};

use anyhow::Context;
use arrow::array::RecordBatch;
use arrow::datatypes::Schema;

use crate::error::ErrorCode;

use super::types::{DatasetHandle, SchemaHandle, StreamHandle};

#[derive(Debug)]
pub(crate) struct FfiError {
    pub(crate) code: ErrorCode,
    pub(crate) message: String,
}

impl FfiError {
    pub(crate) fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
}

pub(crate) type FfiResult<T> = Result<T, FfiError>;

pub(crate) fn to_c_string(s: impl AsRef<str>) -> CString {
    match CString::new(s.as_ref()) {
        Ok(v) => v,
        Err(_) => CString::new(s.as_ref().replace('\0', "\\0"))
            .unwrap_or_else(|_| CString::new("invalid string").unwrap()),
    }
}

pub(crate) unsafe fn cstr_to_str<'a>(
    ptr: *const c_char,
    what: &'static str,
) -> FfiResult<&'a str> {
    if ptr.is_null() {
        return Err(FfiError::new(
            ErrorCode::InvalidArgument,
            format!("{what} is null"),
        ));
    }
    let s = unsafe { CStr::from_ptr(ptr) }
        .to_str()
        .context("utf8 decode")
        .map_err(|err| FfiError::new(ErrorCode::Utf8, format!("{what} utf8: {err}")))?;
    Ok(s)
}

pub(crate) unsafe fn optional_cstr_array(
    ptr: *const *const c_char,
    len: usize,
    what: &'static str,
) -> FfiResult<Vec<String>> {
    if len == 0 {
        return Ok(Vec::new());
    }
    if ptr.is_null() {
        return Err(FfiError::new(
            ErrorCode::InvalidArgument,
            format!("{what} is null with non-zero length"),
        ));
    }

    let slice = unsafe { std::slice::from_raw_parts(ptr, len) };
    let mut out = Vec::with_capacity(len);
    for (idx, &item) in slice.iter().enumerate() {
        if item.is_null() {
            return Err(FfiError::new(
                ErrorCode::InvalidArgument,
                format!("{what}[{idx}] is null"),
            ));
        }
        let s = unsafe { CStr::from_ptr(item) }
            .to_str()
            .context("utf8 decode")
            .map_err(|err| FfiError::new(ErrorCode::Utf8, format!("{what}[{idx}] utf8: {err}")))?;
        out.push(s.to_string());
    }
    Ok(out)
}

pub(crate) unsafe fn slice_from_ptr<'a, T>(
    ptr: *const T,
    len: usize,
    what: &'static str,
) -> FfiResult<&'a [T]> {
    if ptr.is_null() {
        return Err(FfiError::new(
            ErrorCode::InvalidArgument,
            format!("{what} is null"),
        ));
    }
    Ok(unsafe { std::slice::from_raw_parts(ptr, len) })
}

pub(crate) unsafe fn dataset_handle<'a>(dataset: *mut c_void) -> FfiResult<&'a DatasetHandle> {
    if dataset.is_null() {
        return Err(FfiError::new(ErrorCode::InvalidArgument, "dataset is null"));
    }
    Ok(unsafe { &*(dataset as *const DatasetHandle) })
}

pub(crate) unsafe fn stream_handle_mut<'a>(
    stream: *mut c_void,
) -> FfiResult<&'a mut StreamHandle> {
    if stream.is_null() {
        return Err(FfiError::new(ErrorCode::InvalidArgument, "stream is null"));
    }
    Ok(unsafe { &mut *(stream as *mut StreamHandle) })
}

pub(crate) unsafe fn schema_handle<'a>(schema: *mut c_void) -> FfiResult<&'a SchemaHandle> {
    if schema.is_null() {
        return Err(FfiError::new(ErrorCode::InvalidArgument, "schema is null"));
    }
    Ok(unsafe { &*(schema as *const SchemaHandle) })
}

pub(crate) unsafe fn batch_handle<'a>(batch: *mut c_void) -> FfiResult<&'a RecordBatch> {
    if batch.is_null() {
        return Err(FfiError::new(ErrorCode::InvalidArgument, "batch is null"));
    }
    Ok(unsafe { &*(batch as *const RecordBatch) })
}

pub(crate) fn schema_to_ffi_arrow_schema(
    schema: &Schema,
) -> FfiResult<arrow::ffi::FFI_ArrowSchema> {
    let data_type = arrow::datatypes::DataType::Struct(schema.fields().clone());
    arrow::ffi::FFI_ArrowSchema::try_from(&data_type)
        .map_err(|err| FfiError::new(ErrorCode::SchemaExport, format!("schema export: {err}")))
}
