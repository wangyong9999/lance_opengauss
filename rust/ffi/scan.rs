use std::ffi::{c_char, c_void};
use std::ptr;

use crate::error::{clear_last_error, set_last_error, ErrorCode};
use crate::scanner::LanceStream;

use super::types::StreamHandle;
use super::util::{cstr_to_str, optional_cstr_array, FfiError, FfiResult};

/// Create a scan stream over a dataset with optional column projection, SQL filter,
/// limit, and offset.
///
/// # Arguments
/// * `dataset` - Dataset handle from `lance_c_open_dataset`
/// * `columns` / `columns_len` - Column names for projection (NULL/0 = all columns)
/// * `filter_sql` - SQL predicate string for filter pushdown (NULL = no filter)
/// * `limit` - Maximum rows to return (-1 = unlimited)
/// * `offset` - Number of rows to skip (0 = no skip)
///
/// # Returns
/// Stream handle on success, NULL on error.
#[no_mangle]
pub unsafe extern "C" fn lance_c_create_scan_stream(
    dataset: *mut c_void,
    columns: *const *const c_char,
    columns_len: usize,
    filter_sql: *const c_char,
    limit: i64,
    offset: i64,
) -> *mut c_void {
    match create_scan_stream_inner(dataset, columns, columns_len, filter_sql, limit, offset) {
        Ok(stream) => {
            clear_last_error();
            Box::into_raw(Box::new(stream)) as *mut c_void
        }
        Err(err) => {
            set_last_error(err.code, err.message);
            ptr::null_mut()
        }
    }
}

fn create_scan_stream_inner(
    dataset: *mut c_void,
    columns: *const *const c_char,
    columns_len: usize,
    filter_sql: *const c_char,
    limit: i64,
    offset: i64,
) -> FfiResult<StreamHandle> {
    let handle = unsafe { super::util::dataset_handle(dataset)? };

    if offset < 0 {
        return Err(FfiError::new(
            ErrorCode::DatasetScan,
            "offset must be non-negative",
        ));
    }
    if limit < -1 {
        return Err(FfiError::new(
            ErrorCode::DatasetScan,
            "limit must be >= -1",
        ));
    }

    let mut scan = handle.dataset.scan();

    // Column projection
    let projection = unsafe { optional_cstr_array(columns, columns_len, "columns")? };
    if !projection.is_empty() {
        scan.project(&projection).map_err(|err| {
            FfiError::new(
                ErrorCode::DatasetScan,
                format!("dataset scan project: {err}"),
            )
        })?;
    }

    // SQL string filter pushdown
    if !filter_sql.is_null() {
        let filter_str = unsafe { cstr_to_str(filter_sql, "filter_sql")? };
        if !filter_str.is_empty() {
            scan.filter(filter_str).map_err(|err| {
                FfiError::new(
                    ErrorCode::DatasetScan,
                    format!("dataset scan filter '{filter_str}': {err}"),
                )
            })?;
        }
    }

    // Limit and offset
    if limit != -1 || offset != 0 {
        let limit_opt = if limit == -1 { None } else { Some(limit) };
        let offset_opt = if offset == 0 { None } else { Some(offset) };
        scan.limit(limit_opt, offset_opt).map_err(|err| {
            FfiError::new(ErrorCode::DatasetScan, format!("dataset scan limit: {err}"))
        })?;
    }

    let stream = LanceStream::from_scanner(scan)
        .map_err(|err| FfiError::new(ErrorCode::StreamCreate, format!("stream create: {err}")))?;
    Ok(StreamHandle::Lance(stream))
}
