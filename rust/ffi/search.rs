use std::ffi::{c_char, c_void};
use std::ptr;

use crate::error::{clear_last_error, set_last_error, ErrorCode};
use crate::runtime;
use crate::scanner::LanceStream;

use super::types::StreamHandle;
use super::util::{cstr_to_str, FfiError, FfiResult};

/// Create a KNN search stream.
///
/// uri: dataset path
/// vector_column: name of the vector column
/// query: float32 array pointer
/// query_len: number of floats in query vector
/// k: number of nearest neighbors
/// metric: "l2", "cosine", or "dot" (NULL defaults to "l2")
///
/// Returns a stream handle (same as scan stream, use lance_c_stream_next).
#[no_mangle]
pub unsafe extern "C" fn lance_c_knn_search(
    uri: *const c_char,
    vector_column: *const c_char,
    query: *const f32,
    query_len: usize,
    k: usize,
    metric: *const c_char,
) -> *mut c_void {
    match knn_search_inner(uri, vector_column, query, query_len, k, metric) {
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

fn knn_search_inner(
    uri: *const c_char,
    vector_column: *const c_char,
    query: *const f32,
    query_len: usize,
    k: usize,
    metric: *const c_char,
) -> FfiResult<StreamHandle> {
    let uri_str = unsafe { cstr_to_str(uri, "uri")? };
    let col_str = unsafe { cstr_to_str(vector_column, "vector_column")? };

    if query.is_null() || query_len == 0 {
        return Err(FfiError::new(
            ErrorCode::InvalidArgument,
            "query vector is null or empty",
        ));
    }
    if k == 0 {
        return Err(FfiError::new(
            ErrorCode::InvalidArgument,
            "k must be > 0",
        ));
    }

    let query_vec: Vec<f32> = unsafe { std::slice::from_raw_parts(query, query_len) }.to_vec();

    let metric_str = if metric.is_null() {
        "l2"
    } else {
        unsafe { cstr_to_str(metric, "metric")? }
    };
    let dataset = match runtime::block_on(lance::Dataset::open(uri_str)) {
        Ok(Ok(ds)) => std::sync::Arc::new(ds),
        Ok(Err(err)) => {
            return Err(FfiError::new(
                ErrorCode::DatasetOpen,
                format!("dataset open: {err}"),
            ))
        }
        Err(err) => return Err(FfiError::new(ErrorCode::Runtime, format!("runtime: {err}"))),
    };

    let mut scan = dataset.scan();
    let query_arr = arrow::array::Float32Array::from(query_vec);
    scan.nearest(col_str, &query_arr, k)
        .map_err(|e| FfiError::new(ErrorCode::DatasetScan, format!("knn: {e}")))?;

    let dt: lance_linalg::distance::DistanceType = metric_str.try_into()
        .map_err(|_| FfiError::new(ErrorCode::InvalidArgument,
            format!("unknown metric: '{metric_str}', use 'l2', 'cosine', or 'dot'")))?;
    scan.distance_metric(dt);

    let stream = LanceStream::from_scanner(scan)
        .map_err(|e| FfiError::new(ErrorCode::StreamCreate, format!("knn stream: {e}")))?;
    Ok(StreamHandle::Lance(stream))
}
