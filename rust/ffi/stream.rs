use std::ffi::c_void;
use std::ptr;

use crate::error::{clear_last_error, set_last_error, ErrorCode};

use super::util::{stream_handle_mut, FfiError, FfiResult};

/// Get the next RecordBatch from the stream.
///
/// # Returns
/// * `0` - Batch available (written to *out_batch)
/// * `1` - End of stream (no more data)
/// * `-1` - Error occurred (check lance_c_last_error_message)
#[no_mangle]
pub unsafe extern "C" fn lance_c_stream_next(
    stream: *mut c_void,
    out_batch: *mut *mut c_void,
) -> i32 {
    match stream_next_inner(stream, out_batch) {
        Ok(v) => {
            clear_last_error();
            v
        }
        Err(err) => {
            set_last_error(err.code, err.message);
            -1
        }
    }
}

fn stream_next_inner(stream: *mut c_void, out_batch: *mut *mut c_void) -> FfiResult<i32> {
    if out_batch.is_null() {
        return Err(FfiError::new(
            ErrorCode::InvalidArgument,
            "out_batch is null",
        ));
    }
    unsafe {
        ptr::write_unaligned(out_batch, ptr::null_mut());
    }

    let stream = unsafe { stream_handle_mut(stream)? };
    match stream.next_batch() {
        Ok(Some(batch)) => {
            let batch_ptr = Box::into_raw(Box::new(batch)) as *mut c_void;
            unsafe {
                ptr::write_unaligned(out_batch, batch_ptr);
            }
            Ok(0)
        }
        Ok(None) => Ok(1),
        Err(err) => Err(FfiError::new(
            ErrorCode::StreamNext,
            format!("stream next: {err}"),
        )),
    }
}

#[no_mangle]
pub unsafe extern "C" fn lance_c_close_stream(stream: *mut c_void) {
    if !stream.is_null() {
        unsafe {
            let _ = Box::from_raw(stream as *mut super::types::StreamHandle);
        }
    }
}
