use std::future::Future;
use std::io;
use std::sync::OnceLock;

use tokio::runtime::{Handle, Runtime};

static RUNTIME: OnceLock<Result<Runtime, io::Error>> = OnceLock::new();

/// Create a Tokio multi-thread runtime with limited worker threads.
///
/// We use multi_thread because Lance internally uses tokio::spawn() for
/// parallel I/O operations (e.g., reading multiple fragments), which requires
/// a multi-thread runtime. current_thread deadlocks when Lance spawns tasks
/// inside block_on().
///
/// Fork safety: openGauss backends are forked from the postmaster. The runtime
/// is lazily initialized (OnceLock) on first FFI call, which happens in the
/// backend process AFTER fork. So the runtime is created per-backend and never
/// crosses a fork boundary.
///
/// Thread count is limited to 2 to minimize resource usage per backend.
fn create_runtime() -> Result<Runtime, io::Error> {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
}

pub fn runtime() -> Result<&'static Runtime, io::Error> {
    match RUNTIME.get_or_init(create_runtime) {
        Ok(rt) => Ok(rt),
        Err(err) => Err(io::Error::new(err.kind(), err.to_string())),
    }
}

pub fn handle() -> Result<Handle, io::Error> {
    Ok(runtime()?.handle().clone())
}

pub fn block_on<F: Future>(future: F) -> Result<F::Output, io::Error> {
    Ok(runtime()?.block_on(future))
}
