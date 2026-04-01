use std::pin::Pin;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use lance::dataset::scanner::{DatasetRecordBatchStream, Scanner};
use lance::io::RecordBatchStream;
use tokio::runtime::Handle;

/// A stream wrapper that bridges async Lance streams to synchronous C FFI calls.
pub struct LanceStream {
    handle: Handle,
    stream: Pin<Box<DatasetRecordBatchStream>>,
}

impl LanceStream {
    pub fn from_scanner(scanner: Scanner) -> Result<Self, Box<dyn std::error::Error>> {
        let handle = crate::runtime::handle()?;
        let stream = handle.block_on(async { scanner.try_into_stream().await })?;
        Ok(Self {
            handle,
            stream: Box::pin(stream),
        })
    }

    pub fn schema(&self) -> SchemaRef {
        self.stream.schema()
    }

    pub fn next(&mut self) -> Result<Option<RecordBatch>, lance::Error> {
        use futures::StreamExt;
        self.handle.block_on(async {
            match self.stream.next().await {
                Some(Ok(batch)) => Ok(Some(batch)),
                Some(Err(err)) => Err(err),
                None => Ok(None),
            }
        })
    }
}
