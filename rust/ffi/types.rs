use std::sync::Arc;

use arrow::datatypes::Schema;
use lance::Dataset;

use crate::scanner::LanceStream;

pub(crate) type SchemaHandle = Arc<Schema>;

pub(crate) struct DatasetHandle {
    pub(crate) dataset: Arc<Dataset>,
    pub(crate) arrow_schema: SchemaHandle,
}

impl DatasetHandle {
    pub(crate) fn new(dataset: Arc<Dataset>) -> Self {
        let arrow_schema: Schema = dataset.schema().into();
        let arrow_schema = Arc::new(arrow_schema);
        Self {
            dataset,
            arrow_schema,
        }
    }
}

pub(crate) enum StreamHandle {
    Lance(LanceStream),
}

impl StreamHandle {
    pub(crate) fn next_batch(
        &mut self,
    ) -> Result<Option<arrow::array::RecordBatch>, anyhow::Error> {
        match self {
            StreamHandle::Lance(stream) => stream.next().map_err(anyhow::Error::new),
        }
    }
}
