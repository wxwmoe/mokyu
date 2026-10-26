use crate::status::stats::Counters;
use bytes::Bytes;
use futures_util::{StreamExt, stream::BoxStream};
use object_store::{
    CopyOptions, GetOptions, GetResult, GetResultPayload, ListResult, MultipartId, MultipartUpload,
    ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions,
    Result,
    multipart::{MultipartStore, PartId},
    path::Path,
};
use std::{ops::Range, sync::Arc};

#[derive(Default)]
pub(super) struct Io {
    pub(super) read: Arc<Counters>,
    pub(super) write: Arc<Counters>,
    delete: Arc<Counters>,
    control: Arc<Counters>,
}

impl Io {
    pub(super) fn snapshot(&self) -> serde_json::Value {
        serde_json::json!({"read": self.read.snapshot(), "write": self.write.snapshot(),
            "delete": self.delete.snapshot(), "control": self.control.snapshot()})
    }

    pub(super) fn observe(self: &Arc<Self>, inner: Arc<dyn ObjectStore>) -> Arc<dyn ObjectStore> {
        Arc::new(Observed {
            inner,
            io: self.clone(),
        })
    }

    pub(super) fn multipart(
        self: &Arc<Self>,
        inner: Arc<dyn MultipartStore>,
    ) -> Arc<dyn MultipartStore> {
        Arc::new(Multipart {
            inner,
            io: self.clone(),
        })
    }
}

struct Observed {
    inner: Arc<dyn ObjectStore>,
    io: Arc<Io>,
}

impl std::fmt::Display for Observed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("NodeStorage")
    }
}
impl std::fmt::Debug for Observed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}

#[async_trait::async_trait]
impl ObjectStore for Observed {
    async fn put_opts(
        &self,
        path: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> Result<PutResult> {
        let mut operation = self.io.write.begin();
        let bytes = payload.content_length() as u64;
        let result = self.inner.put_opts(path, payload, options).await;
        if result.is_ok() {
            self.io.write.bytes(bytes);
        }
        operation.finish(result.is_err());
        result
    }

    async fn put_multipart_opts(
        &self,
        path: &Path,
        options: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(path, options).await
    }

    async fn get_opts(&self, path: &Path, options: GetOptions) -> Result<GetResult> {
        let mut operation = self.io.read.begin();
        let head = options.head;
        let result = self.inner.get_opts(path, options).await;
        if head || result.is_err() {
            operation.finish(result.is_err());
        }
        let mut result = result?;
        let counts = self.io.read.clone();
        if let GetResultPayload::Stream(mut body) = result.payload {
            result.payload = GetResultPayload::Stream(Box::pin(async_stream::stream! {
                while let Some(value) = body.next().await {
                    match &value {
                        Ok(bytes) => counts.bytes(bytes.len() as u64),
                        Err(_) => operation.finish(true),
                    }
                    yield value;
                }
                operation.finish(false);
            }));
        } else {
            operation.finish(false);
        }
        Ok(result)
    }

    async fn get_ranges(&self, path: &Path, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        let mut operation = self.io.read.begin();
        let result = self.inner.get_ranges(path, ranges).await;
        if let Ok(bytes) = &result {
            self.io
                .read
                .bytes(bytes.iter().map(|v| v.len() as u64).sum());
        }
        operation.finish(result.is_err());
        result
    }

    fn delete_stream(
        &self,
        paths: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        observed_stream(self.inner.delete_stream(paths), self.io.delete.clone())
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        observed_stream(self.inner.list(prefix), self.io.control.clone())
    }
    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        observed_stream(
            self.inner.list_with_offset(prefix, offset),
            self.io.control.clone(),
        )
    }
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        let mut operation = self.io.control.begin();
        let result = self.inner.list_with_delimiter(prefix).await;
        operation.finish(result.is_err());
        result
    }
    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        let mut operation = self.io.control.begin();
        let result = self.inner.copy_opts(from, to, options).await;
        operation.finish(result.is_err());
        result
    }
    async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> Result<()> {
        let mut operation = self.io.control.begin();
        let result = self.inner.rename_opts(from, to, options).await;
        operation.finish(result.is_err());
        result
    }
}

fn observed_stream<T: Send + 'static>(
    mut stream: BoxStream<'static, Result<T>>,
    counts: Arc<Counters>,
) -> BoxStream<'static, Result<T>> {
    Box::pin(async_stream::stream! {
        let mut operation = counts.begin();
        while let Some(value) = stream.next().await {
            if value.is_err() { operation.finish(true); }
            yield value;
        }
        operation.finish(false);
    })
}

struct Multipart {
    inner: Arc<dyn MultipartStore>,
    io: Arc<Io>,
}

#[async_trait::async_trait]
impl MultipartStore for Multipart {
    async fn create_multipart(&self, path: &Path) -> Result<MultipartId> {
        self.create_multipart_opts(path, PutMultipartOptions::default())
            .await
    }
    async fn create_multipart_opts(
        &self,
        path: &Path,
        options: PutMultipartOptions,
    ) -> Result<MultipartId> {
        let mut operation = self.io.control.begin();
        let result = self.inner.create_multipart_opts(path, options).await;
        operation.finish(result.is_err());
        result
    }
    async fn put_part(
        &self,
        path: &Path,
        id: &MultipartId,
        index: usize,
        data: PutPayload,
    ) -> Result<PartId> {
        let mut operation = self.io.write.begin();
        let bytes = data.content_length() as u64;
        let result = self.inner.put_part(path, id, index, data).await;
        if result.is_ok() {
            self.io.write.bytes(bytes);
        }
        operation.finish(result.is_err());
        result
    }
    async fn complete_multipart(
        &self,
        path: &Path,
        id: &MultipartId,
        parts: Vec<PartId>,
    ) -> Result<PutResult> {
        let mut operation = self.io.control.begin();
        let result = self.inner.complete_multipart(path, id, parts).await;
        operation.finish(result.is_err());
        result
    }
    async fn abort_multipart(&self, path: &Path, id: &MultipartId) -> Result<()> {
        let mut operation = self.io.control.begin();
        let result = self.inner.abort_multipart(path, id).await;
        operation.finish(result.is_err());
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::{ObjectStoreExt, memory::InMemory};

    #[tokio::test]
    async fn observes_consumed_bytes_failures_and_cancellation() {
        let io = Arc::new(Io::default());
        let store = io.observe(Arc::new(InMemory::new()));
        let path = Path::from("object");
        store
            .put(&path, Bytes::from_static(b"content").into())
            .await
            .unwrap();
        store.head(&path).await.unwrap();
        assert_eq!(
            store.get(&path).await.unwrap().bytes().await.unwrap(),
            "content"
        );
        drop(store.get(&path).await.unwrap());
        assert!(store.get(&Path::from("missing")).await.is_err());
        store.delete(&path).await.unwrap();
        let status = io.snapshot();
        assert_eq!(status["read"]["bytes"], 7);
        assert_eq!(status["read"]["failed"], 1);
        assert_eq!(status["read"]["canceled"], 1);
        assert_eq!(status["read"]["active"], 0);
        assert_eq!(status["write"]["bytes"], 7);
        assert_eq!(status["delete"]["completed"], 1);
    }
}
