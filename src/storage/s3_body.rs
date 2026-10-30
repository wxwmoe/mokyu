use anyhow::{Context, Result, ensure};
use bytes::Bytes;
use futures_util::{Stream, StreamExt, stream::BoxStream};
use sha2::Digest;
use std::future::Future;
use tokio::{
    sync::{mpsc, watch},
    time::Instant,
};

pub(in crate::storage) fn upload_body<'a, D: Digest + Send + 'a>(
    mut source: BoxStream<'a, Result<Bytes>>,
    length: u64,
    digest: &'a [u8],
) -> (
    impl Stream<Item = Result<Bytes, std::io::Error>> + Send + 'static,
    impl Future<Output = Result<bool>> + Send + 'a,
    watch::Receiver<Instant>,
) {
    let (sender, receiver) = mpsc::channel::<Bytes>(2);
    let (progress, activity) = watch::channel(Instant::now());
    let body =
        futures_util::stream::unfold((receiver, progress), |(mut receiver, progress)| async {
            let bytes = receiver.recv().await?;
            progress.send_replace(Instant::now());
            Some((Ok(bytes), (receiver, progress)))
        });
    let feed = async move {
        let mut sent = 0u64;
        let mut actual = D::new();
        while let Some(bytes) = source.next().await {
            let bytes = bytes?;
            sent = sent
                .checked_add(bytes.len() as u64)
                .context("S3 upload length overflow")?;
            ensure!(sent <= length, "S3 upload source exceeded its length");
            actual.update(&bytes);
            for start in (0..bytes.len()).step_by(256 * 1024) {
                if sender
                    .send(bytes.slice(start..(start + 256 * 1024).min(bytes.len())))
                    .await
                    .is_err()
                {
                    return Ok(false);
                }
            }
        }
        ensure!(sent == length, "S3 upload source was truncated");
        ensure!(
            actual.finalize().as_slice() == digest,
            "S3 upload replay checksum mismatch"
        );
        drop(sender);
        Ok(true)
    };
    (body, feed, activity)
}
