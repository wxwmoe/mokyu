use super::{Active, App, StoredStream};
use crate::{authorization::Permit, model::history::Version};
use anyhow::Result;
use uuid::Uuid;

impl App {
    pub(crate) async fn history_read(
        &self,
        bucket: Uuid,
        key: &str,
        id: Option<&str>,
        permit: &Permit,
    ) -> Result<Option<(StoredStream, Active, String)>> {
        let _guard = self.coord.lock().await;
        self.ensure_mode()?;
        let ttl = if self.role == "single" || self.cluster.is_none() {
            None
        } else {
            Some(self.read_lease_seconds()?)
        };
        let Some((version, stream, lease)) =
            self.objects.version(permit, bucket, key, id, ttl).await?
        else {
            if id.is_some() {
                anyhow::bail!(s3s::s3_error!(NoSuchVersion))
            }
            return Ok(None);
        };
        let stream =
            stream.ok_or_else(|| marker(&version.version_id, version.modified_us, id.is_some()))?;
        let pin = if let Some(lease) = lease {
            self.pin_reader(stream.id, lease, ttl.unwrap())
        } else {
            self.pin(stream.id)
        };
        Ok(Some((stream, pin, version.version_id)))
    }
    pub(crate) async fn restore_version(
        &self,
        principal: &crate::authorization::Principal,
        bucket: Uuid,
        key: &str,
        version: &str,
    ) -> Result<Option<String>> {
        use crate::authorization::Action;
        let mut permit = Permit::for_action(principal.clone(), bucket, Action::RestoreVersion);
        permit.add(bucket, Action::ReadVersion);
        permit.add(bucket, Action::Write);
        let (src, pin, _) = self
            .history_read(bucket, key, Some(version), &permit)
            .await?
            .ok_or_else(|| s3s::s3_error!(NoSuchVersion))?;
        let (id, _write) = self
            .new_stream(
                &permit,
                bucket,
                key,
                crate::model::StreamPurpose::Object,
                src.metadata.clone(),
                false,
                None,
                None,
            )
            .await?;
        self.reserve_quota(id, src.size, false, None).await?;
        pin.with_read_lease(self.copy_extents(id, &src)).await?;
        self.finish_stream(id, src.size, &src.etag, src.checksums)
            .await?;
        self.publish(id, (bucket, key), None, None).await?;
        self.objects.published_version(id).await
    }
    pub(crate) async fn history_delete(
        &self,
        permit: &Permit,
        bucket: Uuid,
        key: &str,
        version: Option<&str>,
        condition: Option<s3s::dto::ETagCondition>,
    ) -> Result<Option<Version>> {
        use crate::model::history::DELETE_RECEIPT;
        if let Some(id) = version {
            anyhow::ensure!(condition.is_none(), s3s::s3_error!(InvalidRequest));
            self.writable()?;
            let _guard = self.coord.lock().await;
            let _cache = self.public_reads.object_change(
                bucket,
                key,
                self.deployment_mode() == "single_control",
            );
            let row = self.objects.delete_version(permit, bucket, key, id).await?;
            self.wake_gc.notify_one();
            self.wake_local_journal.notify_waiters();
            if self.deployment_mode() == "single_control" {
                self.notifications.committed();
            }
            Ok(row)
        } else {
            DELETE_RECEIPT
                .scope(std::cell::RefCell::new(None), async {
                    self.change_object(&permit.principal, bucket, key, None, None, condition)
                        .await?;
                    Ok(DELETE_RECEIPT.with(|c| c.borrow_mut().take()))
                })
                .await
        }
    }
}
pub(crate) fn marker(id: &str, modified_us: i64, explicit: bool) -> s3s::S3Error {
    let mut err = if explicit {
        s3s::s3_error!(MethodNotAllowed)
    } else {
        s3s::s3_error!(NoSuchKey)
    };
    let mut h = hyper::HeaderMap::new();
    h.insert(
        "x-amz-delete-marker",
        hyper::header::HeaderValue::from_static("true"),
    );
    if let Ok(id) = id.parse() {
        h.insert("x-amz-version-id", id);
    }
    if explicit
        && let Some(t) = chrono::DateTime::from_timestamp_micros(modified_us)
        && let Ok(last) = t.format("%a, %d %b %Y %H:%M:%S GMT").to_string().parse()
    {
        h.insert("last-modified", last);
    }
    err.set_headers(h);
    err
}
