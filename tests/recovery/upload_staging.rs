use super::*;
use crate::config::UploadSettings;

#[tokio::test]
#[ignore = "requires an initialized isolated MOKYU_TEST_PAYLOAD_CONFIG database"]
async fn multipart_staging_pins_backend_and_policy_for_the_session() -> Result<()> {
    assert_eq!(
        std::env::var("MOKYU_TEST_ALLOW_STATE_CHANGES")?,
        "isolated-only"
    );
    let backend = super::super::queries::storage_payload::test_backend().await?;
    let mut tx = super::super::queries::storage_payload::begin(&backend).await?;
    let (bucket, other, a, b) = q::fixture(&mut tx).await?;
    let first = Uuid::now_v7();
    let mut settings = UploadSettings::default();
    q::configure(&settings).execute(&mut *tx).await?;
    q::upload(first, bucket).execute(&mut *tx).await?;
    pin(&mut tx, first, bucket).await?;
    let original = q::load(first).fetch_one(&mut *tx).await?;
    assert_eq!(original.storage_id, Some(a));
    assert_eq!(original.tail_upload, "immediate");
    assert_eq!(original.tail_wait_seconds, 600);

    settings.staging_storage_id = Some(a);
    settings.tail_upload = "on_demand_timeout".into();
    settings.tail_upload_timeout = "120s".into();
    q::configure(&settings).execute(&mut *tx).await?;
    let second = Uuid::now_v7();
    q::upload(second, other).execute(&mut *tx).await?;
    pin(&mut tx, second, other).await?;
    let selected = q::load(second).fetch_one(&mut *tx).await?;
    assert_eq!(selected.storage_id, Some(a));
    assert_eq!(selected.tail_upload, "on_demand_timeout");
    assert_eq!(selected.tail_wait_seconds, 120);
    assert!(selected.policy_revision > original.policy_revision);

    settings.staging_storage_id = None;
    settings.tail_upload = "on_demand".into();
    q::configure(&settings).execute(&mut *tx).await?;
    let third = Uuid::now_v7();
    q::upload(third, other).execute(&mut *tx).await?;
    pin(&mut tx, third, other).await?;
    let fallback = q::load(third).fetch_one(&mut *tx).await?;
    assert_eq!(fallback.storage_id, Some(b));
    assert_eq!(fallback.tail_upload, "on_demand");
    let retained = q::load(first).fetch_one(&mut *tx).await?;
    assert_eq!(retained.storage_id, original.storage_id);
    assert_eq!(retained.policy_revision, original.policy_revision);
    assert_eq!(retained.tail_upload, "immediate");

    settings.staging_storage_id = Some(Uuid::now_v7());
    q::configure(&settings).execute(&mut *tx).await?;
    let invalid = Uuid::now_v7();
    q::upload(invalid, bucket).execute(&mut *tx).await?;
    assert!(pin(&mut tx, invalid, bucket).await.is_err());
    assert!(q::load(invalid).fetch_optional(&mut *tx).await?.is_none());
    tx.rollback().await?;
    Ok(())
}
