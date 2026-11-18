use super::*;
use crate::storage::journal::snapshot::BucketCut;
use crate::{content::codec::Key, model::journal::record::BlockKey};

#[test]
fn online_recovery_keeps_history_and_escaped_current_keys() -> Result<()> {
    use crate::model::journal::record::{Attributes, History};
    let bucket = Uuid::new_v4();
    let key = Key::new("aes-256-gcm", [9; 32])?;
    let format = Format {
        storage: Uuid::new_v4(),
        journal: Uuid::new_v4(),
        epoch: Uuid::new_v4(),
        encrypted: true,
        key: &key,
    };
    let mut catalog = Catalog::temporary(None)?;
    catalog.scope(
        &[BucketCut {
            id: bucket,
            name: "media".into(),
            generation: 1,
            sequence: 3,
            stopped: false,
        }],
        &[],
    )?;
    for (n, (slot, history)) in [
        ("~~m", None),
        ("~m", Some(History::State { mode: 1 })),
        (
            "~h/null/~m",
            Some(History::Version {
                order: 1,
                time: 0,
                marker: 0,
            }),
        ),
    ]
    .into_iter()
    .enumerate()
    {
        catalog.row(
            Row::Object {
                bucket,
                generation: 1,
                seq: n as i64 + 1,
                key: slot.into(),
                attributes: Attributes {
                    origin: None,
                    history,
                    size: 0,
                    etag: String::new(),
                    metadata: serde_json::json!({}),
                    checksums: serde_json::json!({}),
                    payload: "chunks".into(),
                    modified_us: 0,
                },
                range_count: 0,
                range_digest: [0; 32],
            },
            &format,
        )?;
    }
    catalog.finish()?;
    let view = crate::storage::journal::view::View::open(catalog)?;
    assert_eq!(view.buckets[&bucket].objects, "1");
    let mut after = String::new();
    for slot in ["~h/null/~m", "~m", "~~m"] {
        let change = view.next_object(bucket, 1, &after)?.unwrap();
        assert_eq!(change.key.as_deref(), Some(slot));
        after = slot.into();
    }
    assert!(view.next_object(bucket, 1, &after)?.is_none());
    Ok(())
}

#[test]
fn recovery_keeps_only_wrapped_keys_and_published_generations() -> Result<()> {
    let mut catalog = Catalog::new(Connection::open_in_memory()?);
    catalog.db.execute_batch(Catalog::SCHEMA)?;
    let id = Uuid::new_v4();
    let crypto = Key::new("aes-256-gcm", [29; 32])?;
    let format = Format {
        storage: Uuid::new_v4(),
        journal: Uuid::new_v4(),
        epoch: Uuid::new_v4(),
        encrypted: false,
        key: &crypto,
    };
    let key = BlockKey {
        id: Uuid::new_v4(),
        name: "archive".into(),
        algorithm: "aes-256-gcm".into(),
        material: [73; 32],
    };
    catalog.stage(Record::Key(WrappedKey::seal(&key, &format)?), &format)?;
    let bytes: Vec<u8> = catalog
        .db
        .query_row("SELECT value FROM keys", [], |r| r.get(0))?;
    assert!(!bytes.windows(32).any(|b| b == key.material));
    assert_eq!(
        serde_json::from_slice::<WrappedKey>(&bytes)?
            .open(&format)?
            .material,
        key.material
    );
    catalog.scope(
        &[BucketCut {
            id,
            name: "media".into(),
            generation: 1,
            sequence: 3,
            stopped: true,
        }],
        &[],
    )?;
    let change = Change {
        bucket: id,
        generation: 2,
        seq: 4,
        name: "media".into(),
        operation: Operation::Start,
        key: None,
        attributes: None,
        ranges: vec![],
    };
    catalog.stage(
        Record::Change {
            value: change.clone(),
            range_count: 0,
            range_digest: [0; 32],
        },
        &format,
    )?;
    catalog.apply()?;
    catalog.stage(
        Record::Change {
            value: change,
            range_count: 0,
            range_digest: [0; 32],
        },
        &format,
    )?;
    catalog.apply()?;
    catalog.finish()?;
    assert_eq!(
        catalog
            .db
            .query_row("SELECT generation,paused FROM buckets", [], |r| Ok((
                r.get::<_, i64>(0)?,
                r.get::<_, bool>(1)?
            )))?,
        (1, true)
    );
    Ok(())
}
