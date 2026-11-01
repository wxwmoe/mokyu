ALTER TABLE chunk_encodings ADD COLUMN sync_pending INTEGER NOT NULL DEFAULT FALSE CHECK(sync_pending IN (0,1));
CREATE INDEX encodings_sync ON chunk_encodings(chunk_id) WHERE sync_pending AND state IN ('ready','uploading') AND quarantined_at IS NULL;
UPDATE mokyu_meta SET schema_version=35 WHERE id=1;
