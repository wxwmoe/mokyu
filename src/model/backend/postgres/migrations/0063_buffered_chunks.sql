ALTER TABLE chunk_encodings ADD COLUMN sync_pending BOOLEAN NOT NULL DEFAULT FALSE;
CREATE INDEX encodings_sync ON chunk_encodings(chunk_id) WHERE sync_pending AND state IN ('ready','uploading') AND quarantined_at IS NULL;
UPDATE mokyu_meta SET schema_version=63 WHERE singleton;
