ALTER TABLE chunk_encodings ADD COLUMN owner_stream BLOB
    CHECK (owner_stream IS NULL OR length(owner_stream)=16)
    REFERENCES streams(id) ON DELETE RESTRICT
    CONSTRAINT encodings_owner_exclusive CHECK (owner_task IS NULL OR owner_stream IS NULL);
ALTER TABLE chunk_encodings ADD COLUMN recovery_pending INTEGER NOT NULL DEFAULT FALSE CHECK (recovery_pending IN (0,1));
CREATE INDEX encodings_owner_stream ON chunk_encodings(owner_stream) WHERE owner_stream IS NOT NULL;
CREATE INDEX encodings_recovery ON chunk_encodings(storage_id,unreferenced_at,encoding_id) WHERE recovery_pending;
UPDATE mokyu_meta SET schema_version=20 WHERE id=1;
