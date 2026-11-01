ALTER TABLE chunk_encodings
    ADD COLUMN owner_stream uuid REFERENCES streams(id) ON DELETE RESTRICT,
    ADD COLUMN recovery_pending boolean NOT NULL DEFAULT false,
    ADD CONSTRAINT encodings_owner_exclusive CHECK (owner_task IS NULL OR owner_stream IS NULL);
CREATE INDEX encodings_owner_stream ON chunk_encodings(owner_stream) WHERE owner_stream IS NOT NULL;
CREATE INDEX encodings_recovery ON chunk_encodings(storage_id,unreferenced_at,encoding_id) WHERE recovery_pending;
UPDATE mokyu_meta SET schema_version=44 WHERE singleton;
