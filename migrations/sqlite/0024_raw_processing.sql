ALTER TABLE storage_profiles ADD COLUMN write_processing TEXT NOT NULL DEFAULT 'inline';

CREATE TABLE raw_processing (
    stream_id BLOB PRIMARY KEY REFERENCES streams(id) ON DELETE CASCADE,
    source_id BLOB NOT NULL REFERENCES raw_sources(id) ON DELETE CASCADE,
    payload_revision INTEGER NOT NULL,
    content_hash BLOB CHECK (content_hash IS NULL OR length(content_hash)=32),
    source_revision INTEGER,
    token BLOB,
    lease_expires_at INTEGER,
    next_attempt_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec')*1000000 AS INTEGER)),
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT
) STRICT;
CREATE INDEX raw_processing_due ON raw_processing(next_attempt_at,stream_id);

UPDATE mokyu_meta SET schema_version=24 WHERE id=1;
