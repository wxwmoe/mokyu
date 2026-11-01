
ALTER TABLE raw_sources ADD COLUMN pending_upload INTEGER NOT NULL DEFAULT false;
CREATE TABLE raw_uploads (
    source_id INTEGER PRIMARY KEY REFERENCES raw_sources(id) ON DELETE CASCADE,
    token BLOB,
    deduplicate INTEGER NOT NULL,
    background INTEGER NOT NULL,
    content_hash BLOB,
    next_attempt_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec')*1000000 AS INTEGER)),
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT
);
CREATE INDEX raw_uploads_due ON raw_uploads(next_attempt_at,source_id);
UPDATE mokyu_meta SET schema_version=29 WHERE id=1;
