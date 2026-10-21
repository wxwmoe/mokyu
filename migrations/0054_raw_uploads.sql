
ALTER TABLE raw_sources ADD COLUMN pending_upload boolean NOT NULL DEFAULT false;
CREATE TABLE raw_uploads (
    source_id uuid PRIMARY KEY REFERENCES raw_sources(id) ON DELETE CASCADE,
    token uuid,
    deduplicate boolean NOT NULL,
    background boolean NOT NULL,
    content_hash bytea,
    next_attempt_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT
);
CREATE INDEX raw_uploads_due ON raw_uploads(next_attempt_at,source_id);
UPDATE mokyu_meta SET schema_version=54;
