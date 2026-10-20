ALTER TABLE storage_profiles ADD COLUMN write_processing text NOT NULL DEFAULT 'inline';

CREATE TABLE raw_processing (
    stream_id uuid PRIMARY KEY REFERENCES streams(id) ON DELETE CASCADE,
    source_id uuid NOT NULL REFERENCES raw_sources(id) ON DELETE CASCADE,
    payload_revision bigint NOT NULL,
    content_hash bytea CHECK (content_hash IS NULL OR octet_length(content_hash)=32),
    source_revision bigint,
    token uuid,
    lease_expires_at timestamptz,
    next_attempt_at timestamptz NOT NULL DEFAULT now(),
    attempts integer NOT NULL DEFAULT 0,
    last_error text
);
CREATE INDEX raw_processing_due ON raw_processing(next_attempt_at,stream_id);

UPDATE mokyu_meta SET schema_version=49 WHERE singleton;
