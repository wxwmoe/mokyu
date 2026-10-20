ALTER TABLE storage_profiles ADD COLUMN write_processing varchar(16) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT 'inline';

CREATE TABLE raw_processing (
    stream_id binary(16) NOT NULL PRIMARY KEY,
    source_id binary(16) NOT NULL,
    payload_revision bigint NOT NULL,
    content_hash binary(32),
    source_revision bigint,
    token binary(16),
    lease_expires_at datetime(6),
    next_attempt_at datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    attempts int NOT NULL DEFAULT 0,
    last_error text,
    CONSTRAINT raw_processing_stream_fk FOREIGN KEY(stream_id) REFERENCES streams(id) ON DELETE CASCADE,
    CONSTRAINT raw_processing_source_fk FOREIGN KEY(source_id) REFERENCES raw_sources(id) ON DELETE CASCADE,
    INDEX raw_processing_due(next_attempt_at,stream_id)
) ENGINE=InnoDB;

UPDATE mokyu_meta SET schema_version=23 WHERE id=1;
