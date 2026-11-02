CREATE TABLE native_uploads (
    upload_id BINARY(16) NOT NULL PRIMARY KEY,
    storage_id BINARY(16) NOT NULL,
    source_location BINARY(16) NOT NULL UNIQUE,
    raw_id BIGINT,
    profile_revision BIGINT NOT NULL,
    upstream TEXT,
    manifest_hash VARCHAR(128),
    token BINARY(16),
    expires_at DATETIME(6),
    FOREIGN KEY (raw_id) REFERENCES raw_sources(id) ON DELETE SET NULL,
    FOREIGN KEY (upload_id) REFERENCES uploads(id) ON DELETE CASCADE
) ENGINE=InnoDB;
CREATE TABLE native_parts (
    upload_id BINARY(16) NOT NULL,
    part_number INTEGER NOT NULL,
    stream_id BIGINT NOT NULL,
    file_id BINARY(16),
    node_id BINARY(16) NOT NULL,
    size BIGINT NOT NULL,
    remote_etag TEXT,
    cached BOOLEAN NOT NULL DEFAULT 0,
    token BINARY(16),
    expires_at DATETIME(6),
    retry_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    error VARCHAR(128),
    PRIMARY KEY (upload_id,part_number),
    FOREIGN KEY (upload_id) REFERENCES native_uploads(upload_id) ON DELETE CASCADE
) ENGINE=InnoDB;
CREATE INDEX native_parts_due ON native_parts(node_id,retry_at,expires_at);
CREATE TABLE node_native_work (
    node_id BINARY(16) NOT NULL,
    operation_id BINARY(16) NOT NULL,
    credential_generation BIGINT NOT NULL,
    boot_id BINARY(16) NOT NULL,
    fence BINARY(16) NOT NULL,
    target_stream BIGINT,
    token BINARY(16) NOT NULL UNIQUE,
    storage_id BINARY(16) NOT NULL,
    work JSON NOT NULL,
    expires_at DATETIME(6) NOT NULL,
    result_hash BINARY(32),
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (node_id,operation_id),
    FOREIGN KEY (node_id) REFERENCES nodes(id)
) ENGINE=InnoDB;
CREATE INDEX node_native_work_due ON node_native_work(node_id,expires_at);
CREATE TABLE native_part_reads (
    token BINARY(16) NOT NULL PRIMARY KEY,
    stream_id BIGINT NOT NULL,
    expires_at DATETIME(6) NOT NULL,
    FOREIGN KEY (stream_id) REFERENCES streams(id) ON DELETE CASCADE
) ENGINE=InnoDB;
CREATE INDEX native_part_reads_stream ON native_part_reads(stream_id,expires_at);
CREATE INDEX native_part_reads_expiry ON native_part_reads(expires_at);
CREATE INDEX node_native_work_expiry ON node_native_work(expires_at);
ALTER TABLE multipart_files ADD COLUMN request_hash BINARY(32);

UPDATE mokyu_meta SET schema_version = 20261102225638;
