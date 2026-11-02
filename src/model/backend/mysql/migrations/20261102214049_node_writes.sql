CREATE TABLE node_writes (
    node_id BINARY(16) NOT NULL,
    operation_id BINARY(16) NOT NULL,
    lease_id BINARY(16) NOT NULL UNIQUE,
    boot_id BINARY(16) NOT NULL,
    fence BINARY(16) NOT NULL,
    credential_generation BIGINT NOT NULL,
    request_hash BINARY(32) NOT NULL,
    stream_id BIGINT,
    bucket_id BINARY(16) NOT NULL,
    storage_id BINARY(16) NOT NULL,
    block_key_id BINARY(16),
    profile JSON NOT NULL,
    intent JSON NOT NULL,
    copy_source JSON,
    next_offset BIGINT NOT NULL DEFAULT 0,
    next_batch BIGINT NOT NULL DEFAULT 1,
    phase VARCHAR(12) NOT NULL DEFAULT 'active',
    publish_hash BINARY(32),
    published JSON,
    expires_at DATETIME(6) NOT NULL,
    updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (node_id,operation_id),
    FOREIGN KEY (node_id) REFERENCES nodes(id),
    FOREIGN KEY (stream_id) REFERENCES streams(id) ON DELETE SET NULL
) ENGINE=InnoDB;
CREATE INDEX node_writes_expiry ON node_writes(phase,expires_at);
CREATE INDEX node_writes_retention ON node_writes(phase,updated_at);
CREATE TABLE node_write_raw (
    node_id BINARY(16) NOT NULL,
    operation_id BINARY(16) NOT NULL,
    request_hash BINARY(32) NOT NULL,
    source_id BIGINT NOT NULL,
    location_id BINARY(16) NOT NULL,
    size BIGINT NOT NULL,
    digest BINARY(16) NOT NULL,
    content_hash BINARY(32),
    committed BOOLEAN NOT NULL,
    PRIMARY KEY (node_id,operation_id),
    FOREIGN KEY (node_id,operation_id) REFERENCES node_writes(node_id,operation_id) ON DELETE CASCADE
) ENGINE=InnoDB;
CREATE TABLE node_write_permits (
    token BINARY(16) NOT NULL PRIMARY KEY,
    node_id BINARY(16) NOT NULL,
    operation_id BINARY(16) NOT NULL,
    FOREIGN KEY (token) REFERENCES storage_permits(token) ON DELETE CASCADE,
    FOREIGN KEY (node_id,operation_id) REFERENCES node_writes(node_id,operation_id) ON DELETE CASCADE
) ENGINE=InnoDB;

UPDATE mokyu_meta SET schema_version = 20261102214049;
