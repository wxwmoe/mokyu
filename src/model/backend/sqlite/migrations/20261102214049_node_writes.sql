CREATE TABLE node_writes (
    node_id BLOB NOT NULL,
    operation_id BLOB NOT NULL,
    lease_id BLOB NOT NULL UNIQUE,
    boot_id BLOB NOT NULL,
    fence BLOB NOT NULL,
    credential_generation INTEGER NOT NULL,
    request_hash BLOB NOT NULL,
    stream_id INTEGER,
    bucket_id BLOB NOT NULL,
    storage_id BLOB NOT NULL,
    block_key_id BLOB,
    profile TEXT NOT NULL,
    intent TEXT NOT NULL,
    copy_source TEXT,
    next_offset INTEGER NOT NULL DEFAULT 0,
    next_batch INTEGER NOT NULL DEFAULT 1,
    phase TEXT NOT NULL DEFAULT 'active',
    publish_hash BLOB,
    published TEXT,
    expires_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec')*1000000 AS INTEGER)),
    PRIMARY KEY (node_id,operation_id),
    FOREIGN KEY (node_id) REFERENCES nodes(id),
    FOREIGN KEY (stream_id) REFERENCES streams(id) ON DELETE SET NULL
) STRICT, WITHOUT ROWID;
CREATE INDEX node_writes_expiry ON node_writes(phase,expires_at);
CREATE INDEX node_writes_retention ON node_writes(phase,updated_at);
CREATE TABLE node_write_raw (
    node_id BLOB NOT NULL,
    operation_id BLOB NOT NULL,
    request_hash BLOB NOT NULL,
    source_id INTEGER NOT NULL,
    location_id BLOB NOT NULL,
    size INTEGER NOT NULL,
    digest BLOB NOT NULL,
    content_hash BLOB,
    committed INTEGER NOT NULL,
    PRIMARY KEY (node_id,operation_id),
    FOREIGN KEY (node_id,operation_id) REFERENCES node_writes(node_id,operation_id) ON DELETE CASCADE
) STRICT, WITHOUT ROWID;
CREATE TABLE node_write_permits (
    token BLOB NOT NULL PRIMARY KEY,
    node_id BLOB NOT NULL,
    operation_id BLOB NOT NULL,
    FOREIGN KEY (token) REFERENCES storage_permits(token) ON DELETE CASCADE,
    FOREIGN KEY (node_id,operation_id) REFERENCES node_writes(node_id,operation_id) ON DELETE CASCADE
) STRICT, WITHOUT ROWID;
CREATE INDEX node_write_permits_operation ON node_write_permits(node_id,operation_id);

UPDATE mokyu_meta SET schema_version = 20261102214049;
