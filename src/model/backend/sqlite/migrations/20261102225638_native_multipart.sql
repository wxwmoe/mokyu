CREATE TABLE native_uploads (
    upload_id BLOB NOT NULL PRIMARY KEY,
    storage_id BLOB NOT NULL,
    source_location BLOB NOT NULL UNIQUE,
    raw_id INTEGER,
    profile_revision INTEGER NOT NULL,
    upstream TEXT,
    manifest_hash TEXT,
    token BLOB,
    expires_at INTEGER,
    FOREIGN KEY (raw_id) REFERENCES raw_sources(id) ON DELETE SET NULL,
    FOREIGN KEY (upload_id) REFERENCES uploads(id) ON DELETE CASCADE
) STRICT, WITHOUT ROWID;
CREATE TABLE native_parts (
    upload_id BLOB NOT NULL,
    part_number INTEGER NOT NULL,
    stream_id INTEGER NOT NULL,
    file_id BLOB,
    node_id BLOB NOT NULL,
    size INTEGER NOT NULL,
    remote_etag TEXT,
    cached INTEGER NOT NULL DEFAULT 0,
    token BLOB,
    expires_at INTEGER,
    retry_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec')*1000000 AS INTEGER)),
    error TEXT,
    PRIMARY KEY (upload_id,part_number),
    FOREIGN KEY (upload_id) REFERENCES native_uploads(upload_id) ON DELETE CASCADE
) STRICT, WITHOUT ROWID;
CREATE INDEX native_parts_due ON native_parts(node_id,retry_at,expires_at);
CREATE TABLE node_native_work (
    node_id BLOB NOT NULL,
    operation_id BLOB NOT NULL,
    credential_generation INTEGER NOT NULL,
    boot_id BLOB NOT NULL,
    fence BLOB NOT NULL,
    target_stream INTEGER,
    token BLOB NOT NULL UNIQUE,
    storage_id BLOB NOT NULL,
    work TEXT NOT NULL,
    expires_at INTEGER NOT NULL,
    result_hash BLOB,
    created_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec')*1000000 AS INTEGER)),
    PRIMARY KEY (node_id,operation_id),
    FOREIGN KEY (node_id) REFERENCES nodes(id)
) STRICT, WITHOUT ROWID;
CREATE INDEX node_native_work_due ON node_native_work(node_id,expires_at);
CREATE TABLE native_part_reads (
    token BLOB NOT NULL PRIMARY KEY,
    stream_id INTEGER NOT NULL,
    expires_at INTEGER NOT NULL,
    FOREIGN KEY (stream_id) REFERENCES streams(id) ON DELETE CASCADE
) STRICT, WITHOUT ROWID;
CREATE INDEX native_part_reads_stream ON native_part_reads(stream_id,expires_at);
CREATE INDEX native_part_reads_expiry ON native_part_reads(expires_at);
CREATE INDEX node_native_work_expiry ON node_native_work(expires_at);
ALTER TABLE multipart_files ADD COLUMN request_hash BLOB;

UPDATE mokyu_meta SET schema_version = 20261102225638;
