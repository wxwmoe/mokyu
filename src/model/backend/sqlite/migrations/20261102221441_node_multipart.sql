CREATE TABLE staging_objects (
    storage_id BLOB NOT NULL,
    object_key TEXT NOT NULL,
    size INTEGER NOT NULL,
    created_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec')*1000000 AS INTEGER)),
    PRIMARY KEY (storage_id,object_key),
    FOREIGN KEY (storage_id) REFERENCES storages(id)
) STRICT, WITHOUT ROWID;
CREATE INDEX staging_objects_age ON staging_objects(created_at);
ALTER TABLE fragments ADD COLUMN staging_storage_id BLOB;
ALTER TABLE fragments ADD COLUMN staging_object_key TEXT;
CREATE INDEX fragments_staging ON fragments(staging_storage_id,staging_object_key);
CREATE TABLE node_write_tails (
    node_id BLOB NOT NULL,
    operation_id BLOB NOT NULL,
    request_hash BLOB NOT NULL,
    fragment_id INTEGER NOT NULL,
    location_id BLOB NOT NULL,
    storage_id BLOB NOT NULL,
    object_key TEXT NOT NULL,
    offset_bytes INTEGER NOT NULL,
    size INTEGER NOT NULL,
    committed INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (node_id,operation_id),
    FOREIGN KEY (node_id,operation_id) REFERENCES node_writes(node_id,operation_id) ON DELETE CASCADE
) STRICT, WITHOUT ROWID;
CREATE TABLE node_completions (
    upload_id BLOB NOT NULL PRIMARY KEY,
    manifest_hash TEXT NOT NULL,
    conditions TEXT NOT NULL,
    summary TEXT NOT NULL,
    FOREIGN KEY (upload_id) REFERENCES uploads(id) ON DELETE CASCADE
) STRICT, WITHOUT ROWID;
CREATE TABLE node_completion_parts (
    upload_id BLOB NOT NULL,
    ordinal INTEGER NOT NULL,
    stream_id INTEGER NOT NULL,
    part_number INTEGER NOT NULL,
    size INTEGER NOT NULL,
    etag TEXT NOT NULL,
    checksums TEXT NOT NULL,
    PRIMARY KEY (upload_id,ordinal),
    FOREIGN KEY (upload_id) REFERENCES node_completions(upload_id) ON DELETE CASCADE
) STRICT, WITHOUT ROWID;
CREATE INDEX node_completion_parts_stream ON node_completion_parts(stream_id);

UPDATE mokyu_meta SET schema_version = 20261102221441;
