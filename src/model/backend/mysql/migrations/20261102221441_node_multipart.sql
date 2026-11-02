CREATE TABLE staging_objects (
    storage_id BINARY(16) NOT NULL,
    object_key VARCHAR(256) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    size BIGINT NOT NULL,
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (storage_id,object_key),
    FOREIGN KEY (storage_id) REFERENCES storages(id)
) ENGINE=InnoDB;
CREATE INDEX staging_objects_age ON staging_objects(created_at);
ALTER TABLE fragments ADD COLUMN staging_storage_id BINARY(16);
ALTER TABLE fragments ADD COLUMN staging_object_key VARCHAR(256) CHARACTER SET ascii COLLATE ascii_bin;
CREATE INDEX fragments_staging ON fragments(staging_storage_id,staging_object_key);
CREATE TABLE node_write_tails (
    node_id BINARY(16) NOT NULL,
    operation_id BINARY(16) NOT NULL,
    request_hash BINARY(32) NOT NULL,
    fragment_id BIGINT NOT NULL,
    location_id BINARY(16) NOT NULL,
    storage_id BINARY(16) NOT NULL,
    object_key VARCHAR(256) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    offset_bytes BIGINT NOT NULL,
    size INTEGER NOT NULL,
    committed BOOLEAN NOT NULL DEFAULT 0,
    PRIMARY KEY (node_id,operation_id),
    FOREIGN KEY (node_id,operation_id) REFERENCES node_writes(node_id,operation_id) ON DELETE CASCADE
) ENGINE=InnoDB;
CREATE TABLE node_completions (
    upload_id BINARY(16) NOT NULL PRIMARY KEY,
    manifest_hash VARCHAR(96) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    conditions JSON NOT NULL,
    summary JSON NOT NULL,
    FOREIGN KEY (upload_id) REFERENCES uploads(id) ON DELETE CASCADE
) ENGINE=InnoDB;
CREATE TABLE node_completion_parts (
    upload_id BINARY(16) NOT NULL,
    ordinal INTEGER NOT NULL,
    stream_id BIGINT NOT NULL,
    part_number INTEGER NOT NULL,
    size BIGINT NOT NULL,
    etag VARCHAR(96) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    checksums JSON NOT NULL,
    PRIMARY KEY (upload_id,ordinal),
    FOREIGN KEY (upload_id) REFERENCES node_completions(upload_id) ON DELETE CASCADE
) ENGINE=InnoDB;
CREATE INDEX node_completion_parts_stream ON node_completion_parts(stream_id);

UPDATE mokyu_meta SET schema_version = 20261102221441;
