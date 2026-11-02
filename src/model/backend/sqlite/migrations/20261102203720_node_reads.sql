CREATE TABLE storage_gates (
    storage_id BLOB NOT NULL,
    direction TEXT NOT NULL,
    PRIMARY KEY (storage_id,direction),
    FOREIGN KEY (storage_id) REFERENCES storages(id) ON DELETE CASCADE
) STRICT, WITHOUT ROWID;
CREATE TABLE storage_permits (
    token BLOB NOT NULL PRIMARY KEY,
    storage_id BLOB NOT NULL,
    direction TEXT NOT NULL,
    holder BLOB NOT NULL,
    expires_at INTEGER NOT NULL,
    FOREIGN KEY (storage_id,direction) REFERENCES storage_gates(storage_id,direction) ON DELETE CASCADE
) STRICT, WITHOUT ROWID;
CREATE INDEX storage_permit_usage ON storage_permits(storage_id,direction,expires_at);
CREATE TABLE node_reads (
    read_id BLOB NOT NULL PRIMARY KEY,
    node_id BLOB NOT NULL,
    boot_id BLOB NOT NULL,
    fence BLOB NOT NULL,
    credential_generation INTEGER NOT NULL,
    operation_id BLOB NOT NULL,
    request_hash BLOB NOT NULL,
    stream_id INTEGER NOT NULL,
    payload_revision INTEGER NOT NULL,
    offset_bytes INTEGER NOT NULL,
    end_bytes INTEGER NOT NULL,
    expires_at INTEGER NOT NULL,
    UNIQUE (node_id,fence,operation_id),
    FOREIGN KEY (node_id) REFERENCES nodes(id),
    FOREIGN KEY (stream_id) REFERENCES streams(id),
    CONSTRAINT node_reads_offsets CHECK (offset_bytes>=0 AND end_bytes>=offset_bytes),
    CONSTRAINT node_reads_generation CHECK (credential_generation>0 AND payload_revision>0)
) STRICT, WITHOUT ROWID;
CREATE INDEX node_reads_expiry ON node_reads(expires_at);
CREATE INDEX node_reads_stream ON node_reads(stream_id,expires_at);
CREATE TABLE node_read_windows (
    id BLOB NOT NULL PRIMARY KEY,
    read_id BLOB NOT NULL,
    offset_bytes INTEGER NOT NULL,
    mapping TEXT NOT NULL,
    UNIQUE (read_id,offset_bytes),
    FOREIGN KEY (read_id) REFERENCES node_reads(read_id) ON DELETE CASCADE
) STRICT, WITHOUT ROWID;
CREATE TABLE node_read_sources (
    window_id BLOB NOT NULL,
    source_type TEXT NOT NULL,
    location_id BLOB NOT NULL,
    PRIMARY KEY (window_id,source_type,location_id),
    FOREIGN KEY (window_id) REFERENCES node_read_windows(id) ON DELETE CASCADE
) STRICT, WITHOUT ROWID;
CREATE INDEX node_read_locations ON node_read_sources(source_type,location_id);
CREATE TABLE node_read_permits (
    token BLOB NOT NULL,
    read_id BLOB NOT NULL,
    PRIMARY KEY(token,read_id),
    FOREIGN KEY (token) REFERENCES storage_permits(token) ON DELETE CASCADE,
    FOREIGN KEY (read_id) REFERENCES node_reads(read_id) ON DELETE CASCADE
) STRICT, WITHOUT ROWID;
CREATE INDEX node_read_permits_read ON node_read_permits(read_id);

UPDATE mokyu_meta SET schema_version = 20261102203720;
