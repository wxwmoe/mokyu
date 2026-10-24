CREATE TABLE node_reads (
    read_id uuid PRIMARY KEY REFERENCES stream_leases(id) ON DELETE CASCADE,
    node_id uuid NOT NULL REFERENCES nodes(id),
    credential_generation bigint NOT NULL CHECK (credential_generation > 0),
    boot_id uuid NOT NULL,
    fence uuid NOT NULL,
    operation_id uuid NOT NULL,
    request_hash bytea NOT NULL CHECK (octet_length(request_hash)=32),
    payload_revision bigint NOT NULL CHECK (payload_revision > 0),
    offset_bytes bigint NOT NULL CHECK (offset_bytes >= 0),
    end_bytes bigint NOT NULL CHECK (end_bytes > offset_bytes),
    UNIQUE (node_id, fence, operation_id)
);

CREATE TABLE node_read_permits (
    token uuid PRIMARY KEY REFERENCES storage_permits(token) ON DELETE CASCADE,
    read_id uuid NOT NULL REFERENCES node_reads(read_id) ON DELETE CASCADE
);
CREATE INDEX node_read_permits_read ON node_read_permits(read_id);

CREATE TABLE node_read_windows (
    id uuid PRIMARY KEY DEFAULT uuidv7(),
    read_id uuid NOT NULL REFERENCES node_reads(read_id) ON DELETE CASCADE,
    offset_bytes bigint NOT NULL CHECK (offset_bytes >= 0),
    end_bytes bigint NOT NULL CHECK (end_bytes > offset_bytes),
    mapping jsonb NOT NULL CHECK (octet_length(mapping::text) <= 4194304),
    UNIQUE (read_id, offset_bytes)
);

CREATE TABLE node_read_sources (
    window_id uuid NOT NULL REFERENCES node_read_windows(id) ON DELETE CASCADE,
    kind text NOT NULL CHECK (kind IN ('chunk','pack','raw','fragment')),
    location_id uuid NOT NULL,
    PRIMARY KEY (window_id, kind, location_id)
);
CREATE INDEX node_read_sources_location ON node_read_sources(kind, location_id);

UPDATE mokyu_meta SET schema_version=73 WHERE singleton;
