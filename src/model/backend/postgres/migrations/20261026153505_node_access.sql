CREATE TABLE node_access (
    node_id uuid PRIMARY KEY REFERENCES nodes(id) ON DELETE CASCADE,
    boot_id uuid NOT NULL,
    sequence bigint NOT NULL CHECK (sequence > 0),
    request_hash bytea NOT NULL CHECK (octet_length(request_hash) = 32),
    received_at timestamptz NOT NULL,
    gap_since timestamptz NOT NULL
);

UPDATE mokyu_meta SET schema_version=86 WHERE singleton;
