CREATE TABLE node_write_local (
    node_id uuid NOT NULL,
    operation_id uuid NOT NULL,
    request_hash bytea NOT NULL CHECK (octet_length(request_hash)=32),
    size bigint NOT NULL CHECK (size BETWEEN 0 AND 5368709120),
    PRIMARY KEY (node_id,operation_id),
    FOREIGN KEY (node_id,operation_id) REFERENCES node_writes(node_id,operation_id) ON DELETE CASCADE
);

CREATE TABLE node_native_work (
    node_id uuid NOT NULL REFERENCES nodes(id) ON DELETE CASCADE,
    operation_id uuid NOT NULL,
    credential_generation bigint NOT NULL,
    boot_id uuid NOT NULL,
    fence uuid NOT NULL,
    target_stream uuid,
    token uuid NOT NULL UNIQUE,
    storage_id uuid NOT NULL REFERENCES storages(id),
    read_lease uuid REFERENCES stream_leases(id) ON DELETE SET NULL,
    work jsonb NOT NULL,
    expires_at timestamptz NOT NULL,
    result_hash bytea CHECK (result_hash IS NULL OR octet_length(result_hash)=32),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (node_id,operation_id)
);
CREATE INDEX node_native_work_expiry ON node_native_work(expires_at) WHERE result_hash IS NULL;

ALTER TABLE node_write_raw DROP CONSTRAINT node_write_raw_size_check;
ALTER TABLE node_write_raw ADD CHECK (size>=0);

UPDATE mokyu_meta SET schema_version=83 WHERE singleton;
