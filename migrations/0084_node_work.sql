CREATE TABLE node_work (
    node_id uuid NOT NULL REFERENCES nodes(id) ON DELETE CASCADE,
    operation_id uuid NOT NULL,
    credential_generation bigint NOT NULL,
    boot_id uuid NOT NULL,
    fence uuid NOT NULL,
    request_hash bytea NOT NULL CHECK (octet_length(request_hash)=32),
    task_id uuid REFERENCES tasks(id) ON DELETE SET NULL,
    token uuid NOT NULL UNIQUE,
    maintenance_token uuid NOT NULL UNIQUE,
    plan jsonb,
    storage_encrypted bytea,
    expires_at timestamptz NOT NULL,
    result_hash bytea CHECK (result_hash IS NULL OR octet_length(result_hash)=32),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (node_id,operation_id),
    CHECK ((plan IS NULL)=(storage_encrypted IS NULL))
);
CREATE INDEX node_work_expiry ON node_work(expires_at) WHERE result_hash IS NULL;
CREATE INDEX node_work_task ON node_work(task_id);

UPDATE mokyu_meta SET schema_version=84 WHERE singleton;
