CREATE TABLE node_writes (
    node_id uuid NOT NULL REFERENCES nodes(id),
    operation_id uuid NOT NULL,
    credential_generation bigint NOT NULL CHECK (credential_generation > 0),
    boot_id uuid NOT NULL,
    fence uuid NOT NULL,
    request_hash bytea NOT NULL CHECK (octet_length(request_hash)=32),
    stream_id uuid UNIQUE REFERENCES streams(id) ON DELETE SET NULL,
    lease_id uuid UNIQUE REFERENCES stream_leases(id) ON DELETE SET NULL,
    storage_id uuid REFERENCES storages(id) ON DELETE SET NULL,
    block_key_id uuid REFERENCES managed_keys(id),
    profile jsonb NOT NULL CHECK (jsonb_typeof(profile)='object' AND octet_length(profile::text)<=32768),
    state text NOT NULL DEFAULT 'active' CHECK (state IN ('active','aborted')),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    completed_at timestamptz,
    PRIMARY KEY (node_id,operation_id),
    CHECK ((state='active')=(completed_at IS NULL))
);
CREATE INDEX node_writes_cleanup ON node_writes(node_id,created_at);
CREATE INDEX node_writes_key ON node_writes(block_key_id) WHERE state='active' AND block_key_id IS NOT NULL;

UPDATE mokyu_meta SET schema_version=75 WHERE singleton;
