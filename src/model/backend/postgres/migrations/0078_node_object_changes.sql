CREATE TABLE node_object_changes (
    node_id uuid NOT NULL REFERENCES nodes(id) ON DELETE CASCADE,
    operation_id uuid NOT NULL,
    credential_generation bigint NOT NULL,
    request_hash bytea NOT NULL CHECK (octet_length(request_hash)=32),
    outcome jsonb NOT NULL CHECK (outcome IN ('"applied"'::jsonb,'"no_such_key"'::jsonb)),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (node_id,operation_id)
);

CREATE INDEX node_object_changes_expiry ON node_object_changes(node_id,created_at);

UPDATE mokyu_meta SET schema_version=78 WHERE singleton;
