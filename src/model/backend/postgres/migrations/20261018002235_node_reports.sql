CREATE TABLE node_reports (
    node_id uuid PRIMARY KEY REFERENCES nodes(id) ON DELETE CASCADE,
    boot_id uuid NOT NULL,
    sequence bigint NOT NULL DEFAULT 0 CHECK (sequence >= 0),
    started_at timestamptz NOT NULL,
    sampled_at timestamptz,
    received_at timestamptz NOT NULL DEFAULT now(),
    metadata jsonb NOT NULL DEFAULT '{}' CHECK (octet_length(metadata::text) <= 65536),
    status jsonb NOT NULL DEFAULT '{}' CHECK (octet_length(status::text) <= 32768)
);
CREATE INDEX tasks_node_running ON tasks (lease_node_id, updated_at DESC, id) WHERE state='running';
UPDATE mokyu_meta SET schema_version=41;
