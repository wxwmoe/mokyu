CREATE TABLE node_runtime_samples (
    node_id uuid NOT NULL REFERENCES nodes(id) ON DELETE CASCADE,
    boot_id uuid NOT NULL,
    sequence bigint NOT NULL CHECK (sequence > 0),
    received_at timestamptz NOT NULL DEFAULT now(),
    uptime_seconds bigint NOT NULL CHECK (uptime_seconds >= 0),
    data jsonb NOT NULL CHECK (octet_length(data::text) <= 16384),
    PRIMARY KEY (node_id, boot_id, sequence)
);
CREATE INDEX node_runtime_time ON node_runtime_samples (node_id, received_at DESC);
CREATE INDEX node_runtime_expire ON node_runtime_samples (received_at);
CREATE INDEX runtime_cluster_time ON runtime_samples (sampled_at DESC) WHERE data->>'scope'='cluster';
UPDATE mokyu_meta SET schema_version=42;
