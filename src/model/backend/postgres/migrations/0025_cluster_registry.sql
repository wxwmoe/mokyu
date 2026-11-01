-- Cluster roles and node leases are introduced together with their registry.
CREATE TABLE nodes (
    id uuid PRIMARY KEY,
    name text NOT NULL UNIQUE CHECK (octet_length(name) BETWEEN 1 AND 128),
    kind text NOT NULL CHECK (kind IN ('control','worker','data')),
    advertise_url text,
    credential_encrypted bytea,
    enabled boolean NOT NULL DEFAULT true,
    last_seen_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CHECK ((kind = 'control' AND advertise_url IS NOT NULL) OR kind <> 'control')
);
CREATE INDEX nodes_kind_enabled ON nodes(kind,enabled);

CREATE TABLE node_join_tokens (
    id uuid PRIMARY KEY,
    node_id uuid NOT NULL REFERENCES nodes ON DELETE CASCADE,
    token_hash bytea NOT NULL UNIQUE CHECK (octet_length(token_hash) = 32),
    expires_at timestamptz NOT NULL,
    consumed_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX node_join_tokens_active ON node_join_tokens(token_hash,expires_at)
    WHERE consumed_at IS NULL;

CREATE TABLE control_leases (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    node_id uuid REFERENCES nodes ON DELETE SET NULL,
    lease_token uuid NOT NULL DEFAULT gen_random_uuid(),
    expires_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);
INSERT INTO control_leases(singleton) VALUES (true);

ALTER TABLE tasks
    ADD COLUMN lease_node_id uuid REFERENCES nodes ON DELETE SET NULL,
    ADD COLUMN lease_token uuid,
    ADD COLUMN lease_expires_at timestamptz;
CREATE INDEX tasks_lease_expiry ON tasks(lease_expires_at)
    WHERE state = 'running';

UPDATE mokyu_meta SET schema_version=25;
