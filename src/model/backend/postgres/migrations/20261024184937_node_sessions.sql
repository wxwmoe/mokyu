ALTER TABLE nodes ADD COLUMN credential_generation bigint NOT NULL DEFAULT 1 CHECK (credential_generation > 0);

CREATE TABLE node_boots (
    node_id uuid NOT NULL REFERENCES nodes(id) ON DELETE CASCADE,
    credential_generation bigint NOT NULL CHECK (credential_generation > 0),
    boot_id uuid NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    retired_at timestamptz,
    PRIMARY KEY (node_id, boot_id)
);

CREATE TABLE node_sessions (
    node_id uuid PRIMARY KEY REFERENCES nodes(id) ON DELETE CASCADE,
    credential_generation bigint NOT NULL CHECK (credential_generation > 0),
    boot_id uuid NOT NULL,
    fence uuid NOT NULL,
    epoch bigint NOT NULL CHECK (epoch > 0),
    connection_id uuid NOT NULL,
    expires_at timestamptz NOT NULL,
    sequence bigint NOT NULL DEFAULT 0 CHECK (sequence >= 0),
    draining boolean NOT NULL DEFAULT false,
    FOREIGN KEY (node_id, boot_id) REFERENCES node_boots(node_id, boot_id)
);

UPDATE mokyu_meta SET schema_version=72 WHERE singleton;
