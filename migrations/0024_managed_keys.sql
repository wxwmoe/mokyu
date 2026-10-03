-- Managed keys are generated and encrypted by Mokyu; callers provide names only.
CREATE TABLE managed_keys (
    id uuid PRIMARY KEY,
    name text NOT NULL UNIQUE CHECK (octet_length(name) BETWEEN 1 AND 128),
    purpose text NOT NULL CHECK (purpose IN ('access','signing','encryption')),
    algorithm text NOT NULL DEFAULT 'raw'
        CHECK (algorithm IN ('raw','aes-256-gcm','chacha20-poly1305')),
    active boolean NOT NULL DEFAULT false,
    secret_encrypted bytea NOT NULL,
    fingerprint bytea NOT NULL CHECK (octet_length(fingerprint) = 32),
    created_at timestamptz NOT NULL DEFAULT now(),
    revoked_at timestamptz
);

ALTER TABLE managed_keys
    ADD CONSTRAINT managed_keys_active_encryption
        CHECK (NOT active OR purpose = 'encryption');
CREATE UNIQUE INDEX managed_keys_one_active_encryption
    ON managed_keys(purpose)
    WHERE active AND revoked_at IS NULL;

UPDATE mokyu_meta SET schema_version=24;
