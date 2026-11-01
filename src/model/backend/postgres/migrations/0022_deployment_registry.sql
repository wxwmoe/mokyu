-- 0.0.4 deployment identity. Storage and cluster records arrive in later migrations.
ALTER TABLE mokyu_meta
    ADD COLUMN deployment_mode text NOT NULL DEFAULT 'single'
        CHECK (deployment_mode IN ('single','cluster')),
    ADD COLUMN deployment_key_fingerprint bytea
        CHECK (deployment_key_fingerprint IS NULL OR octet_length(deployment_key_fingerprint) = 32);

UPDATE mokyu_meta SET schema_version=22;
