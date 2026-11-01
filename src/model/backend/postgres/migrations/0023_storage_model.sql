-- Storage targets are introduced before any bucket is bound to one.
CREATE TABLE storage_targets (
    id uuid PRIMARY KEY,
    name text NOT NULL UNIQUE CHECK (octet_length(name) BETWEEN 1 AND 128),
    type text NOT NULL CHECK (type IN ('local','s3')),
    path text,
    endpoint_url text,
    remote_bucket text,
    region text,
    prefix text NOT NULL DEFAULT '',
    access_key_encrypted bytea,
    secret_key_encrypted bytea,
    settings jsonb NOT NULL DEFAULT '{}',
    metadata jsonb NOT NULL DEFAULT '{}',
    enabled boolean NOT NULL DEFAULT true,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CHECK (
        (type = 'local' AND path IS NOT NULL AND endpoint_url IS NULL AND remote_bucket IS NULL
            AND region IS NULL AND access_key_encrypted IS NULL AND secret_key_encrypted IS NULL)
        OR
        (type = 's3' AND path IS NULL AND endpoint_url IS NOT NULL AND remote_bucket IS NOT NULL
            AND region IS NOT NULL AND access_key_encrypted IS NOT NULL AND secret_key_encrypted IS NOT NULL)
    )
);

ALTER TABLE buckets
    ADD COLUMN storage_target_id uuid REFERENCES storage_targets;
ALTER TABLE chunks
    ADD COLUMN storage_target_id uuid REFERENCES storage_targets;
ALTER TABLE packs
    ADD COLUMN storage_target_id uuid REFERENCES storage_targets;
ALTER TABLE chunk_locations
    ADD COLUMN storage_target_id uuid REFERENCES storage_targets;

CREATE INDEX buckets_storage_target ON buckets(storage_target_id);
CREATE INDEX chunks_storage_target ON chunks(storage_target_id) WHERE storage_target_id IS NOT NULL;
CREATE INDEX packs_storage_target ON packs(storage_target_id) WHERE storage_target_id IS NOT NULL;
CREATE INDEX chunk_locations_storage_target ON chunk_locations(storage_target_id)
    WHERE storage_target_id IS NOT NULL;

UPDATE mokyu_meta SET schema_version=23;
