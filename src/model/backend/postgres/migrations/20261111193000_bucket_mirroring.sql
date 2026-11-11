CREATE TABLE bucket_mirroring (
    bucket_id UUID PRIMARY KEY REFERENCES buckets(id) ON DELETE CASCADE,
    revision BIGINT NOT NULL CHECK (revision > 0),
    enabled BOOLEAN NOT NULL DEFAULT FALSE,
    config_encrypted BYTEA NOT NULL
);

CREATE TABLE mirroring_keys (
    bucket_id UUID NOT NULL REFERENCES buckets(id) ON DELETE CASCADE,
    object_key TEXT NOT NULL,
    blocked BOOLEAN NOT NULL DEFAULT FALSE,
    task_id UUID REFERENCES tasks(id) ON DELETE SET NULL,
    revision BIGINT NOT NULL DEFAULT 0,
    external_id UUID,
    external_revision BIGINT,
    missing_until BIGINT NOT NULL DEFAULT 0,
    retry_after BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (bucket_id, object_key)
);

CREATE UNIQUE INDEX mirroring_keys_task_idx ON mirroring_keys(task_id);

UPDATE mokyu_meta SET schema_version = 20261111193000;
