CREATE TABLE bucket_mirroring (
    bucket_id BLOB NOT NULL PRIMARY KEY REFERENCES buckets(id) ON DELETE CASCADE,
    revision INTEGER NOT NULL CHECK (revision > 0),
    enabled INTEGER NOT NULL DEFAULT FALSE CHECK (enabled IN (0,1)),
    config_encrypted BLOB NOT NULL
);

CREATE TABLE mirroring_keys (
    bucket_id BLOB NOT NULL REFERENCES buckets(id) ON DELETE CASCADE,
    object_key BLOB NOT NULL,
    blocked INTEGER NOT NULL DEFAULT FALSE CHECK (blocked IN (0,1)),
    task_id BLOB REFERENCES tasks(id) ON DELETE SET NULL,
    revision INTEGER NOT NULL DEFAULT 0,
    external_id BLOB,
    external_revision INTEGER,
    missing_until INTEGER NOT NULL DEFAULT 0,
    retry_after INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (bucket_id, object_key)
);

CREATE UNIQUE INDEX mirroring_keys_task_idx ON mirroring_keys(task_id);

UPDATE mokyu_meta SET schema_version = 20261111193000;
