CREATE TABLE bucket_mirroring (
    bucket_id BINARY(16) NOT NULL PRIMARY KEY,
    revision BIGINT NOT NULL CHECK (revision > 0),
    enabled BOOLEAN NOT NULL DEFAULT FALSE,
    config_encrypted MEDIUMBLOB NOT NULL,
    CONSTRAINT bucket_mirroring_bucket FOREIGN KEY (bucket_id) REFERENCES buckets(id) ON DELETE CASCADE
) ENGINE=InnoDB;

CREATE TABLE mirroring_keys (
    bucket_id BINARY(16) NOT NULL,
    object_key VARBINARY(1024) NOT NULL,
    blocked BOOLEAN NOT NULL DEFAULT FALSE,
    task_id BINARY(16),
    revision BIGINT NOT NULL DEFAULT 0,
    external_id BINARY(16),
    external_revision BIGINT,
    missing_until BIGINT NOT NULL DEFAULT 0,
    retry_after BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (bucket_id, object_key),
    CONSTRAINT mirroring_keys_bucket FOREIGN KEY (bucket_id) REFERENCES buckets(id) ON DELETE CASCADE,
    CONSTRAINT mirroring_keys_task FOREIGN KEY (task_id) REFERENCES tasks(id) ON DELETE SET NULL
) ENGINE=InnoDB;

CREATE UNIQUE INDEX mirroring_keys_task_idx ON mirroring_keys(task_id);

UPDATE mokyu_meta SET schema_version = 20261111193000;
