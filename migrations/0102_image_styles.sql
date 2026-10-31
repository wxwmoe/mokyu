ALTER TABLE buckets ADD COLUMN image_revision BIGINT NOT NULL DEFAULT 0;
ALTER TABLE buckets ADD COLUMN image_families BIGINT NOT NULL DEFAULT 0;
ALTER TABLE buckets ADD COLUMN image_policy TEXT;
ALTER TABLE buckets ADD COLUMN image_import_token VARCHAR(64);

CREATE TABLE image_styles (
    bucket_id uuid NOT NULL,
    name VARCHAR(128) COLLATE "C" NOT NULL,
    enabled BOOLEAN NOT NULL,
    body TEXT NOT NULL,
    PRIMARY KEY (bucket_id, name),
    FOREIGN KEY (bucket_id) REFERENCES buckets(id) ON DELETE CASCADE
);

UPDATE mokyu_meta SET schema_version=102 WHERE singleton;
