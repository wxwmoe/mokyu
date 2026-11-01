ALTER TABLE buckets ADD COLUMN image_revision INTEGER NOT NULL DEFAULT 0;
ALTER TABLE buckets ADD COLUMN image_families INTEGER NOT NULL DEFAULT 0;
ALTER TABLE buckets ADD COLUMN image_policy TEXT;
ALTER TABLE buckets ADD COLUMN image_import_token TEXT;

CREATE TABLE image_styles (
    bucket_id BLOB NOT NULL,
    name TEXT COLLATE BINARY NOT NULL,
    enabled BOOLEAN NOT NULL,
    body TEXT NOT NULL,
    PRIMARY KEY (bucket_id, name),
    FOREIGN KEY (bucket_id) REFERENCES buckets(id) ON DELETE CASCADE
);

UPDATE mokyu_meta SET schema_version=56 WHERE id=1;
