CREATE TABLE IF NOT EXISTS bucket_domains (
    host VARCHAR(253) CHARACTER SET ascii COLLATE ascii_bin NOT NULL PRIMARY KEY,
    bucket_id BINARY(16) NOT NULL,
    CONSTRAINT bucket_domains_bucket FOREIGN KEY (bucket_id) REFERENCES buckets(id) ON DELETE CASCADE
) ENGINE=InnoDB;

UPDATE mokyu_meta SET schema_version=8 WHERE id=1;
