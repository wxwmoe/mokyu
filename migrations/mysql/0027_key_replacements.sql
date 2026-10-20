CREATE TABLE IF NOT EXISTS key_replacements (
    id BINARY(16) PRIMARY KEY,
    actor VARCHAR(160) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    plan JSON NOT NULL,
    result JSON,
    expires_at TIMESTAMP(6) NOT NULL, INDEX key_replacements_expiry(expires_at)
) ENGINE=InnoDB;
UPDATE mokyu_meta SET schema_version=27 WHERE id=1;
