ALTER TABLE uploads ADD COLUMN completion_token BLOB;
ALTER TABLE uploads ADD COLUMN completion_expires_at INTEGER;
UPDATE mokyu_meta SET schema_version=31 WHERE id=1;
