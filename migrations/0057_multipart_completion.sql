ALTER TABLE uploads ADD COLUMN completion_token uuid;
ALTER TABLE uploads ADD COLUMN completion_expires_at timestamptz;
UPDATE mokyu_meta SET schema_version=57 WHERE singleton;
