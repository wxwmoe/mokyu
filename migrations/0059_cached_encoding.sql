ALTER TABLE pending_uploads ADD COLUMN encoding_policy JSONB;
UPDATE mokyu_meta SET schema_version=59 WHERE singleton;
