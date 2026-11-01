ALTER TABLE pending_uploads ADD COLUMN encoding_policy TEXT CHECK (encoding_policy IS NULL OR json_valid(encoding_policy));
UPDATE mokyu_meta SET schema_version=32 WHERE id=1;
