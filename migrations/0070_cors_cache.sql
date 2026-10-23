ALTER TABLE storage_profiles ADD COLUMN cors_cache_enabled BOOLEAN NOT NULL DEFAULT FALSE;
UPDATE mokyu_meta SET schema_version=70 WHERE singleton;
