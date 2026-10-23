ALTER TABLE storage_profiles ADD COLUMN cors_cache_enabled INTEGER NOT NULL DEFAULT 0 CHECK (cors_cache_enabled IN (0, 1));
UPDATE mokyu_meta SET schema_version=42 WHERE id=1;
