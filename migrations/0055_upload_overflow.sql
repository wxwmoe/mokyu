ALTER TABLE storage_profiles ADD COLUMN upload_cache_overflow TEXT NOT NULL DEFAULT 'fallback' CONSTRAINT storage_upload_overflow_check CHECK (upload_cache_overflow IN ('fallback','reject'));
UPDATE mokyu_meta SET schema_version=55;
