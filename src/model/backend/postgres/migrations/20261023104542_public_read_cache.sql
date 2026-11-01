ALTER TABLE storage_profiles ADD COLUMN public_cache_validation TEXT NOT NULL DEFAULT 'strict' CONSTRAINT storage_public_cache_validation CHECK(public_cache_validation IN ('strict','background'));
ALTER TABLE storage_profiles ADD COLUMN public_cache_refresh_interval TEXT NOT NULL DEFAULT '1s';
ALTER TABLE storage_profiles ADD COLUMN public_cache_max_age TEXT NOT NULL DEFAULT '5s';
UPDATE mokyu_meta SET schema_version=66 WHERE singleton;
