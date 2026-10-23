ALTER TABLE storage_profiles ADD COLUMN read_source_validation TEXT NOT NULL DEFAULT 'strict' CONSTRAINT storage_source_validation CHECK(read_source_validation IN ('strict','background'));
ALTER TABLE storage_profiles ADD COLUMN source_validation_max_age TEXT NOT NULL DEFAULT '5s';
UPDATE mokyu_meta SET schema_version=39 WHERE id=1;
