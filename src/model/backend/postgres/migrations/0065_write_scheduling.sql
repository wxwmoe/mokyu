ALTER TABLE storage_profiles ADD COLUMN write_flush_interval BIGINT NOT NULL DEFAULT 0;
ALTER TABLE storage_profiles ADD COLUMN write_backlog_max_age BIGINT NOT NULL DEFAULT 300;
ALTER TABLE storage_profiles ADD COLUMN background_scheduling TEXT NOT NULL DEFAULT 'balanced' CONSTRAINT storage_background_scheduling CHECK(background_scheduling IN ('balanced','foreground'));
UPDATE mokyu_meta SET schema_version=65 WHERE singleton;
