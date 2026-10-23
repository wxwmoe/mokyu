SET @mokyu_cors_ddl = IF(EXISTS(SELECT 1 FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='storage_profiles' AND COLUMN_NAME='cors_cache_enabled'), 'SELECT 1', 'ALTER TABLE storage_profiles ADD COLUMN cors_cache_enabled BOOLEAN NOT NULL DEFAULT FALSE');
PREPARE mokyu_cors_cache FROM @mokyu_cors_ddl;
EXECUTE mokyu_cors_cache;
DEALLOCATE PREPARE mokyu_cors_cache;
UPDATE mokyu_meta SET schema_version=41 WHERE id=1;
