SET @mokyu_overflow_ddl = IF(EXISTS(SELECT 1 FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='storage_profiles' AND COLUMN_NAME='upload_cache_overflow'), 'SELECT 1', 'ALTER TABLE storage_profiles ADD COLUMN upload_cache_overflow VARCHAR(16) NOT NULL DEFAULT ''fallback'', ADD CONSTRAINT storage_upload_overflow_check CHECK (upload_cache_overflow IN (''fallback'',''reject''))');
PREPARE mokyu_overflow FROM @mokyu_overflow_ddl;
EXECUTE mokyu_overflow;
DEALLOCATE PREPARE mokyu_overflow;
UPDATE mokyu_meta SET schema_version=29 WHERE id=1;
