SET @mokyu_source_ddl = IF(EXISTS(SELECT 1 FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='storage_profiles' AND COLUMN_NAME='read_source_validation'), 'SELECT 1', 'ALTER TABLE storage_profiles ADD COLUMN read_source_validation VARCHAR(32) NOT NULL DEFAULT ''strict'', ADD CONSTRAINT storage_source_validation CHECK(read_source_validation IN (''strict'',''background''))');
PREPARE mokyu_source FROM @mokyu_source_ddl;
EXECUTE mokyu_source;
DEALLOCATE PREPARE mokyu_source;
SET @mokyu_source_ddl = IF(EXISTS(SELECT 1 FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='storage_profiles' AND COLUMN_NAME='source_validation_max_age'), 'SELECT 1', 'ALTER TABLE storage_profiles ADD COLUMN source_validation_max_age VARCHAR(32) NOT NULL DEFAULT ''5s''');
PREPARE mokyu_source FROM @mokyu_source_ddl;
EXECUTE mokyu_source;
DEALLOCATE PREPARE mokyu_source;
UPDATE mokyu_meta SET schema_version=38 WHERE id=1;
