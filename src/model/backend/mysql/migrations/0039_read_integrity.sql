SET @mokyu_integrity_ddl = IF(EXISTS(SELECT 1 FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='storage_profiles' AND COLUMN_NAME='read_integrity'), 'SELECT 1', 'ALTER TABLE storage_profiles ADD COLUMN read_integrity VARCHAR(32) NOT NULL DEFAULT ''inline'', ADD CONSTRAINT storage_read_integrity CHECK(read_integrity IN (''inline'',''background'',''off''))');
PREPARE mokyu_read_integrity FROM @mokyu_integrity_ddl;
EXECUTE mokyu_read_integrity;
DEALLOCATE PREPARE mokyu_read_integrity;
UPDATE mokyu_meta SET schema_version=39 WHERE id=1;
