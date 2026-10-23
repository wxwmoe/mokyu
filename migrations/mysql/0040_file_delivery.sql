SET @mokyu_delivery_ddl = IF(EXISTS(SELECT 1 FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='storage_profiles' AND COLUMN_NAME='file_delivery'), 'SELECT 1', 'ALTER TABLE storage_profiles ADD COLUMN file_delivery VARCHAR(32) NOT NULL DEFAULT ''application'', ADD COLUMN file_delivery_prefix VARCHAR(512) NULL, ADD CONSTRAINT storage_file_delivery CHECK(file_delivery IN (''application'',''x_accel_redirect'',''x_sendfile'')), ADD CONSTRAINT storage_delivery_prefix CHECK((file_delivery=''application'' AND file_delivery_prefix IS NULL) OR (file_delivery<>''application'' AND file_delivery_prefix IS NOT NULL))');
PREPARE mokyu_file_delivery FROM @mokyu_delivery_ddl;
EXECUTE mokyu_file_delivery;
DEALLOCATE PREPARE mokyu_file_delivery;
UPDATE mokyu_meta SET schema_version=40 WHERE id=1;
