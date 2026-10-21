SET @mokyu_cdc_ddl = IF(EXISTS(SELECT 1 FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='raw_sources' AND COLUMN_NAME='processing_task'), 'SELECT 1', 'ALTER TABLE raw_sources ADD COLUMN processing_task BINARY(16), ADD INDEX raw_sources_processing(processing_task), ADD CONSTRAINT raw_sources_processing_fk FOREIGN KEY(processing_task) REFERENCES tasks(id) ON DELETE SET NULL');
PREPARE mokyu_cdc FROM @mokyu_cdc_ddl;
EXECUTE mokyu_cdc;
DEALLOCATE PREPARE mokyu_cdc;
UPDATE mokyu_meta SET schema_version=32 WHERE id=1;
