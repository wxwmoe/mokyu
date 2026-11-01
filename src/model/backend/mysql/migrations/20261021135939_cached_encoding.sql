SET @mokyu_encoding_ddl = IF(EXISTS(SELECT 1 FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='pending_uploads' AND COLUMN_NAME='encoding_policy'), 'SELECT 1', 'ALTER TABLE pending_uploads ADD COLUMN encoding_policy JSON');
PREPARE mokyu_encoding FROM @mokyu_encoding_ddl;
EXECUTE mokyu_encoding;
DEALLOCATE PREPARE mokyu_encoding;
UPDATE mokyu_meta SET schema_version=31 WHERE id=1;
