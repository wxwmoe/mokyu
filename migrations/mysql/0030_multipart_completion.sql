SET @mokyu_completion_ddl = IF(EXISTS(SELECT 1 FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='uploads' AND COLUMN_NAME='completion_token'), 'SELECT 1', 'ALTER TABLE uploads ADD COLUMN completion_token BINARY(16), ADD COLUMN completion_expires_at DATETIME(6)');
PREPARE mokyu_completion FROM @mokyu_completion_ddl;
EXECUTE mokyu_completion;
DEALLOCATE PREPARE mokyu_completion;
UPDATE mokyu_meta SET schema_version=30 WHERE id=1;
