SET @mokyu_memory_ddl = IF(EXISTS(SELECT 1 FROM information_schema.CHECK_CONSTRAINTS WHERE CONSTRAINT_SCHEMA=DATABASE() AND CONSTRAINT_NAME='storage_write_durability' AND CHECK_CLAUSE NOT LIKE '%volatile%'), CONCAT('ALTER TABLE storage_profiles ', IF(LOCATE('MariaDB', VERSION())>0, 'DROP CONSTRAINT ', 'DROP CHECK '), 'storage_write_durability, ADD CONSTRAINT storage_write_durability CHECK(write_durability IN (''durable'',''buffered'',''volatile''))'), 'SELECT 1');
PREPARE mokyu_memory FROM @mokyu_memory_ddl;
EXECUTE mokyu_memory;
DEALLOCATE PREPARE mokyu_memory;
SET @mokyu_memory_ddl = IF(EXISTS(SELECT 1 FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='chunk_encodings' AND COLUMN_NAME='boot_id'), 'SELECT 1', 'ALTER TABLE chunk_encodings ADD COLUMN boot_id BINARY(16) NULL, ADD CONSTRAINT encoding_memory_sync CHECK(boot_id IS NULL OR sync_pending)');
PREPARE mokyu_memory FROM @mokyu_memory_ddl;
EXECUTE mokyu_memory;
DEALLOCATE PREPARE mokyu_memory;
UPDATE mokyu_meta SET schema_version=35 WHERE id=1;
