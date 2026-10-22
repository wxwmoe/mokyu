SET @mokyu_buffer_ddl = IF(EXISTS(SELECT 1 FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='storage_profiles' AND COLUMN_NAME='write_durability'), 'SELECT 1', 'ALTER TABLE storage_profiles ADD COLUMN write_durability VARCHAR(16) NOT NULL DEFAULT ''durable'', ADD CONSTRAINT storage_write_durability CHECK(write_durability IN (''durable'',''buffered''))');
PREPARE mokyu_buffer FROM @mokyu_buffer_ddl;
EXECUTE mokyu_buffer;
DEALLOCATE PREPARE mokyu_buffer;
SET @mokyu_buffer_ddl = IF(EXISTS(SELECT 1 FROM information_schema.STATISTICS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='raw_sources' AND INDEX_NAME='raw_sources_sync'), 'SELECT 1', 'CREATE INDEX raw_sources_sync ON raw_sources(durability,state,quarantine_code,id)');
PREPARE mokyu_buffer FROM @mokyu_buffer_ddl;
EXECUTE mokyu_buffer;
DEALLOCATE PREPARE mokyu_buffer;
UPDATE mokyu_meta SET schema_version=33 WHERE id=1;
