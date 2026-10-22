SET @mokyu_sync_ddl = IF(EXISTS(SELECT 1 FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='chunk_encodings' AND COLUMN_NAME='sync_pending'), 'SELECT 1', 'ALTER TABLE chunk_encodings ADD COLUMN sync_pending BOOLEAN NOT NULL DEFAULT FALSE, ADD INDEX encodings_sync(sync_pending,state,quarantined_at,chunk_id)');
PREPARE mokyu_sync FROM @mokyu_sync_ddl;
EXECUTE mokyu_sync;
DEALLOCATE PREPARE mokyu_sync;
UPDATE mokyu_meta SET schema_version=34 WHERE id=1;
