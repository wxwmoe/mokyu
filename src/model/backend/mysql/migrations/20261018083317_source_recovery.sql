SET @mokyu_recovery_ddl = IF(
    EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name='chunk_encodings' AND column_name='owner_stream'),
    'DO 0',
    'ALTER TABLE chunk_encodings ADD COLUMN owner_stream BINARY(16) NULL, ADD COLUMN recovery_pending BOOLEAN NOT NULL DEFAULT FALSE, ADD CONSTRAINT encodings_owner_stream FOREIGN KEY (owner_stream) REFERENCES streams(id) ON DELETE RESTRICT, ADD INDEX encodings_recovery (recovery_pending,storage_id,unreferenced_at,encoding_id)'
);
PREPARE mokyu_recovery FROM @mokyu_recovery_ddl;
EXECUTE mokyu_recovery;
DEALLOCATE PREPARE mokyu_recovery;
-- MySQL disallows a CHECK on owner_task because its foreign key uses SET NULL.
DROP TRIGGER IF EXISTS encodings_owner_insert;
CREATE TRIGGER encodings_owner_insert BEFORE INSERT ON chunk_encodings FOR EACH ROW
BEGIN
    IF NEW.owner_task IS NOT NULL AND NEW.owner_stream IS NOT NULL THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='encoding owners are exclusive';
    END IF;
END;
DROP TRIGGER IF EXISTS encodings_owner_update;
CREATE TRIGGER encodings_owner_update BEFORE UPDATE ON chunk_encodings FOR EACH ROW
BEGIN
    IF NEW.owner_task IS NOT NULL AND NEW.owner_stream IS NOT NULL THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='encoding owners are exclusive';
    END IF;
END;
UPDATE mokyu_meta SET schema_version=19 WHERE id=1;
