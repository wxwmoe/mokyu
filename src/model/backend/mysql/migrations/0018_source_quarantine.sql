-- Each ALTER is atomic; a restart may repeat either completed table change.
SET @mokyu_source_ddl = IF(
    EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name='chunk_encodings' AND column_name='quarantine_revision'),
    'DO 0',
    'ALTER TABLE chunk_encodings ADD COLUMN quarantined_at DATETIME(6) NULL, ADD COLUMN quarantine_code VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NULL, ADD COLUMN quarantine_revision BIGINT NOT NULL DEFAULT 0, ADD CONSTRAINT encodings_quarantine CHECK ((quarantined_at IS NULL) = (quarantine_code IS NULL)), ADD CONSTRAINT encodings_quarantine_revision CHECK (quarantine_revision >= 0), ADD INDEX encodings_quarantined (storage_id,quarantined_at,encoding_id)'
);
PREPARE mokyu_source FROM @mokyu_source_ddl;
EXECUTE mokyu_source;
DEALLOCATE PREPARE mokyu_source;
SET @mokyu_source_ddl = IF(
    EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name='packs' AND column_name='quarantine_revision'),
    'DO 0',
    'ALTER TABLE packs ADD COLUMN quarantined_at DATETIME(6) NULL, ADD COLUMN quarantine_code VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NULL, ADD COLUMN quarantine_revision BIGINT NOT NULL DEFAULT 0, ADD CONSTRAINT packs_quarantine CHECK ((quarantined_at IS NULL) = (quarantine_code IS NULL)), ADD CONSTRAINT packs_quarantine_revision CHECK (quarantine_revision >= 0), ADD INDEX packs_quarantined (storage_id,quarantined_at,id)'
);
PREPARE mokyu_source FROM @mokyu_source_ddl;
EXECUTE mokyu_source;
DEALLOCATE PREPARE mokyu_source;
SET @mokyu_source_ddl = CONCAT(
    'ALTER TABLE maintenance_previews MODIFY COLUMN action VARCHAR(10) CHARACTER SET ascii COLLATE ascii_bin NOT NULL, MODIFY COLUMN expires_at DATETIME(6) NOT NULL DEFAULT (CURRENT_TIMESTAMP(6)+INTERVAL 10 MINUTE), ',
    IF(LOCATE('MariaDB',VERSION())>0, 'DROP CONSTRAINT ', 'DROP CHECK '),
    'maintenance_previews_action, ADD CONSTRAINT maintenance_previews_action CHECK (action IN (''unpack'',''sweep'',''quarantine'',''recheck''))'
);
PREPARE mokyu_source FROM @mokyu_source_ddl;
EXECUTE mokyu_source;
DEALLOCATE PREPARE mokyu_source;
UPDATE mokyu_meta SET schema_version=18 WHERE id=1;
