CREATE TABLE IF NOT EXISTS nonce_counters (
    block_key_id binary(16) NOT NULL,
    type_tag bigint NOT NULL CHECK (type_tag BETWEEN 0 AND 31),
    reserved_id bigint NOT NULL DEFAULT 0 CHECK (reserved_id>=0),
    PRIMARY KEY (block_key_id,type_tag),
    CONSTRAINT nonce_counters_key FOREIGN KEY(block_key_id) REFERENCES managed_keys(id)
);

SET @mokyu_nonce_ddl = IF(EXISTS(SELECT 1 FROM information_schema.STATISTICS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='managed_keys' AND INDEX_NAME='managed_keys_fingerprint'), 'SELECT 1', 'CREATE UNIQUE INDEX managed_keys_fingerprint ON managed_keys(fingerprint)');
PREPARE mokyu_nonce FROM @mokyu_nonce_ddl;
EXECUTE mokyu_nonce;
DEALLOCATE PREPARE mokyu_nonce;

SET @mokyu_nonce_ddl = IF(EXISTS(SELECT 1 FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='chunk_encodings' AND COLUMN_NAME='nonce_id'), 'SELECT 1', 'ALTER TABLE chunk_encodings ADD COLUMN nonce_id bigint');
PREPARE mokyu_nonce FROM @mokyu_nonce_ddl;
EXECUTE mokyu_nonce;
DEALLOCATE PREPARE mokyu_nonce;
UPDATE chunk_encodings SET nonce_id=encoding_id WHERE nonce IS NOT NULL AND nonce_id IS NULL;
SET @mokyu_nonce_ddl = IF(EXISTS(SELECT 1 FROM information_schema.TABLE_CONSTRAINTS WHERE CONSTRAINT_SCHEMA=DATABASE() AND TABLE_NAME='chunk_encodings' AND CONSTRAINT_NAME='chunk_encodings_nonce_id'), 'SELECT 1', 'ALTER TABLE chunk_encodings ADD CONSTRAINT chunk_encodings_nonce_id CHECK ((encryption_algorithm=''none'' AND nonce_id IS NULL AND nonce IS NULL AND block_key_id IS NULL) OR (encryption_algorithm<>''none'' AND block_key_id IS NOT NULL AND ((nonce IS NULL AND nonce_id IS NULL AND state<>''ready'') OR (nonce IS NOT NULL AND nonce_id IS NOT NULL AND nonce_id>0))))');
PREPARE mokyu_nonce FROM @mokyu_nonce_ddl;
EXECUTE mokyu_nonce;
DEALLOCATE PREPARE mokyu_nonce;

SET @mokyu_nonce_ddl = IF(EXISTS(SELECT 1 FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='packs' AND COLUMN_NAME='nonce_id'), 'SELECT 1', 'ALTER TABLE packs ADD COLUMN nonce_id bigint');
PREPARE mokyu_nonce FROM @mokyu_nonce_ddl;
EXECUTE mokyu_nonce;
DEALLOCATE PREPARE mokyu_nonce;
UPDATE packs SET nonce_id=id WHERE nonce IS NOT NULL AND nonce_id IS NULL;
SET @mokyu_nonce_ddl = IF(EXISTS(SELECT 1 FROM information_schema.TABLE_CONSTRAINTS WHERE CONSTRAINT_SCHEMA=DATABASE() AND TABLE_NAME='packs' AND CONSTRAINT_NAME='packs_nonce_id'), 'SELECT 1', 'ALTER TABLE packs ADD CONSTRAINT packs_nonce_id CHECK ((encryption_algorithm=''none'' AND nonce_id IS NULL AND nonce IS NULL AND block_key_id IS NULL) OR (encryption_algorithm<>''none'' AND block_key_id IS NOT NULL AND ((nonce IS NULL AND nonce_id IS NULL AND state<>''ready'') OR (nonce IS NOT NULL AND nonce_id IS NOT NULL AND nonce_id>0))))');
PREPARE mokyu_nonce FROM @mokyu_nonce_ddl;
EXECUTE mokyu_nonce;
DEALLOCATE PREPARE mokyu_nonce;

SET @mokyu_nonce_ddl = IF(LOCATE('MariaDB',VERSION())>0, 'SELECT 1', 'SET SESSION information_schema_stats_expiry=0');
PREPARE mokyu_nonce FROM @mokyu_nonce_ddl;
EXECUTE mokyu_nonce;
DEALLOCATE PREPARE mokyu_nonce;
INSERT INTO nonce_counters(block_key_id,type_tag,reserved_id) SELECT k.id,0,GREATEST(COALESCE(t.AUTO_INCREMENT,1)-1,0) FROM managed_keys k CROSS JOIN information_schema.TABLES t WHERE k.purpose='block' AND t.TABLE_SCHEMA=DATABASE() AND t.TABLE_NAME='chunk_encodings' ON DUPLICATE KEY UPDATE reserved_id=GREATEST(nonce_counters.reserved_id,VALUES(reserved_id));
INSERT INTO nonce_counters(block_key_id,type_tag,reserved_id) SELECT k.id,1,GREATEST(COALESCE(t.AUTO_INCREMENT,1)-1,0) FROM managed_keys k CROSS JOIN information_schema.TABLES t WHERE k.purpose='block' AND t.TABLE_SCHEMA=DATABASE() AND t.TABLE_NAME='packs' ON DUPLICATE KEY UPDATE reserved_id=GREATEST(nonce_counters.reserved_id,VALUES(reserved_id));

UPDATE mokyu_meta SET schema_version=24 WHERE id=1;
