ALTER TABLE buckets ADD COLUMN version_mode SMALLINT NOT NULL DEFAULT 0 CHECK (version_mode IN (0,1,2));
CREATE TABLE object_version_clocks (
    bucket_id BINARY(16) NOT NULL PRIMARY KEY,
    version_seq BIGINT NOT NULL DEFAULT 0,
    FOREIGN KEY (bucket_id) REFERENCES buckets(id) ON DELETE CASCADE
);
ALTER TABLE objects ADD COLUMN version_switch BOOLEAN NOT NULL DEFAULT FALSE;

CREATE TABLE object_versions (
    bucket_id BINARY(16) NOT NULL,
    object_key VARBINARY(1024) NOT NULL,
    seq BIGINT NOT NULL,
    version_id VARCHAR(36) NOT NULL,
    stream_id BIGINT,
    modified_us BIGINT NOT NULL,
    PRIMARY KEY (bucket_id,object_key,seq),
    UNIQUE (bucket_id,object_key,version_id),
    FOREIGN KEY (bucket_id) REFERENCES buckets(id),
    FOREIGN KEY (stream_id) REFERENCES streams(id)
);
CREATE INDEX object_versions_stream ON object_versions(stream_id);

CREATE TRIGGER object_versions_insert AFTER INSERT ON objects FOR EACH ROW
BEGIN
    DECLARE mode_value SMALLINT;
    IF NEW.stream_id IS NOT NULL THEN
        SELECT version_mode INTO mode_value FROM buckets WHERE id=NEW.bucket_id LOCK IN SHARE MODE;
        IF mode_value>0 THEN
            UPDATE object_version_clocks SET version_seq=version_seq+1 WHERE bucket_id=NEW.bucket_id;
            IF mode_value=2 THEN
                DELETE FROM object_versions WHERE bucket_id=NEW.bucket_id AND object_key=NEW.object_key AND version_id='null';
            END IF;
            INSERT INTO object_versions(bucket_id,object_key,seq,version_id,stream_id,modified_us)
                SELECT bucket_id,NEW.object_key,version_seq,IF(mode_value=1,REPLACE(UUID(),'-',''),'null'),NEW.stream_id,TIMESTAMPDIFF(MICROSECOND,'1970-01-01',UTC_TIMESTAMP(6)) FROM object_version_clocks WHERE bucket_id=NEW.bucket_id;
        END IF;
    END IF;
END;

CREATE TRIGGER object_versions_update AFTER UPDATE ON objects FOR EACH ROW
BEGIN
    DECLARE mode_value SMALLINT;
    IF NOT (OLD.stream_id <=> NEW.stream_id) AND OLD.version_switch=NEW.version_switch THEN
        SELECT version_mode INTO mode_value FROM buckets WHERE id=NEW.bucket_id LOCK IN SHARE MODE;
        IF mode_value>0 THEN
            UPDATE object_version_clocks SET version_seq=version_seq+1 WHERE bucket_id=NEW.bucket_id;
            IF mode_value=2 THEN
                DELETE FROM object_versions WHERE bucket_id=NEW.bucket_id AND object_key=NEW.object_key AND version_id='null';
            END IF;
            INSERT INTO object_versions(bucket_id,object_key,seq,version_id,stream_id,modified_us)
                SELECT bucket_id,NEW.object_key,version_seq,IF(mode_value=1,REPLACE(UUID(),'-',''),'null'),NEW.stream_id,TIMESTAMPDIFF(MICROSECOND,'1970-01-01',UTC_TIMESTAMP(6)) FROM object_version_clocks WHERE bucket_id=NEW.bucket_id;
        END IF;
    END IF;
END;

SET @version_grant_ddl=CONCAT('ALTER TABLE member_bucket_grants ',IF(LOCATE('MariaDB',VERSION())>0,'DROP CONSTRAINT ','DROP CHECK '),'member_grants_actions, ADD CONSTRAINT member_grants_actions CHECK (actions BETWEEN 0 AND 2047)');
PREPARE version_grant_ddl FROM @version_grant_ddl;
EXECUTE version_grant_ddl;
DEALLOCATE PREPARE version_grant_ddl;

SET @version_grant_ddl=CONCAT('ALTER TABLE s3_credential_grants ',IF(LOCATE('MariaDB',VERSION())>0,'DROP CONSTRAINT ','DROP CHECK '),'credential_grants_actions, ADD CONSTRAINT credential_grants_actions CHECK (actions BETWEEN 0 AND 2047)');
PREPARE version_grant_ddl FROM @version_grant_ddl;
EXECUTE version_grant_ddl;
DEALLOCATE PREPARE version_grant_ddl;

SET @version_grant_ddl=CONCAT('ALTER TABLE token_bucket_grants ',IF(LOCATE('MariaDB',VERSION())>0,'DROP CONSTRAINT ','DROP CHECK '),'token_grants_actions, ADD CONSTRAINT token_grants_actions CHECK (actions BETWEEN 0 AND 2047)');
PREPARE version_grant_ddl FROM @version_grant_ddl;
EXECUTE version_grant_ddl;
DEALLOCATE PREPARE version_grant_ddl;

CREATE OR REPLACE VIEW user_bucket_access AS
SELECT m.user_id,b.id AS bucket_id,
    (CASE m.role WHEN 'reader' THEN 67 WHEN 'writer' THEN 479 WHEN 'maintainer' THEN 2047 ELSE 0 END)
    & IF(m.scope='all',2047,COALESCE(g.actions,0)) AS actions
FROM project_members m JOIN buckets b ON b.project_id=m.project_id
LEFT JOIN member_bucket_grants g ON g.user_id=m.user_id AND g.bucket_id=b.id;

CREATE OR REPLACE VIEW token_bucket_access AS
SELECT t.id AS token_id,g.bucket_id,
    g.actions & IF(u.role='admin',2047,COALESCE(a.actions,0)) AS actions
FROM api_tokens t JOIN web_users u ON u.id=t.user_id
JOIN token_bucket_grants g ON g.token_id=t.id
LEFT JOIN user_bucket_access a ON a.user_id=t.user_id AND a.bucket_id=g.bucket_id;

ALTER TABLE journal_entries ADD COLUMN history_subject BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE journal_restore_items ADD COLUMN stage_key VARBINARY(64);
ALTER TABLE journal_restore_items ADD COLUMN logical_key VARBINARY(1100);
ALTER TABLE journal_restore_items ADD COLUMN history_id VARCHAR(32);
ALTER TABLE journal_restore_items ADD COLUMN history_seq BIGINT;
ALTER TABLE journal_restore_items ADD COLUMN history_time BIGINT;
ALTER TABLE journal_restore_items ADD COLUMN history_marker BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE journal_restore_ranges DROP FOREIGN KEY journal_restore_ranges_ibfk_1;
ALTER TABLE journal_entries MODIFY object_key VARBINARY(1100);
ALTER TABLE journal_snapshot_current MODIFY object_key VARBINARY(1100) NOT NULL;
ALTER TABLE journal_snapshot_members MODIFY object_key VARBINARY(1100) NOT NULL;
ALTER TABLE journal_snapshot_work MODIFY object_key VARBINARY(1100) NOT NULL;
ALTER TABLE journal_restore_items MODIFY object_key VARBINARY(1100) NOT NULL;
ALTER TABLE journal_restore_ranges MODIFY object_key VARBINARY(1100) NOT NULL;
ALTER TABLE journal_restore_buckets MODIFY next_key VARBINARY(1100);
ALTER TABLE journal_restore_ranges ADD CONSTRAINT journal_restore_ranges_ibfk_1 FOREIGN KEY (job,source_bucket,object_key) REFERENCES journal_restore_items(job,source_bucket,object_key) ON DELETE CASCADE;
CREATE TRIGGER journal_version_insert AFTER INSERT ON object_versions FOR EACH ROW
BEGIN
    DECLARE journal SMALLINT;
        SELECT journal_mode INTO journal FROM bucket_change_gates WHERE bucket_id=NEW.bucket_id LOCK IN SHARE MODE;
        IF journal=1 THEN
        UPDATE journal_buckets SET last_seq=last_seq+1 WHERE bucket_id=NEW.bucket_id;
        INSERT INTO journal_entries(encrypt_frames,bucket_id,seq,generation,operation,object_key,stream_id,payload_revision,attributes,history_subject) SELECT st.block_key_id IS NOT NULL,j.bucket_id,j.last_seq,j.generation,'put',CONCAT('~h/',NEW.version_id,'/',NEW.object_key),NEW.stream_id,s.payload_revision,JSON_OBJECT('size',COALESCE(s.size,0),'etag',COALESCE(s.etag,''),'metadata',COALESCE(JSON_EXTRACT(s.metadata,'$'),JSON_OBJECT()),'checksums',COALESCE(JSON_EXTRACT(s.checksums,'$'),JSON_OBJECT()),'payload',COALESCE(s.payload_type,'chunks'),'modified_us',NEW.modified_us,'history',JSON_OBJECT('order',NEW.seq,'time',NEW.modified_us,'marker',CASE WHEN NEW.stream_id IS NULL THEN 1 ELSE 0 END)),TRUE FROM journal_buckets j JOIN storages st ON st.id=j.storage_id LEFT JOIN streams s ON s.id=NEW.stream_id WHERE j.bucket_id=NEW.bucket_id;
    ELSEIF journal=2 THEN
        UPDATE journal_buckets SET dirty=TRUE WHERE bucket_id=NEW.bucket_id;
    END IF;
END;
CREATE TRIGGER journal_version_remove AFTER DELETE ON object_versions FOR EACH ROW
BEGIN
    DECLARE journal SMALLINT;
        SELECT journal_mode INTO journal FROM bucket_change_gates WHERE bucket_id=OLD.bucket_id LOCK IN SHARE MODE;
        IF journal=1 THEN
        UPDATE journal_buckets SET last_seq=last_seq+1 WHERE bucket_id=OLD.bucket_id;
        INSERT INTO journal_entries(encrypt_frames,bucket_id,seq,generation,operation,object_key,stream_id,payload_revision,attributes,history_subject) SELECT st.block_key_id IS NOT NULL,j.bucket_id,j.last_seq,j.generation,'delete',CONCAT('~h/',OLD.version_id,'/',OLD.object_key),NULL,NULL,NULL,TRUE FROM journal_buckets j JOIN storages st ON st.id=j.storage_id WHERE j.bucket_id=OLD.bucket_id;
    ELSEIF journal=2 THEN
        UPDATE journal_buckets SET dirty=TRUE WHERE bucket_id=OLD.bucket_id;
    END IF;
END;
CREATE TRIGGER journal_version_mode AFTER UPDATE ON buckets FOR EACH ROW
BEGIN
    DECLARE journal SMALLINT;
    IF OLD.version_mode<>NEW.version_mode THEN
        SELECT journal_mode INTO journal FROM bucket_change_gates WHERE bucket_id=NEW.id LOCK IN SHARE MODE;
        IF journal=1 THEN
        UPDATE journal_buckets SET last_seq=last_seq+1 WHERE bucket_id=NEW.id;
        INSERT INTO journal_entries(encrypt_frames,bucket_id,seq,generation,operation,object_key,stream_id,payload_revision,attributes,history_subject) SELECT st.block_key_id IS NOT NULL,j.bucket_id,j.last_seq,j.generation,'put','~m',NULL,NULL,JSON_OBJECT('size',0,'etag','','metadata',JSON_OBJECT(),'checksums',JSON_OBJECT(),'payload','chunks','modified_us',0,'history',JSON_OBJECT('mode',NEW.version_mode)),TRUE FROM journal_buckets j JOIN storages st ON st.id=j.storage_id WHERE j.bucket_id=NEW.id;
    ELSEIF journal=2 THEN
        UPDATE journal_buckets SET dirty=TRUE WHERE bucket_id=NEW.id;
    END IF;
    END IF;
END;
UPDATE journal_restore_items SET logical_key=object_key,stage_key=object_key WHERE logical_key IS NULL;

CREATE TRIGGER object_version_quota_insert AFTER INSERT ON object_versions FOR EACH ROW
BEGIN
    IF NEW.stream_id IS NOT NULL AND (SELECT version_mode FROM buckets WHERE id=NEW.bucket_id)>0 THEN
        IF EXISTS(SELECT 1 FROM quota_scopes WHERE ((scope='bucket' AND scope_id=NEW.bucket_id) OR (scope='project' AND scope_id=(SELECT project_id FROM buckets WHERE id=NEW.bucket_id))) AND logical_bytes_limit IS NOT NULL AND used_bytes+reserved_bytes+(SELECT size FROM streams WHERE id=NEW.stream_id)>logical_bytes_limit AND (SELECT size FROM streams WHERE id=NEW.stream_id)>0) THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='version_quota_exceeded'; END IF;
        UPDATE quota_scopes SET used_bytes=used_bytes+(SELECT size FROM streams WHERE id=NEW.stream_id) WHERE ((scope='bucket' AND scope_id=NEW.bucket_id) OR (scope='project' AND scope_id=(SELECT project_id FROM buckets WHERE id=NEW.bucket_id)));
    END IF;
END;

CREATE TRIGGER object_version_quota_delete AFTER DELETE ON object_versions FOR EACH ROW
BEGIN
    IF OLD.stream_id IS NOT NULL AND (SELECT version_mode FROM buckets WHERE id=OLD.bucket_id)>0 THEN
        UPDATE quota_scopes SET used_bytes=used_bytes-(SELECT size FROM streams WHERE id=OLD.stream_id) WHERE ((scope='bucket' AND scope_id=OLD.bucket_id) OR (scope='project' AND scope_id=(SELECT project_id FROM buckets WHERE id=OLD.bucket_id)));
    END IF;
END;

CREATE TABLE object_write_receipts (
    stream_id BIGINT NOT NULL PRIMARY KEY,
    version_id VARCHAR(36) NOT NULL,
    FOREIGN KEY(stream_id) REFERENCES streams(id) ON DELETE CASCADE
);
CREATE TRIGGER version_write_receipt AFTER INSERT ON object_versions FOR EACH ROW
BEGIN
    IF NEW.seq>0 AND NEW.stream_id IS NOT NULL THEN
        INSERT INTO object_write_receipts(stream_id,version_id) VALUES(NEW.stream_id,NEW.version_id) ON DUPLICATE KEY UPDATE stream_id=NEW.stream_id;
    END IF;
END;

UPDATE mokyu_meta SET schema_version=20261114202600 WHERE id=1;
