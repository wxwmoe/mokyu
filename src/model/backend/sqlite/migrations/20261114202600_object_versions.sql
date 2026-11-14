ALTER TABLE buckets ADD COLUMN version_mode INTEGER NOT NULL DEFAULT 0 CHECK (version_mode IN (0,1,2));
CREATE TABLE object_version_clocks (
    bucket_id BLOB NOT NULL PRIMARY KEY,
    version_seq INTEGER NOT NULL DEFAULT 0,
    FOREIGN KEY (bucket_id) REFERENCES buckets(id) ON DELETE CASCADE
) STRICT, WITHOUT ROWID;
ALTER TABLE objects ADD COLUMN version_switch INTEGER NOT NULL DEFAULT FALSE;

CREATE TABLE object_versions (
    bucket_id BLOB NOT NULL,
    object_key BLOB NOT NULL,
    seq INTEGER NOT NULL,
    version_id TEXT NOT NULL,
    stream_id INTEGER,
    modified_us INTEGER NOT NULL,
    PRIMARY KEY (bucket_id,object_key,seq),
    UNIQUE (bucket_id,object_key,version_id),
    FOREIGN KEY (bucket_id) REFERENCES buckets(id),
    FOREIGN KEY (stream_id) REFERENCES streams(id)
) STRICT, WITHOUT ROWID;
CREATE INDEX object_versions_stream ON object_versions(stream_id);

CREATE TRIGGER object_versions_insert AFTER INSERT ON objects
WHEN NEW.stream_id IS NOT NULL AND (SELECT version_mode FROM buckets WHERE id=NEW.bucket_id)>0
BEGIN
    UPDATE object_version_clocks SET version_seq=version_seq+1 WHERE bucket_id=NEW.bucket_id;
    DELETE FROM object_versions WHERE bucket_id=NEW.bucket_id AND object_key=NEW.object_key AND version_id='null' AND (SELECT version_mode FROM buckets WHERE id=NEW.bucket_id)=2;
    INSERT INTO object_versions(bucket_id,object_key,seq,version_id,stream_id,modified_us)
        SELECT bucket_id,NEW.object_key,version_seq,CASE WHEN version_mode=1 THEN lower(hex(randomblob(16))) ELSE 'null' END,NEW.stream_id,CAST(unixepoch('subsec')*1000000 AS INTEGER) FROM object_version_clocks JOIN buckets ON buckets.id=object_version_clocks.bucket_id WHERE bucket_id=NEW.bucket_id;
END;

CREATE TRIGGER object_versions_update AFTER UPDATE OF stream_id ON objects
WHEN OLD.stream_id IS NOT NEW.stream_id AND OLD.version_switch=NEW.version_switch AND (SELECT version_mode FROM buckets WHERE id=NEW.bucket_id)>0
BEGIN
    UPDATE object_version_clocks SET version_seq=version_seq+1 WHERE bucket_id=NEW.bucket_id;
    DELETE FROM object_versions WHERE bucket_id=NEW.bucket_id AND object_key=NEW.object_key AND version_id='null' AND (SELECT version_mode FROM buckets WHERE id=NEW.bucket_id)=2;
    INSERT INTO object_versions(bucket_id,object_key,seq,version_id,stream_id,modified_us)
        SELECT bucket_id,NEW.object_key,version_seq,CASE WHEN version_mode=1 THEN lower(hex(randomblob(16))) ELSE 'null' END,NEW.stream_id,CAST(unixepoch('subsec')*1000000 AS INTEGER) FROM object_version_clocks JOIN buckets ON buckets.id=object_version_clocks.bucket_id WHERE bucket_id=NEW.bucket_id;
END;

DROP VIEW token_bucket_access;
DROP VIEW user_bucket_access;

CREATE TABLE member_bucket_grants_expanded (
    user_id BLOB CHECK (user_id IS NULL OR length(user_id)=16) NOT NULL,
    project_id BLOB CHECK (project_id IS NULL OR length(project_id)=16) NOT NULL,
    bucket_id BLOB CHECK (bucket_id IS NULL OR length(bucket_id)=16) NOT NULL,
    actions INTEGER NOT NULL,
    PRIMARY KEY (user_id,bucket_id),
    CONSTRAINT member_grants_actions CHECK (actions BETWEEN 0 AND 2047),
    CONSTRAINT member_grants_member FOREIGN KEY (user_id,project_id) REFERENCES project_members(user_id,project_id) ON DELETE CASCADE,
    CONSTRAINT member_grants_bucket FOREIGN KEY (bucket_id,project_id) REFERENCES buckets(id,project_id) ON DELETE CASCADE
) STRICT, WITHOUT ROWID;
INSERT INTO member_bucket_grants_expanded SELECT * FROM member_bucket_grants;
DROP TABLE member_bucket_grants;
ALTER TABLE member_bucket_grants_expanded RENAME TO member_bucket_grants;
CREATE INDEX member_grants_bucket ON member_bucket_grants(bucket_id,project_id);
CREATE INDEX member_grants_member ON member_bucket_grants(user_id,project_id);

CREATE TABLE s3_credential_grants_expanded (
    access_key TEXT NOT NULL,
    bucket_id BLOB CHECK (bucket_id IS NULL OR length(bucket_id)=16) NOT NULL,
    actions INTEGER NOT NULL,
    PRIMARY KEY (access_key,bucket_id),
    CONSTRAINT credential_grants_actions CHECK (actions BETWEEN 0 AND 2047),
    CONSTRAINT credential_grants_credential FOREIGN KEY (access_key) REFERENCES s3_credentials(access_key_id) ON DELETE CASCADE,
    CONSTRAINT credential_grants_bucket FOREIGN KEY (bucket_id) REFERENCES buckets(id) ON DELETE CASCADE
) STRICT, WITHOUT ROWID;
INSERT INTO s3_credential_grants_expanded SELECT * FROM s3_credential_grants;
DROP TABLE s3_credential_grants;
ALTER TABLE s3_credential_grants_expanded RENAME TO s3_credential_grants;
CREATE INDEX credential_grants_bucket ON s3_credential_grants(bucket_id);

CREATE TABLE token_bucket_grants_expanded (
    token_id BLOB CHECK (token_id IS NULL OR length(token_id)=16) NOT NULL,
    bucket_id BLOB CHECK (bucket_id IS NULL OR length(bucket_id)=16) NOT NULL,
    actions INTEGER NOT NULL,
    PRIMARY KEY (token_id,bucket_id),
    CONSTRAINT token_grants_actions CHECK (actions BETWEEN 0 AND 2047),
    CONSTRAINT token_grants_token FOREIGN KEY (token_id) REFERENCES api_tokens(id) ON DELETE CASCADE,
    CONSTRAINT token_grants_bucket FOREIGN KEY (bucket_id) REFERENCES buckets(id) ON DELETE CASCADE
) STRICT, WITHOUT ROWID;
INSERT INTO token_bucket_grants_expanded SELECT * FROM token_bucket_grants;
DROP TABLE token_bucket_grants;
ALTER TABLE token_bucket_grants_expanded RENAME TO token_bucket_grants;
CREATE INDEX token_grants_bucket ON token_bucket_grants(bucket_id);

CREATE VIEW user_bucket_access AS
SELECT m.user_id,b.id AS bucket_id,
    (CASE m.role WHEN 'reader' THEN 67 WHEN 'writer' THEN 479 WHEN 'maintainer' THEN 2047 ELSE 0 END)
    & IIF(m.scope='all',2047,COALESCE(g.actions,0)) AS actions
FROM project_members m JOIN buckets b ON b.project_id=m.project_id
LEFT JOIN member_bucket_grants g ON g.user_id=m.user_id AND g.bucket_id=b.id;

CREATE VIEW token_bucket_access AS
SELECT t.id AS token_id,g.bucket_id,
    g.actions & IIF(u.role='admin',2047,COALESCE(a.actions,0)) AS actions
FROM api_tokens t JOIN web_users u ON u.id=t.user_id
JOIN token_bucket_grants g ON g.token_id=t.id
LEFT JOIN user_bucket_access a ON a.user_id=t.user_id AND a.bucket_id=g.bucket_id;

ALTER TABLE journal_entries ADD COLUMN history_subject INTEGER NOT NULL DEFAULT FALSE;
ALTER TABLE journal_restore_items ADD COLUMN stage_key BLOB;
ALTER TABLE journal_restore_items ADD COLUMN logical_key BLOB;
ALTER TABLE journal_restore_items ADD COLUMN history_id TEXT;
ALTER TABLE journal_restore_items ADD COLUMN history_seq INTEGER;
ALTER TABLE journal_restore_items ADD COLUMN history_time INTEGER;
ALTER TABLE journal_restore_items ADD COLUMN history_marker INTEGER NOT NULL DEFAULT FALSE;
CREATE TRIGGER journal_version_insert AFTER INSERT ON object_versions WHEN (SELECT journal_mode FROM bucket_change_gates WHERE bucket_id=NEW.bucket_id)>0
BEGIN
    UPDATE journal_buckets SET last_seq=last_seq+1 WHERE bucket_id=NEW.bucket_id AND (SELECT journal_mode FROM bucket_change_gates WHERE bucket_id=NEW.bucket_id)=1;
    INSERT INTO journal_entries(encrypt_frames,bucket_id,seq,generation,operation,object_key,stream_id,payload_revision,attributes,history_subject) SELECT st.block_key_id IS NOT NULL,j.bucket_id,j.last_seq,j.generation,'put',CAST(('~h/' || NEW.version_id || '/' || NEW.object_key) AS BLOB),NEW.stream_id,s.payload_revision,json_object('size',COALESCE(s.size,0),'etag',COALESCE(s.etag,''),'metadata',json(COALESCE(s.metadata,'{}')),'checksums',json(COALESCE(s.checksums,'{}')),'payload',COALESCE(s.payload_type,'chunks'),'modified_us',NEW.modified_us,'history',json_object('order',NEW.seq,'time',NEW.modified_us,'marker',CASE WHEN NEW.stream_id IS NULL THEN 1 ELSE 0 END)),TRUE FROM journal_buckets j JOIN storages st ON st.id=j.storage_id LEFT JOIN streams s ON s.id=NEW.stream_id WHERE j.bucket_id=NEW.bucket_id AND (SELECT journal_mode FROM bucket_change_gates WHERE bucket_id=NEW.bucket_id)=1;
    UPDATE journal_buckets SET dirty=TRUE WHERE bucket_id=NEW.bucket_id AND (SELECT journal_mode FROM bucket_change_gates WHERE bucket_id=NEW.bucket_id)=2;
END;
CREATE TRIGGER journal_version_remove AFTER DELETE ON object_versions WHEN (SELECT journal_mode FROM bucket_change_gates WHERE bucket_id=OLD.bucket_id)>0
BEGIN
    UPDATE journal_buckets SET last_seq=last_seq+1 WHERE bucket_id=OLD.bucket_id AND (SELECT journal_mode FROM bucket_change_gates WHERE bucket_id=OLD.bucket_id)=1;
    INSERT INTO journal_entries(encrypt_frames,bucket_id,seq,generation,operation,object_key,stream_id,payload_revision,attributes,history_subject) SELECT st.block_key_id IS NOT NULL,j.bucket_id,j.last_seq,j.generation,'delete',CAST(('~h/' || OLD.version_id || '/' || OLD.object_key) AS BLOB),NULL,NULL,NULL,TRUE FROM journal_buckets j JOIN storages st ON st.id=j.storage_id WHERE j.bucket_id=OLD.bucket_id AND (SELECT journal_mode FROM bucket_change_gates WHERE bucket_id=OLD.bucket_id)=1;
    UPDATE journal_buckets SET dirty=TRUE WHERE bucket_id=OLD.bucket_id AND (SELECT journal_mode FROM bucket_change_gates WHERE bucket_id=OLD.bucket_id)=2;
END;
CREATE TRIGGER journal_version_mode AFTER UPDATE ON buckets WHEN (SELECT journal_mode FROM bucket_change_gates WHERE bucket_id=NEW.id)>0 AND OLD.version_mode<>NEW.version_mode
BEGIN
    UPDATE journal_buckets SET last_seq=last_seq+1 WHERE bucket_id=NEW.id AND (SELECT journal_mode FROM bucket_change_gates WHERE bucket_id=NEW.id)=1;
    INSERT INTO journal_entries(encrypt_frames,bucket_id,seq,generation,operation,object_key,stream_id,payload_revision,attributes,history_subject) SELECT st.block_key_id IS NOT NULL,j.bucket_id,j.last_seq,j.generation,'put',CAST('~m' AS BLOB),NULL,NULL,json_object('size',0,'etag','','metadata',json_object(),'checksums',json_object(),'payload','chunks','modified_us',0,'history',json_object('mode',NEW.version_mode)),TRUE FROM journal_buckets j JOIN storages st ON st.id=j.storage_id WHERE j.bucket_id=NEW.id AND (SELECT journal_mode FROM bucket_change_gates WHERE bucket_id=NEW.id)=1;
    UPDATE journal_buckets SET dirty=TRUE WHERE bucket_id=NEW.id AND (SELECT journal_mode FROM bucket_change_gates WHERE bucket_id=NEW.id)=2;
END;
UPDATE journal_restore_items SET logical_key=object_key,stage_key=object_key WHERE logical_key IS NULL;

CREATE TRIGGER object_version_quota_insert AFTER INSERT ON object_versions WHEN NEW.stream_id IS NOT NULL AND (SELECT version_mode FROM buckets WHERE id=NEW.bucket_id)>0
BEGIN
    SELECT RAISE(ABORT,'version_quota_exceeded') WHERE EXISTS(SELECT 1 FROM quota_scopes WHERE ((scope='bucket' AND scope_id=NEW.bucket_id) OR (scope='project' AND scope_id=(SELECT project_id FROM buckets WHERE id=NEW.bucket_id))) AND logical_bytes_limit IS NOT NULL AND used_bytes+reserved_bytes+(SELECT size FROM streams WHERE id=NEW.stream_id)>logical_bytes_limit AND (SELECT size FROM streams WHERE id=NEW.stream_id)>0);
    UPDATE quota_scopes SET used_bytes=used_bytes+(SELECT size FROM streams WHERE id=NEW.stream_id) WHERE ((scope='bucket' AND scope_id=NEW.bucket_id) OR (scope='project' AND scope_id=(SELECT project_id FROM buckets WHERE id=NEW.bucket_id)));
END;

CREATE TRIGGER object_version_quota_delete AFTER DELETE ON object_versions WHEN OLD.stream_id IS NOT NULL AND (SELECT version_mode FROM buckets WHERE id=OLD.bucket_id)>0
BEGIN
    UPDATE quota_scopes SET used_bytes=used_bytes-(SELECT size FROM streams WHERE id=OLD.stream_id) WHERE ((scope='bucket' AND scope_id=OLD.bucket_id) OR (scope='project' AND scope_id=(SELECT project_id FROM buckets WHERE id=OLD.bucket_id)));
END;

CREATE TABLE object_write_receipts (
    stream_id INTEGER NOT NULL PRIMARY KEY REFERENCES streams(id) ON DELETE CASCADE,
    version_id TEXT NOT NULL
) STRICT, WITHOUT ROWID;
CREATE TRIGGER version_write_receipt AFTER INSERT ON object_versions WHEN NEW.seq>0 AND NEW.stream_id IS NOT NULL BEGIN
    INSERT INTO object_write_receipts(stream_id,version_id) VALUES(NEW.stream_id,NEW.version_id) ON CONFLICT(stream_id) DO NOTHING;
END;

UPDATE mokyu_meta SET schema_version=20261114202600 WHERE id=1;
