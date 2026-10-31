CREATE TABLE bucket_change_gates (
    bucket_id BINARY(16) PRIMARY KEY,
    subscribers INTEGER NOT NULL DEFAULT 0 CHECK (subscribers>=0),
    FOREIGN KEY (bucket_id) REFERENCES buckets(id) ON DELETE CASCADE
);
INSERT INTO bucket_change_gates(bucket_id) SELECT id FROM buckets;

CREATE TABLE transfer_subscriptions (
    task_id BINARY(16) PRIMARY KEY,
    bucket_id BINARY(16) NOT NULL,
    scope JSON NOT NULL,
    active BOOLEAN NOT NULL DEFAULT TRUE,
    FOREIGN KEY (task_id) REFERENCES transfer_runs(task_id) ON DELETE CASCADE,
    FOREIGN KEY (bucket_id) REFERENCES buckets(id)
);
CREATE INDEX transfer_subscriptions_bucket ON transfer_subscriptions(bucket_id,active);

CREATE TABLE transfer_changes (
    task_id BINARY(16) NOT NULL,
    object_key VARCHAR(1024) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    key_hash BINARY(32) GENERATED ALWAYS AS (UNHEX(SHA2(CAST(object_key AS BINARY),256))) STORED,
    source_version BIGINT,
    snapshot JSON,
    revision BIGINT NOT NULL DEFAULT 1 CHECK (revision>0),
    UNIQUE (task_id,key_hash),
    CHECK (octet_length(object_key) BETWEEN 1 AND 1024),
    CHECK ((source_version IS NULL)=(snapshot IS NULL)),
    FOREIGN KEY (task_id) REFERENCES transfer_subscriptions(task_id) ON DELETE CASCADE,
    FOREIGN KEY (source_version) REFERENCES streams(id)
);
CREATE INDEX transfer_changes_source ON transfer_changes(source_version);

ALTER TABLE transfer_runs ADD COLUMN scanned BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN captured BOOLEAN NOT NULL DEFAULT FALSE, ADD COLUMN sealed BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN scan_end VARCHAR(1024) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;
UPDATE transfer_runs SET scanned=TRUE,sealed=TRUE WHERE phase<>'scan';
ALTER TABLE transfer_items ADD COLUMN confirmation JSON;

CREATE TRIGGER buckets_change_gate AFTER INSERT ON buckets FOR EACH ROW
    INSERT INTO bucket_change_gates(bucket_id) VALUES(NEW.id);

CREATE TRIGGER objects_capture_insert AFTER INSERT ON objects FOR EACH ROW
BEGIN
    DECLARE readers INTEGER DEFAULT 0;
    IF NEW.stream_id IS NOT NULL THEN
        SELECT subscribers INTO readers FROM bucket_change_gates WHERE bucket_id=NEW.bucket_id LOCK IN SHARE MODE;
        IF readers>0 THEN
            INSERT INTO transfer_changes(task_id,object_key,source_version,snapshot)
                SELECT t.task_id,CAST(NEW.object_key AS CHAR CHARACTER SET utf8mb4),s.id,
                    JSON_OBJECT('metadata',JSON_EXTRACT(s.metadata,'$'),'checksums',JSON_EXTRACT(s.checksums,'$'),
                        'public',JSON_EXTRACT(IF(s.public_read,'true','false'),'$'),'size',s.size,'etag',s.etag,'payload',s.payload_type)
                FROM transfer_subscriptions t JOIN streams s ON s.id=NEW.stream_id
                WHERE t.bucket_id=NEW.bucket_id AND t.active AND (
                    JSON_UNQUOTE(JSON_EXTRACT(t.scope,'$.selection'))='all'
                    OR JSON_UNQUOTE(JSON_EXTRACT(t.scope,'$.selection'))='prefix' AND
                        LEFT(NEW.object_key,OCTET_LENGTH(JSON_UNQUOTE(JSON_EXTRACT(t.scope,'$.prefix'))))=CAST(JSON_UNQUOTE(JSON_EXTRACT(t.scope,'$.prefix')) AS BINARY)
                    OR JSON_UNQUOTE(JSON_EXTRACT(t.scope,'$.selection'))='objects' AND
                        JSON_CONTAINS(JSON_EXTRACT(t.scope,'$.keys'),JSON_QUOTE(CAST(NEW.object_key AS CHAR CHARACTER SET utf8mb4))))
                ON DUPLICATE KEY UPDATE object_key=IF(BINARY transfer_changes.object_key=BINARY VALUES(object_key),transfer_changes.object_key,NULL),
                    source_version=VALUES(source_version),snapshot=VALUES(snapshot),revision=transfer_changes.revision+1;
        END IF;
    END IF;
END;

CREATE TRIGGER objects_capture_update AFTER UPDATE ON objects FOR EACH ROW
BEGIN
    DECLARE readers INTEGER DEFAULT 0;
    IF NOT (OLD.stream_id<=>NEW.stream_id AND OLD.catalog_public<=>NEW.catalog_public) THEN
        SELECT subscribers INTO readers FROM bucket_change_gates WHERE bucket_id=NEW.bucket_id LOCK IN SHARE MODE;
        IF readers>0 THEN
            INSERT INTO transfer_changes(task_id,object_key,source_version,snapshot)
                SELECT t.task_id,CAST(NEW.object_key AS CHAR CHARACTER SET utf8mb4),s.id,
                    IF(s.id IS NULL,NULL,JSON_OBJECT('metadata',JSON_EXTRACT(s.metadata,'$'),'checksums',JSON_EXTRACT(s.checksums,'$'),
                        'public',JSON_EXTRACT(IF(s.public_read,'true','false'),'$'),'size',s.size,'etag',s.etag,'payload',s.payload_type))
                FROM transfer_subscriptions t LEFT JOIN streams s ON s.id=NEW.stream_id
                WHERE t.bucket_id=NEW.bucket_id AND t.active AND (
                    JSON_UNQUOTE(JSON_EXTRACT(t.scope,'$.selection'))='all'
                    OR JSON_UNQUOTE(JSON_EXTRACT(t.scope,'$.selection'))='prefix' AND
                        LEFT(NEW.object_key,OCTET_LENGTH(JSON_UNQUOTE(JSON_EXTRACT(t.scope,'$.prefix'))))=CAST(JSON_UNQUOTE(JSON_EXTRACT(t.scope,'$.prefix')) AS BINARY)
                    OR JSON_UNQUOTE(JSON_EXTRACT(t.scope,'$.selection'))='objects' AND
                        JSON_CONTAINS(JSON_EXTRACT(t.scope,'$.keys'),JSON_QUOTE(CAST(NEW.object_key AS CHAR CHARACTER SET utf8mb4))))
                ON DUPLICATE KEY UPDATE object_key=IF(BINARY transfer_changes.object_key=BINARY VALUES(object_key),transfer_changes.object_key,NULL),
                    source_version=VALUES(source_version),snapshot=VALUES(snapshot),revision=transfer_changes.revision+1;
        END IF;
    END IF;
END;

CREATE TRIGGER objects_capture_delete AFTER DELETE ON objects FOR EACH ROW
BEGIN
    DECLARE readers INTEGER DEFAULT 0;
    IF OLD.stream_id IS NOT NULL THEN
        SELECT subscribers INTO readers FROM bucket_change_gates WHERE bucket_id=OLD.bucket_id LOCK IN SHARE MODE;
        IF readers>0 THEN
            INSERT INTO transfer_changes(task_id,object_key,source_version,snapshot)
                SELECT task_id,CAST(OLD.object_key AS CHAR CHARACTER SET utf8mb4),NULL,NULL FROM transfer_subscriptions
                WHERE bucket_id=OLD.bucket_id AND active AND (
                    JSON_UNQUOTE(JSON_EXTRACT(scope,'$.selection'))='all'
                    OR JSON_UNQUOTE(JSON_EXTRACT(scope,'$.selection'))='prefix' AND
                        LEFT(OLD.object_key,OCTET_LENGTH(JSON_UNQUOTE(JSON_EXTRACT(scope,'$.prefix'))))=CAST(JSON_UNQUOTE(JSON_EXTRACT(scope,'$.prefix')) AS BINARY)
                    OR JSON_UNQUOTE(JSON_EXTRACT(scope,'$.selection'))='objects' AND
                        JSON_CONTAINS(JSON_EXTRACT(scope,'$.keys'),JSON_QUOTE(CAST(OLD.object_key AS CHAR CHARACTER SET utf8mb4))))
                ON DUPLICATE KEY UPDATE object_key=IF(BINARY transfer_changes.object_key=BINARY VALUES(object_key),transfer_changes.object_key,NULL),
                    source_version=NULL,snapshot=NULL,revision=transfer_changes.revision+1;
        END IF;
    END IF;
END;

UPDATE mokyu_meta SET schema_version=50 WHERE id=1;
