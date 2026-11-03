DROP TRIGGER IF EXISTS objects_capture_insert;
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
                    CAST(JSON_UNQUOTE(JSON_EXTRACT(t.scope,'$.selection')) AS BINARY)='all'
                    OR CAST(JSON_UNQUOTE(JSON_EXTRACT(t.scope,'$.selection')) AS BINARY)='prefix' AND
                        LEFT(NEW.object_key,OCTET_LENGTH(JSON_UNQUOTE(JSON_EXTRACT(t.scope,'$.prefix'))))=CAST(JSON_UNQUOTE(JSON_EXTRACT(t.scope,'$.prefix')) AS BINARY)
                    OR CAST(JSON_UNQUOTE(JSON_EXTRACT(t.scope,'$.selection')) AS BINARY)='objects' AND
                        JSON_CONTAINS(JSON_EXTRACT(t.scope,'$.keys'),JSON_QUOTE(CAST(NEW.object_key AS CHAR CHARACTER SET utf8mb4))))
                ON DUPLICATE KEY UPDATE object_key=IF(BINARY transfer_changes.object_key=BINARY VALUES(object_key),transfer_changes.object_key,NULL),
                    source_version=VALUES(source_version),snapshot=VALUES(snapshot),revision=transfer_changes.revision+1;
        END IF;
    END IF;
END;

DROP TRIGGER IF EXISTS objects_capture_update;
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
                    CAST(JSON_UNQUOTE(JSON_EXTRACT(t.scope,'$.selection')) AS BINARY)='all'
                    OR CAST(JSON_UNQUOTE(JSON_EXTRACT(t.scope,'$.selection')) AS BINARY)='prefix' AND
                        LEFT(NEW.object_key,OCTET_LENGTH(JSON_UNQUOTE(JSON_EXTRACT(t.scope,'$.prefix'))))=CAST(JSON_UNQUOTE(JSON_EXTRACT(t.scope,'$.prefix')) AS BINARY)
                    OR CAST(JSON_UNQUOTE(JSON_EXTRACT(t.scope,'$.selection')) AS BINARY)='objects' AND
                        JSON_CONTAINS(JSON_EXTRACT(t.scope,'$.keys'),JSON_QUOTE(CAST(NEW.object_key AS CHAR CHARACTER SET utf8mb4))))
                ON DUPLICATE KEY UPDATE object_key=IF(BINARY transfer_changes.object_key=BINARY VALUES(object_key),transfer_changes.object_key,NULL),
                    source_version=VALUES(source_version),snapshot=VALUES(snapshot),revision=transfer_changes.revision+1;
        END IF;
    END IF;
END;

DROP TRIGGER IF EXISTS objects_capture_delete;
CREATE TRIGGER objects_capture_delete AFTER DELETE ON objects FOR EACH ROW
BEGIN
    DECLARE readers INTEGER DEFAULT 0;
    IF OLD.stream_id IS NOT NULL THEN
        SELECT subscribers INTO readers FROM bucket_change_gates WHERE bucket_id=OLD.bucket_id LOCK IN SHARE MODE;
        IF readers>0 THEN
            INSERT INTO transfer_changes(task_id,object_key,source_version,snapshot)
                SELECT task_id,CAST(OLD.object_key AS CHAR CHARACTER SET utf8mb4),NULL,NULL FROM transfer_subscriptions
                WHERE bucket_id=OLD.bucket_id AND active AND (
                    CAST(JSON_UNQUOTE(JSON_EXTRACT(scope,'$.selection')) AS BINARY)='all'
                    OR CAST(JSON_UNQUOTE(JSON_EXTRACT(scope,'$.selection')) AS BINARY)='prefix' AND
                        LEFT(OLD.object_key,OCTET_LENGTH(JSON_UNQUOTE(JSON_EXTRACT(scope,'$.prefix'))))=CAST(JSON_UNQUOTE(JSON_EXTRACT(scope,'$.prefix')) AS BINARY)
                    OR CAST(JSON_UNQUOTE(JSON_EXTRACT(scope,'$.selection')) AS BINARY)='objects' AND
                        JSON_CONTAINS(JSON_EXTRACT(scope,'$.keys'),JSON_QUOTE(CAST(OLD.object_key AS CHAR CHARACTER SET utf8mb4))))
                ON DUPLICATE KEY UPDATE object_key=IF(BINARY transfer_changes.object_key=BINARY VALUES(object_key),transfer_changes.object_key,NULL),
                    source_version=NULL,snapshot=NULL,revision=transfer_changes.revision+1;
        END IF;
    END IF;
END;

UPDATE mokyu_meta SET schema_version = 20261103205120;
