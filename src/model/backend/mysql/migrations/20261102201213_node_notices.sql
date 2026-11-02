CREATE TABLE notification_consumers (
    node_id BINARY(16) NOT NULL PRIMARY KEY,
    generation BINARY(16) NOT NULL,
    role VARCHAR(8) NOT NULL CHECK (role IN ('control','data','worker','node')),
    expires_at DATETIME(6) NOT NULL,
    FOREIGN KEY (node_id) REFERENCES nodes(id) ON DELETE CASCADE
) ENGINE=InnoDB;

CREATE TABLE node_notices (
    node_id BINARY(16) NOT NULL,
    slot SMALLINT NOT NULL CHECK (slot BETWEEN 0 AND 1023),
    generation BINARY(16) NOT NULL,
    event_id BINARY(16) NOT NULL UNIQUE,
    scope VARCHAR(8) NOT NULL CHECK (scope IN ('object','bucket','storage','reset')),
    resource_id BINARY(16) NOT NULL,
    object_key VARBINARY(1024) NOT NULL,
    retry_at DATETIME(6),
    PRIMARY KEY (node_id,slot),
    FOREIGN KEY (node_id) REFERENCES notification_consumers(node_id) ON DELETE CASCADE
) ENGINE=InnoDB;

CREATE TABLE notice_membership (id TINYINT NOT NULL PRIMARY KEY, CONSTRAINT notice_membership_single CHECK (id=1)) ENGINE=InnoDB;
INSERT INTO notice_membership(id) VALUES(1);

CREATE TRIGGER objects_notice_insert AFTER INSERT ON objects FOR EACH ROW
BEGIN
    DECLARE gate TINYINT;
    IF (SELECT deployment_mode FROM mokyu_meta WHERE id=1)='single_control' AND (NEW.stream_id IS NOT NULL) THEN
        SELECT id INTO gate FROM notice_membership WHERE id=1 LOCK IN SHARE MODE;
        INSERT INTO node_notices(node_id,slot,generation,event_id,scope,resource_id,object_key)
    SELECT c.node_id,(CRC32(CONCAT(NEW.bucket_id,NEW.object_key)) & 1023),c.generation,UNHEX(REPLACE(UUID(),'-','')),'object',NEW.bucket_id,NEW.object_key
    FROM notification_consumers c WHERE c.expires_at>UTC_TIMESTAMP(6) AND ('object'='storage' OR c.role<>'worker')
    ON DUPLICATE KEY UPDATE scope=CASE WHEN node_notices.generation<>VALUES(generation) THEN VALUES(scope)
        WHEN node_notices.scope=VALUES(scope) AND node_notices.resource_id=VALUES(resource_id) AND node_notices.object_key=VALUES(object_key) THEN VALUES(scope)
        WHEN node_notices.scope IN ('object','bucket') AND VALUES(scope) IN ('object','bucket') AND node_notices.resource_id=VALUES(resource_id) THEN 'bucket'
        ELSE 'reset' END,
        generation=VALUES(generation),event_id=VALUES(event_id),resource_id=VALUES(resource_id),object_key=VALUES(object_key),retry_at=NULL;
    END IF;
END;

CREATE TRIGGER objects_notice_update AFTER UPDATE ON objects FOR EACH ROW
BEGIN
    DECLARE gate TINYINT;
    IF (SELECT deployment_mode FROM mokyu_meta WHERE id=1)='single_control' AND (NOT (OLD.stream_id<=>NEW.stream_id)) THEN
        SELECT id INTO gate FROM notice_membership WHERE id=1 LOCK IN SHARE MODE;
        INSERT INTO node_notices(node_id,slot,generation,event_id,scope,resource_id,object_key)
    SELECT c.node_id,(CRC32(CONCAT(NEW.bucket_id,NEW.object_key)) & 1023),c.generation,UNHEX(REPLACE(UUID(),'-','')),'object',NEW.bucket_id,NEW.object_key
    FROM notification_consumers c WHERE c.expires_at>UTC_TIMESTAMP(6) AND ('object'='storage' OR c.role<>'worker')
    ON DUPLICATE KEY UPDATE scope=CASE WHEN node_notices.generation<>VALUES(generation) THEN VALUES(scope)
        WHEN node_notices.scope=VALUES(scope) AND node_notices.resource_id=VALUES(resource_id) AND node_notices.object_key=VALUES(object_key) THEN VALUES(scope)
        WHEN node_notices.scope IN ('object','bucket') AND VALUES(scope) IN ('object','bucket') AND node_notices.resource_id=VALUES(resource_id) THEN 'bucket'
        ELSE 'reset' END,
        generation=VALUES(generation),event_id=VALUES(event_id),resource_id=VALUES(resource_id),object_key=VALUES(object_key),retry_at=NULL;
    END IF;
END;

CREATE TRIGGER objects_notice_delete AFTER DELETE ON objects FOR EACH ROW
BEGIN
    DECLARE gate TINYINT;
    IF (SELECT deployment_mode FROM mokyu_meta WHERE id=1)='single_control' AND (OLD.stream_id IS NOT NULL) THEN
        SELECT id INTO gate FROM notice_membership WHERE id=1 LOCK IN SHARE MODE;
        INSERT INTO node_notices(node_id,slot,generation,event_id,scope,resource_id,object_key)
    SELECT c.node_id,(CRC32(CONCAT(OLD.bucket_id,OLD.object_key)) & 1023),c.generation,UNHEX(REPLACE(UUID(),'-','')),'object',OLD.bucket_id,OLD.object_key
    FROM notification_consumers c WHERE c.expires_at>UTC_TIMESTAMP(6) AND ('object'='storage' OR c.role<>'worker')
    ON DUPLICATE KEY UPDATE scope=CASE WHEN node_notices.generation<>VALUES(generation) THEN VALUES(scope)
        WHEN node_notices.scope=VALUES(scope) AND node_notices.resource_id=VALUES(resource_id) AND node_notices.object_key=VALUES(object_key) THEN VALUES(scope)
        WHEN node_notices.scope IN ('object','bucket') AND VALUES(scope) IN ('object','bucket') AND node_notices.resource_id=VALUES(resource_id) THEN 'bucket'
        ELSE 'reset' END,
        generation=VALUES(generation),event_id=VALUES(event_id),resource_id=VALUES(resource_id),object_key=VALUES(object_key),retry_at=NULL;
    END IF;
END;

CREATE TRIGGER streams_notice_update AFTER UPDATE ON streams FOR EACH ROW
BEGIN
    DECLARE gate TINYINT;
    IF (SELECT deployment_mode FROM mokyu_meta WHERE id=1)='single_control' AND (NEW.stream_type='object' AND NEW.state='ready' AND (NOT (OLD.public_read<=>NEW.public_read) OR NOT (OLD.etag<=>NEW.etag) OR NOT (OLD.size<=>NEW.size) OR NOT (OLD.metadata<=>NEW.metadata) OR NOT (OLD.payload_revision<=>NEW.payload_revision)) AND EXISTS(SELECT 1 FROM objects o WHERE o.bucket_id=NEW.bucket_id AND o.object_key=NEW.object_key AND o.stream_id=NEW.id)) THEN
        SELECT id INTO gate FROM notice_membership WHERE id=1 LOCK IN SHARE MODE;
        INSERT INTO node_notices(node_id,slot,generation,event_id,scope,resource_id,object_key)
    SELECT c.node_id,(CRC32(CONCAT(NEW.bucket_id,NEW.object_key)) & 1023),c.generation,UNHEX(REPLACE(UUID(),'-','')),'object',NEW.bucket_id,NEW.object_key
    FROM notification_consumers c WHERE c.expires_at>UTC_TIMESTAMP(6) AND ('object'='storage' OR c.role<>'worker')
    ON DUPLICATE KEY UPDATE scope=CASE WHEN node_notices.generation<>VALUES(generation) THEN VALUES(scope)
        WHEN node_notices.scope=VALUES(scope) AND node_notices.resource_id=VALUES(resource_id) AND node_notices.object_key=VALUES(object_key) THEN VALUES(scope)
        WHEN node_notices.scope IN ('object','bucket') AND VALUES(scope) IN ('object','bucket') AND node_notices.resource_id=VALUES(resource_id) THEN 'bucket'
        ELSE 'reset' END,
        generation=VALUES(generation),event_id=VALUES(event_id),resource_id=VALUES(resource_id),object_key=VALUES(object_key),retry_at=NULL;
    END IF;
END;

CREATE TRIGGER buckets_notice_update AFTER UPDATE ON buckets FOR EACH ROW
BEGIN
    DECLARE gate TINYINT;
    IF (SELECT deployment_mode FROM mokyu_meta WHERE id=1)='single_control' THEN
        SELECT id INTO gate FROM notice_membership WHERE id=1 LOCK IN SHARE MODE;
        INSERT INTO node_notices(node_id,slot,generation,event_id,scope,resource_id,object_key)
    SELECT c.node_id,(CRC32(CONCAT(NEW.id,X'')) & 1023),c.generation,UNHEX(REPLACE(UUID(),'-','')),'bucket',NEW.id,X''
    FROM notification_consumers c WHERE c.expires_at>UTC_TIMESTAMP(6) AND ('bucket'='storage' OR c.role<>'worker')
    ON DUPLICATE KEY UPDATE scope=CASE WHEN node_notices.generation<>VALUES(generation) THEN VALUES(scope)
        WHEN node_notices.scope=VALUES(scope) AND node_notices.resource_id=VALUES(resource_id) AND node_notices.object_key=VALUES(object_key) THEN VALUES(scope)
        WHEN node_notices.scope IN ('object','bucket') AND VALUES(scope) IN ('object','bucket') AND node_notices.resource_id=VALUES(resource_id) THEN 'bucket'
        ELSE 'reset' END,
        generation=VALUES(generation),event_id=VALUES(event_id),resource_id=VALUES(resource_id),object_key=VALUES(object_key),retry_at=NULL;
    END IF;
END;

CREATE TRIGGER buckets_notice_delete AFTER DELETE ON buckets FOR EACH ROW
BEGIN
    DECLARE gate TINYINT;
    IF (SELECT deployment_mode FROM mokyu_meta WHERE id=1)='single_control' THEN
        SELECT id INTO gate FROM notice_membership WHERE id=1 LOCK IN SHARE MODE;
        INSERT INTO node_notices(node_id,slot,generation,event_id,scope,resource_id,object_key)
    SELECT c.node_id,(CRC32(CONCAT(OLD.id,X'')) & 1023),c.generation,UNHEX(REPLACE(UUID(),'-','')),'bucket',OLD.id,X''
    FROM notification_consumers c WHERE c.expires_at>UTC_TIMESTAMP(6) AND ('bucket'='storage' OR c.role<>'worker')
    ON DUPLICATE KEY UPDATE scope=CASE WHEN node_notices.generation<>VALUES(generation) THEN VALUES(scope)
        WHEN node_notices.scope=VALUES(scope) AND node_notices.resource_id=VALUES(resource_id) AND node_notices.object_key=VALUES(object_key) THEN VALUES(scope)
        WHEN node_notices.scope IN ('object','bucket') AND VALUES(scope) IN ('object','bucket') AND node_notices.resource_id=VALUES(resource_id) THEN 'bucket'
        ELSE 'reset' END,
        generation=VALUES(generation),event_id=VALUES(event_id),resource_id=VALUES(resource_id),object_key=VALUES(object_key),retry_at=NULL;
    END IF;
END;

CREATE TRIGGER bucket_domains_notice_insert AFTER INSERT ON bucket_domains FOR EACH ROW
BEGIN
    DECLARE gate TINYINT;
    IF (SELECT deployment_mode FROM mokyu_meta WHERE id=1)='single_control' THEN
        SELECT id INTO gate FROM notice_membership WHERE id=1 LOCK IN SHARE MODE;
        INSERT INTO node_notices(node_id,slot,generation,event_id,scope,resource_id,object_key)
    SELECT c.node_id,(CRC32(CONCAT(NEW.bucket_id,X'')) & 1023),c.generation,UNHEX(REPLACE(UUID(),'-','')),'bucket',NEW.bucket_id,X''
    FROM notification_consumers c WHERE c.expires_at>UTC_TIMESTAMP(6) AND ('bucket'='storage' OR c.role<>'worker')
    ON DUPLICATE KEY UPDATE scope=CASE WHEN node_notices.generation<>VALUES(generation) THEN VALUES(scope)
        WHEN node_notices.scope=VALUES(scope) AND node_notices.resource_id=VALUES(resource_id) AND node_notices.object_key=VALUES(object_key) THEN VALUES(scope)
        WHEN node_notices.scope IN ('object','bucket') AND VALUES(scope) IN ('object','bucket') AND node_notices.resource_id=VALUES(resource_id) THEN 'bucket'
        ELSE 'reset' END,
        generation=VALUES(generation),event_id=VALUES(event_id),resource_id=VALUES(resource_id),object_key=VALUES(object_key),retry_at=NULL;
    END IF;
END;

CREATE TRIGGER bucket_domains_notice_delete AFTER DELETE ON bucket_domains FOR EACH ROW
BEGIN
    DECLARE gate TINYINT;
    IF (SELECT deployment_mode FROM mokyu_meta WHERE id=1)='single_control' THEN
        SELECT id INTO gate FROM notice_membership WHERE id=1 LOCK IN SHARE MODE;
        INSERT INTO node_notices(node_id,slot,generation,event_id,scope,resource_id,object_key)
    SELECT c.node_id,(CRC32(CONCAT(OLD.bucket_id,X'')) & 1023),c.generation,UNHEX(REPLACE(UUID(),'-','')),'bucket',OLD.bucket_id,X''
    FROM notification_consumers c WHERE c.expires_at>UTC_TIMESTAMP(6) AND ('bucket'='storage' OR c.role<>'worker')
    ON DUPLICATE KEY UPDATE scope=CASE WHEN node_notices.generation<>VALUES(generation) THEN VALUES(scope)
        WHEN node_notices.scope=VALUES(scope) AND node_notices.resource_id=VALUES(resource_id) AND node_notices.object_key=VALUES(object_key) THEN VALUES(scope)
        WHEN node_notices.scope IN ('object','bucket') AND VALUES(scope) IN ('object','bucket') AND node_notices.resource_id=VALUES(resource_id) THEN 'bucket'
        ELSE 'reset' END,
        generation=VALUES(generation),event_id=VALUES(event_id),resource_id=VALUES(resource_id),object_key=VALUES(object_key),retry_at=NULL;
    END IF;
END;

CREATE TRIGGER bucket_domains_notice_update AFTER UPDATE ON bucket_domains FOR EACH ROW
BEGIN
    DECLARE gate TINYINT;
    IF (SELECT deployment_mode FROM mokyu_meta WHERE id=1)='single_control' THEN
        SELECT id INTO gate FROM notice_membership WHERE id=1 LOCK IN SHARE MODE;
        INSERT INTO node_notices(node_id,slot,generation,event_id,scope,resource_id,object_key)
    SELECT c.node_id,(CRC32(CONCAT(OLD.bucket_id,X'')) & 1023),c.generation,UNHEX(REPLACE(UUID(),'-','')),'bucket',OLD.bucket_id,X''
    FROM notification_consumers c WHERE c.expires_at>UTC_TIMESTAMP(6) AND ('bucket'='storage' OR c.role<>'worker')
    ON DUPLICATE KEY UPDATE scope=CASE WHEN node_notices.generation<>VALUES(generation) THEN VALUES(scope)
        WHEN node_notices.scope=VALUES(scope) AND node_notices.resource_id=VALUES(resource_id) AND node_notices.object_key=VALUES(object_key) THEN VALUES(scope)
        WHEN node_notices.scope IN ('object','bucket') AND VALUES(scope) IN ('object','bucket') AND node_notices.resource_id=VALUES(resource_id) THEN 'bucket'
        ELSE 'reset' END,
        generation=VALUES(generation),event_id=VALUES(event_id),resource_id=VALUES(resource_id),object_key=VALUES(object_key),retry_at=NULL;
    END IF;
END;

CREATE TRIGGER bucket_domains_notice_update_destination AFTER UPDATE ON bucket_domains FOR EACH ROW
BEGIN
    DECLARE gate TINYINT;
    IF (SELECT deployment_mode FROM mokyu_meta WHERE id=1)='single_control' AND (NOT (OLD.bucket_id<=>NEW.bucket_id)) THEN
        SELECT id INTO gate FROM notice_membership WHERE id=1 LOCK IN SHARE MODE;
        INSERT INTO node_notices(node_id,slot,generation,event_id,scope,resource_id,object_key)
    SELECT c.node_id,(CRC32(CONCAT(NEW.bucket_id,X'')) & 1023),c.generation,UNHEX(REPLACE(UUID(),'-','')),'bucket',NEW.bucket_id,X''
    FROM notification_consumers c WHERE c.expires_at>UTC_TIMESTAMP(6) AND ('bucket'='storage' OR c.role<>'worker')
    ON DUPLICATE KEY UPDATE scope=CASE WHEN node_notices.generation<>VALUES(generation) THEN VALUES(scope)
        WHEN node_notices.scope=VALUES(scope) AND node_notices.resource_id=VALUES(resource_id) AND node_notices.object_key=VALUES(object_key) THEN VALUES(scope)
        WHEN node_notices.scope IN ('object','bucket') AND VALUES(scope) IN ('object','bucket') AND node_notices.resource_id=VALUES(resource_id) THEN 'bucket'
        ELSE 'reset' END,
        generation=VALUES(generation),event_id=VALUES(event_id),resource_id=VALUES(resource_id),object_key=VALUES(object_key),retry_at=NULL;
    END IF;
END;

CREATE TRIGGER storages_notice_update AFTER UPDATE ON storages FOR EACH ROW
BEGIN
    DECLARE gate TINYINT;
    IF (SELECT deployment_mode FROM mokyu_meta WHERE id=1)='single_control' THEN
        SELECT id INTO gate FROM notice_membership WHERE id=1 LOCK IN SHARE MODE;
        INSERT INTO node_notices(node_id,slot,generation,event_id,scope,resource_id,object_key)
    SELECT c.node_id,(CRC32(CONCAT(NEW.id,X'')) & 1023),c.generation,UNHEX(REPLACE(UUID(),'-','')),'storage',NEW.id,X''
    FROM notification_consumers c WHERE c.expires_at>UTC_TIMESTAMP(6) AND ('storage'='storage' OR c.role<>'worker')
    ON DUPLICATE KEY UPDATE scope=CASE WHEN node_notices.generation<>VALUES(generation) THEN VALUES(scope)
        WHEN node_notices.scope=VALUES(scope) AND node_notices.resource_id=VALUES(resource_id) AND node_notices.object_key=VALUES(object_key) THEN VALUES(scope)
        WHEN node_notices.scope IN ('object','bucket') AND VALUES(scope) IN ('object','bucket') AND node_notices.resource_id=VALUES(resource_id) THEN 'bucket'
        ELSE 'reset' END,
        generation=VALUES(generation),event_id=VALUES(event_id),resource_id=VALUES(resource_id),object_key=VALUES(object_key),retry_at=NULL;
    END IF;
END;

CREATE TRIGGER storages_notice_delete AFTER DELETE ON storages FOR EACH ROW
BEGIN
    DECLARE gate TINYINT;
    IF (SELECT deployment_mode FROM mokyu_meta WHERE id=1)='single_control' THEN
        SELECT id INTO gate FROM notice_membership WHERE id=1 LOCK IN SHARE MODE;
        INSERT INTO node_notices(node_id,slot,generation,event_id,scope,resource_id,object_key)
    SELECT c.node_id,(CRC32(CONCAT(OLD.id,X'')) & 1023),c.generation,UNHEX(REPLACE(UUID(),'-','')),'storage',OLD.id,X''
    FROM notification_consumers c WHERE c.expires_at>UTC_TIMESTAMP(6) AND ('storage'='storage' OR c.role<>'worker')
    ON DUPLICATE KEY UPDATE scope=CASE WHEN node_notices.generation<>VALUES(generation) THEN VALUES(scope)
        WHEN node_notices.scope=VALUES(scope) AND node_notices.resource_id=VALUES(resource_id) AND node_notices.object_key=VALUES(object_key) THEN VALUES(scope)
        WHEN node_notices.scope IN ('object','bucket') AND VALUES(scope) IN ('object','bucket') AND node_notices.resource_id=VALUES(resource_id) THEN 'bucket'
        ELSE 'reset' END,
        generation=VALUES(generation),event_id=VALUES(event_id),resource_id=VALUES(resource_id),object_key=VALUES(object_key),retry_at=NULL;
    END IF;
END;

CREATE TRIGGER storage_profiles_notice_update AFTER UPDATE ON storage_profiles FOR EACH ROW
BEGIN
    DECLARE gate TINYINT;
    IF (SELECT deployment_mode FROM mokyu_meta WHERE id=1)='single_control' THEN
        SELECT id INTO gate FROM notice_membership WHERE id=1 LOCK IN SHARE MODE;
        INSERT INTO node_notices(node_id,slot,generation,event_id,scope,resource_id,object_key)
    SELECT c.node_id,(CRC32(CONCAT(NEW.storage_id,X'')) & 1023),c.generation,UNHEX(REPLACE(UUID(),'-','')),'storage',NEW.storage_id,X''
    FROM notification_consumers c WHERE c.expires_at>UTC_TIMESTAMP(6) AND ('storage'='storage' OR c.role<>'worker')
    ON DUPLICATE KEY UPDATE scope=CASE WHEN node_notices.generation<>VALUES(generation) THEN VALUES(scope)
        WHEN node_notices.scope=VALUES(scope) AND node_notices.resource_id=VALUES(resource_id) AND node_notices.object_key=VALUES(object_key) THEN VALUES(scope)
        WHEN node_notices.scope IN ('object','bucket') AND VALUES(scope) IN ('object','bucket') AND node_notices.resource_id=VALUES(resource_id) THEN 'bucket'
        ELSE 'reset' END,
        generation=VALUES(generation),event_id=VALUES(event_id),resource_id=VALUES(resource_id),object_key=VALUES(object_key),retry_at=NULL;
    END IF;
END;

CREATE TRIGGER storage_profiles_notice_delete AFTER DELETE ON storage_profiles FOR EACH ROW
BEGIN
    DECLARE gate TINYINT;
    IF (SELECT deployment_mode FROM mokyu_meta WHERE id=1)='single_control' THEN
        SELECT id INTO gate FROM notice_membership WHERE id=1 LOCK IN SHARE MODE;
        INSERT INTO node_notices(node_id,slot,generation,event_id,scope,resource_id,object_key)
    SELECT c.node_id,(CRC32(CONCAT(OLD.storage_id,X'')) & 1023),c.generation,UNHEX(REPLACE(UUID(),'-','')),'storage',OLD.storage_id,X''
    FROM notification_consumers c WHERE c.expires_at>UTC_TIMESTAMP(6) AND ('storage'='storage' OR c.role<>'worker')
    ON DUPLICATE KEY UPDATE scope=CASE WHEN node_notices.generation<>VALUES(generation) THEN VALUES(scope)
        WHEN node_notices.scope=VALUES(scope) AND node_notices.resource_id=VALUES(resource_id) AND node_notices.object_key=VALUES(object_key) THEN VALUES(scope)
        WHEN node_notices.scope IN ('object','bucket') AND VALUES(scope) IN ('object','bucket') AND node_notices.resource_id=VALUES(resource_id) THEN 'bucket'
        ELSE 'reset' END,
        generation=VALUES(generation),event_id=VALUES(event_id),resource_id=VALUES(resource_id),object_key=VALUES(object_key),retry_at=NULL;
    END IF;
END;

CREATE TRIGGER chunk_encodings_notice_update AFTER UPDATE ON chunk_encodings FOR EACH ROW
BEGIN
    DECLARE gate TINYINT;
    IF (SELECT deployment_mode FROM mokyu_meta WHERE id=1)='single_control' AND ((NOT (OLD.quarantine_code<=>NEW.quarantine_code) OR NOT (OLD.quarantine_revision<=>NEW.quarantine_revision))) THEN
        SELECT id INTO gate FROM notice_membership WHERE id=1 LOCK IN SHARE MODE;
        INSERT INTO node_notices(node_id,slot,generation,event_id,scope,resource_id,object_key)
    SELECT c.node_id,(CRC32(CONCAT(NEW.storage_id,X'')) & 1023),c.generation,UNHEX(REPLACE(UUID(),'-','')),'storage',NEW.storage_id,X''
    FROM notification_consumers c WHERE c.expires_at>UTC_TIMESTAMP(6) AND ('storage'='storage' OR c.role<>'worker')
    ON DUPLICATE KEY UPDATE scope=CASE WHEN node_notices.generation<>VALUES(generation) THEN VALUES(scope)
        WHEN node_notices.scope=VALUES(scope) AND node_notices.resource_id=VALUES(resource_id) AND node_notices.object_key=VALUES(object_key) THEN VALUES(scope)
        WHEN node_notices.scope IN ('object','bucket') AND VALUES(scope) IN ('object','bucket') AND node_notices.resource_id=VALUES(resource_id) THEN 'bucket'
        ELSE 'reset' END,
        generation=VALUES(generation),event_id=VALUES(event_id),resource_id=VALUES(resource_id),object_key=VALUES(object_key),retry_at=NULL;
    END IF;
END;

CREATE TRIGGER packs_notice_update AFTER UPDATE ON packs FOR EACH ROW
BEGIN
    DECLARE gate TINYINT;
    IF (SELECT deployment_mode FROM mokyu_meta WHERE id=1)='single_control' AND ((NOT (OLD.quarantine_code<=>NEW.quarantine_code) OR NOT (OLD.quarantine_revision<=>NEW.quarantine_revision))) THEN
        SELECT id INTO gate FROM notice_membership WHERE id=1 LOCK IN SHARE MODE;
        INSERT INTO node_notices(node_id,slot,generation,event_id,scope,resource_id,object_key)
    SELECT c.node_id,(CRC32(CONCAT(NEW.storage_id,X'')) & 1023),c.generation,UNHEX(REPLACE(UUID(),'-','')),'storage',NEW.storage_id,X''
    FROM notification_consumers c WHERE c.expires_at>UTC_TIMESTAMP(6) AND ('storage'='storage' OR c.role<>'worker')
    ON DUPLICATE KEY UPDATE scope=CASE WHEN node_notices.generation<>VALUES(generation) THEN VALUES(scope)
        WHEN node_notices.scope=VALUES(scope) AND node_notices.resource_id=VALUES(resource_id) AND node_notices.object_key=VALUES(object_key) THEN VALUES(scope)
        WHEN node_notices.scope IN ('object','bucket') AND VALUES(scope) IN ('object','bucket') AND node_notices.resource_id=VALUES(resource_id) THEN 'bucket'
        ELSE 'reset' END,
        generation=VALUES(generation),event_id=VALUES(event_id),resource_id=VALUES(resource_id),object_key=VALUES(object_key),retry_at=NULL;
    END IF;
END;

CREATE TRIGGER raw_sources_notice_update AFTER UPDATE ON raw_sources FOR EACH ROW
BEGIN
    DECLARE gate TINYINT;
    IF (SELECT deployment_mode FROM mokyu_meta WHERE id=1)='single_control' AND (NOT (OLD.quarantine_code<=>NEW.quarantine_code)) THEN
        SELECT id INTO gate FROM notice_membership WHERE id=1 LOCK IN SHARE MODE;
        INSERT INTO node_notices(node_id,slot,generation,event_id,scope,resource_id,object_key)
    SELECT c.node_id,(CRC32(CONCAT(NEW.storage_id,X'')) & 1023),c.generation,UNHEX(REPLACE(UUID(),'-','')),'storage',NEW.storage_id,X''
    FROM notification_consumers c WHERE c.expires_at>UTC_TIMESTAMP(6) AND ('storage'='storage' OR c.role<>'worker')
    ON DUPLICATE KEY UPDATE scope=CASE WHEN node_notices.generation<>VALUES(generation) THEN VALUES(scope)
        WHEN node_notices.scope=VALUES(scope) AND node_notices.resource_id=VALUES(resource_id) AND node_notices.object_key=VALUES(object_key) THEN VALUES(scope)
        WHEN node_notices.scope IN ('object','bucket') AND VALUES(scope) IN ('object','bucket') AND node_notices.resource_id=VALUES(resource_id) THEN 'bucket'
        ELSE 'reset' END,
        generation=VALUES(generation),event_id=VALUES(event_id),resource_id=VALUES(resource_id),object_key=VALUES(object_key),retry_at=NULL;
    END IF;
END;

CREATE TRIGGER tier_locations_notice_insert AFTER INSERT ON tier_locations FOR EACH ROW
BEGIN
    DECLARE gate TINYINT;
    IF (SELECT deployment_mode FROM mokyu_meta WHERE id=1)='single_control' THEN
        SELECT id INTO gate FROM notice_membership WHERE id=1 LOCK IN SHARE MODE;
        INSERT INTO node_notices(node_id,slot,generation,event_id,scope,resource_id,object_key)
    SELECT c.node_id,(CRC32(CONCAT(NEW.home_storage,X'')) & 1023),c.generation,UNHEX(REPLACE(UUID(),'-','')),'storage',NEW.home_storage,X''
    FROM notification_consumers c WHERE c.expires_at>UTC_TIMESTAMP(6) AND ('storage'='storage' OR c.role<>'worker')
    ON DUPLICATE KEY UPDATE scope=CASE WHEN node_notices.generation<>VALUES(generation) THEN VALUES(scope)
        WHEN node_notices.scope=VALUES(scope) AND node_notices.resource_id=VALUES(resource_id) AND node_notices.object_key=VALUES(object_key) THEN VALUES(scope)
        WHEN node_notices.scope IN ('object','bucket') AND VALUES(scope) IN ('object','bucket') AND node_notices.resource_id=VALUES(resource_id) THEN 'bucket'
        ELSE 'reset' END,
        generation=VALUES(generation),event_id=VALUES(event_id),resource_id=VALUES(resource_id),object_key=VALUES(object_key),retry_at=NULL;
    END IF;
END;

CREATE TRIGGER tier_locations_notice_update AFTER UPDATE ON tier_locations FOR EACH ROW
BEGIN
    DECLARE gate TINYINT;
    IF (SELECT deployment_mode FROM mokyu_meta WHERE id=1)='single_control' AND (NOT (OLD.generation<=>NEW.generation)) THEN
        SELECT id INTO gate FROM notice_membership WHERE id=1 LOCK IN SHARE MODE;
        INSERT INTO node_notices(node_id,slot,generation,event_id,scope,resource_id,object_key)
    SELECT c.node_id,(CRC32(CONCAT(NEW.home_storage,X'')) & 1023),c.generation,UNHEX(REPLACE(UUID(),'-','')),'storage',NEW.home_storage,X''
    FROM notification_consumers c WHERE c.expires_at>UTC_TIMESTAMP(6) AND ('storage'='storage' OR c.role<>'worker')
    ON DUPLICATE KEY UPDATE scope=CASE WHEN node_notices.generation<>VALUES(generation) THEN VALUES(scope)
        WHEN node_notices.scope=VALUES(scope) AND node_notices.resource_id=VALUES(resource_id) AND node_notices.object_key=VALUES(object_key) THEN VALUES(scope)
        WHEN node_notices.scope IN ('object','bucket') AND VALUES(scope) IN ('object','bucket') AND node_notices.resource_id=VALUES(resource_id) THEN 'bucket'
        ELSE 'reset' END,
        generation=VALUES(generation),event_id=VALUES(event_id),resource_id=VALUES(resource_id),object_key=VALUES(object_key),retry_at=NULL;
    END IF;
END;

CREATE TRIGGER tier_locations_notice_delete AFTER DELETE ON tier_locations FOR EACH ROW
BEGIN
    DECLARE gate TINYINT;
    IF (SELECT deployment_mode FROM mokyu_meta WHERE id=1)='single_control' THEN
        SELECT id INTO gate FROM notice_membership WHERE id=1 LOCK IN SHARE MODE;
        INSERT INTO node_notices(node_id,slot,generation,event_id,scope,resource_id,object_key)
    SELECT c.node_id,(CRC32(CONCAT(OLD.home_storage,X'')) & 1023),c.generation,UNHEX(REPLACE(UUID(),'-','')),'storage',OLD.home_storage,X''
    FROM notification_consumers c WHERE c.expires_at>UTC_TIMESTAMP(6) AND ('storage'='storage' OR c.role<>'worker')
    ON DUPLICATE KEY UPDATE scope=CASE WHEN node_notices.generation<>VALUES(generation) THEN VALUES(scope)
        WHEN node_notices.scope=VALUES(scope) AND node_notices.resource_id=VALUES(resource_id) AND node_notices.object_key=VALUES(object_key) THEN VALUES(scope)
        WHEN node_notices.scope IN ('object','bucket') AND VALUES(scope) IN ('object','bucket') AND node_notices.resource_id=VALUES(resource_id) THEN 'bucket'
        ELSE 'reset' END,
        generation=VALUES(generation),event_id=VALUES(event_id),resource_id=VALUES(resource_id),object_key=VALUES(object_key),retry_at=NULL;
    END IF;
END;

UPDATE mokyu_meta SET schema_version = 20261102201213;
