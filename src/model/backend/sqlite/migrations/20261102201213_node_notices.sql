CREATE TABLE notification_consumers (
    node_id BLOB NOT NULL PRIMARY KEY,
    generation BLOB NOT NULL,
    role TEXT NOT NULL CHECK (role IN ('control','data','worker','node')),
    expires_at INTEGER NOT NULL,
    FOREIGN KEY (node_id) REFERENCES nodes(id) ON DELETE CASCADE
) STRICT, WITHOUT ROWID;

CREATE TABLE node_notices (
    node_id BLOB NOT NULL,
    slot INTEGER NOT NULL CHECK (slot BETWEEN 0 AND 1023),
    generation BLOB NOT NULL,
    event_id BLOB NOT NULL UNIQUE,
    scope TEXT NOT NULL CHECK (scope IN ('object','bucket','storage','reset')),
    resource_id BLOB NOT NULL,
    object_key BLOB NOT NULL,
    retry_at INTEGER,
    PRIMARY KEY (node_id,slot),
    FOREIGN KEY (node_id) REFERENCES notification_consumers(node_id) ON DELETE CASCADE
) STRICT, WITHOUT ROWID;

CREATE VIEW notice_changes(scope,resource_id,object_key) AS SELECT scope,resource_id,object_key FROM node_notices WHERE FALSE;
CREATE TRIGGER notice_publish INSTEAD OF INSERT ON notice_changes
WHEN (SELECT deployment_mode FROM mokyu_meta WHERE id=1)='single_control'
BEGIN
    INSERT INTO node_notices(node_id,slot,generation,event_id,scope,resource_id,object_key)
    SELECT c.node_id,(
        instr('0123456789ABCDEF',substr(hex(NEW.resource_id),-1))*61 +
        instr('0123456789ABCDEF',substr(hex(NEW.resource_id),-2,1))*17 +
        length(NEW.object_key)*31 + coalesce(unicode(substr(CAST(NEW.object_key AS TEXT),-1)),0)*7 +
        coalesce(unicode(substr(CAST(NEW.object_key AS TEXT),-3,1)),0)*53 +
        coalesce(unicode(substr(CAST(NEW.object_key AS TEXT),1+length(CAST(NEW.object_key AS TEXT))/2,1)),0)
    ) & 1023,c.generation,randomblob(16),NEW.scope,NEW.resource_id,NEW.object_key
    FROM notification_consumers c WHERE c.expires_at>CAST(unixepoch('subsec')*1000000 AS INTEGER)
        AND (NEW.scope='storage' OR c.role<>'worker')
    ON CONFLICT (node_id,slot) DO UPDATE SET scope=CASE WHEN node_notices.generation<>excluded.generation THEN excluded.scope
        WHEN node_notices.scope=excluded.scope AND node_notices.resource_id=excluded.resource_id AND node_notices.object_key=excluded.object_key THEN excluded.scope
        WHEN node_notices.scope IN ('object','bucket') AND excluded.scope IN ('object','bucket') AND node_notices.resource_id=excluded.resource_id THEN 'bucket'
        ELSE 'reset' END,
        generation=excluded.generation,event_id=excluded.event_id,resource_id=excluded.resource_id,object_key=excluded.object_key,retry_at=NULL;
END;

CREATE TRIGGER objects_notice_insert AFTER INSERT ON objects
WHEN NEW.stream_id IS NOT NULL
BEGIN
    INSERT INTO notice_changes VALUES('object',NEW.bucket_id,NEW.object_key);
END;

CREATE TRIGGER objects_notice_update AFTER UPDATE ON objects
WHEN OLD.stream_id IS NOT NEW.stream_id
BEGIN
    INSERT INTO notice_changes VALUES('object',NEW.bucket_id,NEW.object_key);
END;

CREATE TRIGGER objects_notice_delete AFTER DELETE ON objects
WHEN OLD.stream_id IS NOT NULL
BEGIN
    INSERT INTO notice_changes VALUES('object',OLD.bucket_id,OLD.object_key);
END;

CREATE TRIGGER streams_notice_update AFTER UPDATE ON streams
WHEN NEW.stream_type='object' AND NEW.state='ready' AND (OLD.public_read IS NOT NEW.public_read OR OLD.etag IS NOT NEW.etag OR OLD.size IS NOT NEW.size OR OLD.metadata IS NOT NEW.metadata OR OLD.payload_revision IS NOT NEW.payload_revision) AND EXISTS(SELECT 1 FROM objects o WHERE o.bucket_id=NEW.bucket_id AND o.object_key=NEW.object_key AND o.stream_id=NEW.id)
BEGIN
    INSERT INTO notice_changes VALUES('object',NEW.bucket_id,NEW.object_key);
END;

CREATE TRIGGER buckets_notice_update AFTER UPDATE ON buckets
BEGIN
    INSERT INTO notice_changes VALUES('bucket',NEW.id,X'');
END;

CREATE TRIGGER buckets_notice_delete AFTER DELETE ON buckets
BEGIN
    INSERT INTO notice_changes VALUES('bucket',OLD.id,X'');
END;

CREATE TRIGGER bucket_domains_notice_insert AFTER INSERT ON bucket_domains
BEGIN
    INSERT INTO notice_changes VALUES('bucket',NEW.bucket_id,X'');
END;

CREATE TRIGGER bucket_domains_notice_delete AFTER DELETE ON bucket_domains
BEGIN
    INSERT INTO notice_changes VALUES('bucket',OLD.bucket_id,X'');
END;

CREATE TRIGGER bucket_domains_notice_update AFTER UPDATE ON bucket_domains
BEGIN
    INSERT INTO notice_changes VALUES('bucket',OLD.bucket_id,X'');
END;

CREATE TRIGGER bucket_domains_notice_update_destination AFTER UPDATE ON bucket_domains
WHEN OLD.bucket_id IS NOT NEW.bucket_id
BEGIN
    INSERT INTO notice_changes VALUES('bucket',NEW.bucket_id,X'');
END;

CREATE TRIGGER storages_notice_update AFTER UPDATE ON storages
BEGIN
    INSERT INTO notice_changes VALUES('storage',NEW.id,X'');
END;

CREATE TRIGGER storages_notice_delete AFTER DELETE ON storages
BEGIN
    INSERT INTO notice_changes VALUES('storage',OLD.id,X'');
END;

CREATE TRIGGER storage_profiles_notice_update AFTER UPDATE ON storage_profiles
BEGIN
    INSERT INTO notice_changes VALUES('storage',NEW.storage_id,X'');
END;

CREATE TRIGGER storage_profiles_notice_delete AFTER DELETE ON storage_profiles
BEGIN
    INSERT INTO notice_changes VALUES('storage',OLD.storage_id,X'');
END;

CREATE TRIGGER chunk_encodings_notice_update AFTER UPDATE ON chunk_encodings
WHEN (OLD.quarantine_code IS NOT NEW.quarantine_code OR OLD.quarantine_revision IS NOT NEW.quarantine_revision)
BEGIN
    INSERT INTO notice_changes VALUES('storage',NEW.storage_id,X'');
END;

CREATE TRIGGER packs_notice_update AFTER UPDATE ON packs
WHEN (OLD.quarantine_code IS NOT NEW.quarantine_code OR OLD.quarantine_revision IS NOT NEW.quarantine_revision)
BEGIN
    INSERT INTO notice_changes VALUES('storage',NEW.storage_id,X'');
END;

CREATE TRIGGER raw_sources_notice_update AFTER UPDATE ON raw_sources
WHEN OLD.quarantine_code IS NOT NEW.quarantine_code
BEGIN
    INSERT INTO notice_changes VALUES('storage',NEW.storage_id,X'');
END;

CREATE TRIGGER tier_locations_notice_insert AFTER INSERT ON tier_locations
BEGIN
    INSERT INTO notice_changes VALUES('storage',NEW.home_storage,X'');
END;

CREATE TRIGGER tier_locations_notice_update AFTER UPDATE ON tier_locations
WHEN OLD.generation IS NOT NEW.generation
BEGIN
    INSERT INTO notice_changes VALUES('storage',NEW.home_storage,X'');
END;

CREATE TRIGGER tier_locations_notice_delete AFTER DELETE ON tier_locations
BEGIN
    INSERT INTO notice_changes VALUES('storage',OLD.home_storage,X'');
END;

UPDATE mokyu_meta SET schema_version = 20261102201213;
