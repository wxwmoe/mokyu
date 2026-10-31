CREATE TABLE bucket_change_gates (
    bucket_id BLOB PRIMARY KEY REFERENCES buckets(id) ON DELETE CASCADE,
    subscribers INTEGER NOT NULL DEFAULT 0 CHECK (subscribers>=0)
);
INSERT INTO bucket_change_gates(bucket_id) SELECT id FROM buckets;

CREATE TABLE transfer_subscriptions (
    task_id BLOB PRIMARY KEY REFERENCES transfer_runs(task_id) ON DELETE CASCADE,
    bucket_id BLOB NOT NULL REFERENCES buckets(id),
    scope TEXT NOT NULL,
    active BOOLEAN NOT NULL DEFAULT TRUE
);
CREATE INDEX transfer_subscriptions_bucket ON transfer_subscriptions(bucket_id) WHERE active;

CREATE TABLE transfer_changes (
    task_id BLOB NOT NULL REFERENCES transfer_subscriptions(task_id) ON DELETE CASCADE,
    object_key TEXT NOT NULL CHECK (length(CAST(object_key AS BLOB)) BETWEEN 1 AND 1024),
    source_version INTEGER REFERENCES streams(id),
    snapshot TEXT,
    revision INTEGER NOT NULL DEFAULT 1 CHECK (revision>0),
    PRIMARY KEY (task_id,object_key),
    CHECK ((source_version IS NULL)=(snapshot IS NULL))
);
CREATE INDEX transfer_changes_source ON transfer_changes(source_version);

ALTER TABLE transfer_runs ADD COLUMN scanned BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE transfer_runs ADD COLUMN captured BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE transfer_runs ADD COLUMN sealed BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE transfer_runs ADD COLUMN scan_end TEXT;
UPDATE transfer_runs SET scanned=TRUE,sealed=TRUE WHERE phase<>'scan';
ALTER TABLE transfer_items ADD COLUMN confirmation TEXT;

CREATE TRIGGER buckets_change_gate AFTER INSERT ON buckets BEGIN
    INSERT INTO bucket_change_gates(bucket_id) VALUES(NEW.id);
END;

CREATE TRIGGER objects_capture_insert AFTER INSERT ON objects
WHEN NEW.stream_id IS NOT NULL AND EXISTS(SELECT 1 FROM bucket_change_gates WHERE bucket_id=NEW.bucket_id AND subscribers>0)
BEGIN
    INSERT INTO transfer_changes(task_id,object_key,source_version,snapshot)
        SELECT t.task_id,CAST(NEW.object_key AS TEXT),s.id,
            json_object('metadata',json(s.metadata),'checksums',json(s.checksums),'public',json(CASE WHEN s.public_read THEN 'true' ELSE 'false' END),
                'size',s.size,'etag',s.etag,'payload',s.payload_type)
        FROM transfer_subscriptions t JOIN streams s ON s.id=NEW.stream_id
        WHERE t.bucket_id=NEW.bucket_id AND t.active AND (
            t.scope->>'selection'='all'
            OR t.scope->>'selection'='prefix' AND substr(CAST(NEW.object_key AS TEXT),1,length(t.scope->>'prefix'))=t.scope->>'prefix'
            OR t.scope->>'selection'='objects' AND EXISTS(SELECT 1 FROM json_each(t.scope,'$.keys') WHERE value=CAST(NEW.object_key AS TEXT)))
        ON CONFLICT(task_id,object_key) DO UPDATE SET source_version=excluded.source_version,
            snapshot=excluded.snapshot,revision=transfer_changes.revision+1;
END;

CREATE TRIGGER objects_capture_update AFTER UPDATE OF stream_id ON objects
WHEN OLD.stream_id IS NOT NEW.stream_id AND EXISTS(SELECT 1 FROM bucket_change_gates WHERE bucket_id=NEW.bucket_id AND subscribers>0)
BEGIN
    INSERT INTO transfer_changes(task_id,object_key,source_version,snapshot)
        SELECT t.task_id,CAST(NEW.object_key AS TEXT),s.id,
            CASE WHEN s.id IS NOT NULL THEN json_object('metadata',json(s.metadata),'checksums',json(s.checksums),
                'public',json(CASE WHEN s.public_read THEN 'true' ELSE 'false' END),'size',s.size,'etag',s.etag,'payload',s.payload_type) END
        FROM transfer_subscriptions t LEFT JOIN streams s ON s.id=NEW.stream_id
        WHERE t.bucket_id=NEW.bucket_id AND t.active AND (
            t.scope->>'selection'='all'
            OR t.scope->>'selection'='prefix' AND substr(CAST(NEW.object_key AS TEXT),1,length(t.scope->>'prefix'))=t.scope->>'prefix'
            OR t.scope->>'selection'='objects' AND EXISTS(SELECT 1 FROM json_each(t.scope,'$.keys') WHERE value=CAST(NEW.object_key AS TEXT)))
        ON CONFLICT(task_id,object_key) DO UPDATE SET source_version=excluded.source_version,
            snapshot=excluded.snapshot,revision=transfer_changes.revision+1;
END;

CREATE TRIGGER objects_capture_delete AFTER DELETE ON objects
WHEN OLD.stream_id IS NOT NULL AND EXISTS(SELECT 1 FROM bucket_change_gates WHERE bucket_id=OLD.bucket_id AND subscribers>0)
BEGIN
    INSERT INTO transfer_changes(task_id,object_key,source_version,snapshot)
        SELECT task_id,CAST(OLD.object_key AS TEXT),NULL,NULL FROM transfer_subscriptions
        WHERE bucket_id=OLD.bucket_id AND active AND (
            scope->>'selection'='all'
            OR scope->>'selection'='prefix' AND substr(CAST(OLD.object_key AS TEXT),1,length(scope->>'prefix'))=scope->>'prefix'
            OR scope->>'selection'='objects' AND EXISTS(SELECT 1 FROM json_each(scope,'$.keys') WHERE value=CAST(OLD.object_key AS TEXT)))
        ON CONFLICT(task_id,object_key) DO UPDATE SET source_version=NULL,snapshot=NULL,revision=transfer_changes.revision+1;
END;

CREATE TRIGGER streams_capture AFTER UPDATE OF public_read,metadata,checksums,etag,size,payload_type ON streams
WHEN NEW.state='ready' AND NEW.stream_type='object'
    AND (OLD.public_read IS NOT NEW.public_read OR OLD.metadata IS NOT NEW.metadata OR OLD.checksums IS NOT NEW.checksums
        OR OLD.etag IS NOT NEW.etag OR OLD.size IS NOT NEW.size OR OLD.payload_type IS NOT NEW.payload_type)
    AND EXISTS(SELECT 1 FROM bucket_change_gates WHERE bucket_id=NEW.bucket_id AND subscribers>0)
    AND EXISTS(SELECT 1 FROM objects WHERE bucket_id=NEW.bucket_id AND object_key=NEW.object_key AND stream_id=NEW.id)
BEGIN
    INSERT INTO transfer_changes(task_id,object_key,source_version,snapshot)
        SELECT task_id,CAST(NEW.object_key AS TEXT),NEW.id,
            json_object('metadata',json(NEW.metadata),'checksums',json(NEW.checksums),'public',json(CASE WHEN NEW.public_read THEN 'true' ELSE 'false' END),
                'size',NEW.size,'etag',NEW.etag,'payload',NEW.payload_type)
        FROM transfer_subscriptions WHERE bucket_id=NEW.bucket_id AND active AND (
            scope->>'selection'='all'
            OR scope->>'selection'='prefix' AND substr(CAST(NEW.object_key AS TEXT),1,length(scope->>'prefix'))=scope->>'prefix'
            OR scope->>'selection'='objects' AND EXISTS(SELECT 1 FROM json_each(scope,'$.keys') WHERE value=CAST(NEW.object_key AS TEXT)))
        ON CONFLICT(task_id,object_key) DO UPDATE SET source_version=excluded.source_version,
            snapshot=excluded.snapshot,revision=transfer_changes.revision+1;
END;

UPDATE mokyu_meta SET schema_version=51 WHERE id=1;
