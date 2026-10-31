CREATE TABLE bucket_change_gates (
    bucket_id uuid PRIMARY KEY REFERENCES buckets(id) ON DELETE CASCADE,
    subscribers integer NOT NULL DEFAULT 0 CHECK (subscribers >= 0)
);
INSERT INTO bucket_change_gates(bucket_id) SELECT id FROM buckets;

CREATE TABLE transfer_subscriptions (
    task_id uuid PRIMARY KEY REFERENCES transfer_runs(task_id) ON DELETE CASCADE,
    bucket_id uuid NOT NULL REFERENCES buckets(id),
    scope jsonb NOT NULL,
    active boolean NOT NULL DEFAULT true
);
CREATE INDEX transfer_subscriptions_bucket ON transfer_subscriptions(bucket_id) WHERE active;

CREATE TABLE transfer_changes (
    task_id uuid NOT NULL REFERENCES transfer_subscriptions(task_id) ON DELETE CASCADE,
    object_key text COLLATE "C" NOT NULL CHECK (octet_length(object_key) BETWEEN 1 AND 1024),
    source_version uuid REFERENCES streams(id),
    snapshot jsonb,
    revision bigint NOT NULL DEFAULT 1 CHECK (revision > 0),
    PRIMARY KEY (task_id,object_key),
    CHECK ((source_version IS NULL) = (snapshot IS NULL))
);
CREATE INDEX transfer_changes_source ON transfer_changes(source_version);

ALTER TABLE transfer_runs ADD COLUMN scanned boolean NOT NULL DEFAULT false;
ALTER TABLE transfer_runs ADD COLUMN captured boolean NOT NULL DEFAULT false;
ALTER TABLE transfer_runs ADD COLUMN sealed boolean NOT NULL DEFAULT false;
ALTER TABLE transfer_runs ADD COLUMN scan_end text;
UPDATE transfer_runs SET scanned=true,sealed=true WHERE phase<>'scan';
ALTER TABLE transfer_items ADD COLUMN confirmation jsonb;

CREATE FUNCTION mokyu_change_gate() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO bucket_change_gates(bucket_id) VALUES(NEW.id);
    RETURN NULL;
END
$$;
CREATE TRIGGER buckets_change_gate AFTER INSERT ON buckets
    FOR EACH ROW EXECUTE FUNCTION mokyu_change_gate();

CREATE FUNCTION mokyu_capture_change(bucket uuid, object_name text, version uuid)
RETURNS void LANGUAGE plpgsql AS $$
DECLARE
    readers integer;
    value jsonb;
BEGIN
    -- A shared gate covers the commit; activation and sealing hold it exclusively.
    SELECT subscribers INTO readers FROM bucket_change_gates WHERE bucket_id=bucket FOR SHARE;
    IF readers IS NULL OR readers=0 THEN
        RETURN;
    END IF;
    IF version IS NOT NULL THEN
        SELECT jsonb_build_object('metadata',metadata,'checksums',checksums,'public',public_read,
            'size',size,'etag',etag,'payload',payload_type) INTO STRICT value FROM streams WHERE id=version;
    END IF;
    INSERT INTO transfer_changes(task_id,object_key,source_version,snapshot)
        SELECT task_id,object_name,version,value FROM transfer_subscriptions
        WHERE bucket_id=bucket AND active AND (
            scope->>'selection'='all'
            OR scope->>'selection'='prefix' AND left(object_name,char_length(scope->>'prefix'))=scope->>'prefix'
            OR scope->>'selection'='objects' AND scope->'keys' ? object_name)
        ON CONFLICT (task_id,object_key) DO UPDATE SET
            source_version=EXCLUDED.source_version,snapshot=EXCLUDED.snapshot,
            revision=transfer_changes.revision+1;
END
$$;

CREATE FUNCTION mokyu_capture_object() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    bucket uuid;
    object_name text;
    version uuid;
BEGIN
    IF TG_OP='DELETE' THEN
        IF OLD.stream_id IS NULL THEN RETURN NULL; END IF;
        bucket:=OLD.bucket_id;
        object_name:=OLD.object_key;
    ELSE
        IF TG_OP='INSERT' AND NEW.stream_id IS NULL THEN RETURN NULL; END IF;
        IF TG_OP='UPDATE' AND OLD.stream_id IS NOT DISTINCT FROM NEW.stream_id THEN RETURN NULL; END IF;
        bucket:=NEW.bucket_id;
        object_name:=NEW.object_key;
        version:=NEW.stream_id;
    END IF;
    PERFORM mokyu_capture_change(bucket,object_name,version);
    RETURN NULL;
END
$$;
CREATE TRIGGER objects_capture AFTER INSERT OR UPDATE OF stream_id OR DELETE ON objects
    FOR EACH ROW EXECUTE FUNCTION mokyu_capture_object();

CREATE FUNCTION mokyu_capture_stream() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF EXISTS (SELECT 1 FROM objects WHERE bucket_id=NEW.bucket_id AND object_key=NEW.object_key AND stream_id=NEW.id) THEN
        PERFORM mokyu_capture_change(NEW.bucket_id,NEW.object_key,NEW.id);
    END IF;
    RETURN NULL;
END
$$;
CREATE TRIGGER streams_capture AFTER UPDATE OF public_read,metadata,checksums,etag,size,payload_type ON streams
    FOR EACH ROW WHEN (NEW.state='ready' AND NEW.stream_type='object'
        AND (OLD.public_read IS DISTINCT FROM NEW.public_read OR OLD.metadata IS DISTINCT FROM NEW.metadata
            OR OLD.checksums IS DISTINCT FROM NEW.checksums OR OLD.etag IS DISTINCT FROM NEW.etag
            OR OLD.size IS DISTINCT FROM NEW.size OR OLD.payload_type IS DISTINCT FROM NEW.payload_type))
    EXECUTE FUNCTION mokyu_capture_stream();

UPDATE mokyu_meta SET schema_version=97 WHERE singleton;
