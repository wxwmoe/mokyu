CREATE TABLE notification_consumers (
    node_id uuid PRIMARY KEY REFERENCES nodes(id) ON DELETE CASCADE,
    generation uuid NOT NULL,
    role text NOT NULL CHECK (role IN ('control','data','worker','node')),
    expires_at timestamptz NOT NULL
);

CREATE TABLE notifications (
    id uuid PRIMARY KEY DEFAULT uuidv7(),
    transaction_id xid8 NOT NULL DEFAULT pg_current_xact_id(),
    scope text NOT NULL CHECK (scope IN ('object','bucket','storage')),
    resource_id uuid NOT NULL,
    object_key text NOT NULL DEFAULT '' CHECK (octet_length(object_key) <= 1024),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK ((scope='object') = (object_key<>'')),
    UNIQUE (transaction_id,scope,resource_id,object_key)
);

CREATE TABLE notification_targets (
    notification_id uuid NOT NULL REFERENCES notifications(id) ON DELETE CASCADE,
    node_id uuid NOT NULL,
    generation uuid NOT NULL,
    claim_token uuid,
    claim_until timestamptz,
    attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    applied_at timestamptz,
    retired_at timestamptz,
    PRIMARY KEY (notification_id,node_id,generation),
    CHECK ((claim_token IS NULL) = (claim_until IS NULL)),
    CHECK (applied_at IS NULL OR retired_at IS NULL)
);

CREATE INDEX notification_targets_pending ON notification_targets (node_id,generation,notification_id)
    WHERE applied_at IS NULL AND retired_at IS NULL;
CREATE INDEX notifications_created ON notifications (created_at,id);

CREATE FUNCTION mokyu_notify_change(change_scope text, resource uuid, object_name text DEFAULT '') RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    event uuid;
BEGIN
    IF NOT EXISTS (SELECT 1 FROM mokyu_meta WHERE singleton AND deployment_mode='cluster') THEN
        RETURN;
    END IF;
    PERFORM pg_advisory_xact_lock_shared(734922709851088);
    IF NOT EXISTS (SELECT 1 FROM notification_consumers c JOIN nodes n ON n.id=c.node_id
        WHERE n.enabled AND c.expires_at>clock_timestamp()
        AND (change_scope='storage' OR c.role<>'worker')) THEN
        RETURN;
    END IF;
    INSERT INTO notifications(scope,resource_id,object_key) VALUES(change_scope,resource,object_name)
        ON CONFLICT (transaction_id,scope,resource_id,object_key) DO NOTHING RETURNING id INTO event;
    IF event IS NULL THEN
        RETURN;
    END IF;
    INSERT INTO notification_targets(notification_id,node_id,generation)
        SELECT event,c.node_id,c.generation FROM notification_consumers c JOIN nodes n ON n.id=c.node_id
        WHERE n.enabled AND c.expires_at>clock_timestamp()
        AND (change_scope='storage' OR c.role<>'worker');
    PERFORM pg_notify('mokyu_notifications','changed');
END
$$;

CREATE FUNCTION mokyu_notify_object() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP='DELETE' THEN
        IF OLD.stream_id IS NOT NULL THEN
            PERFORM mokyu_notify_change('object',OLD.bucket_id,OLD.object_key);
        END IF;
    ELSIF TG_OP='INSERT' THEN
        IF NEW.stream_id IS NOT NULL THEN
            PERFORM mokyu_notify_change('object',NEW.bucket_id,NEW.object_key);
        END IF;
    ELSIF OLD.stream_id IS DISTINCT FROM NEW.stream_id THEN
        PERFORM mokyu_notify_change('object',NEW.bucket_id,NEW.object_key);
    END IF;
    RETURN NULL;
END
$$;
CREATE TRIGGER objects_notify AFTER INSERT OR UPDATE OF stream_id OR DELETE ON objects
    FOR EACH ROW EXECUTE FUNCTION mokyu_notify_object();

CREATE FUNCTION mokyu_notify_stream() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.stream_type='object' AND NEW.state='ready'
        AND EXISTS (SELECT 1 FROM objects WHERE bucket_id=NEW.bucket_id AND object_key=NEW.object_key AND stream_id=NEW.id)
        AND (OLD.public_read,OLD.metadata,OLD.etag,OLD.size,OLD.payload_revision)
            IS DISTINCT FROM (NEW.public_read,NEW.metadata,NEW.etag,NEW.size,NEW.payload_revision) THEN
        PERFORM mokyu_notify_change('object',NEW.bucket_id,NEW.object_key);
    END IF;
    RETURN NULL;
END
$$;
CREATE TRIGGER streams_notify AFTER UPDATE OF public_read,metadata,etag,size,payload_revision ON streams
    FOR EACH ROW EXECUTE FUNCTION mokyu_notify_stream();

CREATE FUNCTION mokyu_notify_bucket() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP='DELETE' THEN
        PERFORM mokyu_notify_change('bucket',OLD.id);
    ELSIF OLD IS DISTINCT FROM NEW THEN
        PERFORM mokyu_notify_change('bucket',NEW.id);
    END IF;
    RETURN NULL;
END
$$;
CREATE TRIGGER buckets_notify AFTER UPDATE OR DELETE ON buckets
    FOR EACH ROW EXECUTE FUNCTION mokyu_notify_bucket();

CREATE FUNCTION mokyu_notify_domain() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP<>'INSERT' THEN
        PERFORM mokyu_notify_change('bucket',OLD.bucket_id);
    END IF;
    IF TG_OP<>'DELETE' THEN
        PERFORM mokyu_notify_change('bucket',NEW.bucket_id);
    END IF;
    RETURN NULL;
END
$$;
CREATE TRIGGER domains_notify AFTER INSERT OR UPDATE OR DELETE ON bucket_domains
    FOR EACH ROW EXECUTE FUNCTION mokyu_notify_domain();

CREATE FUNCTION mokyu_notify_storage() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    previous jsonb;
    current jsonb;
    resource uuid;
BEGIN
    previous:=to_jsonb(OLD);
    current:=to_jsonb(NEW);
    IF TG_OP='DELETE' OR previous IS DISTINCT FROM current THEN
        resource:=COALESCE((previous->>'storage_id')::uuid,(previous->>'id')::uuid);
        PERFORM mokyu_notify_change('storage',resource);
    END IF;
    RETURN NULL;
END
$$;
CREATE TRIGGER profiles_notify AFTER UPDATE OR DELETE ON storage_profiles
    FOR EACH ROW EXECUTE FUNCTION mokyu_notify_storage();
CREATE TRIGGER storages_notify AFTER UPDATE OR DELETE ON storages
    FOR EACH ROW EXECUTE FUNCTION mokyu_notify_storage();

CREATE FUNCTION mokyu_notify_health() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF (to_jsonb(OLD)->'quarantine_revision') IS DISTINCT FROM (to_jsonb(NEW)->'quarantine_revision')
        OR OLD.quarantine_code IS DISTINCT FROM NEW.quarantine_code THEN
        PERFORM mokyu_notify_change('storage',NEW.storage_id);
    END IF;
    RETURN NULL;
END
$$;
CREATE TRIGGER encodings_notify AFTER UPDATE OF quarantine_code,quarantine_revision ON chunk_encodings
    FOR EACH ROW EXECUTE FUNCTION mokyu_notify_health();
CREATE TRIGGER packs_notify AFTER UPDATE OF quarantine_code,quarantine_revision ON packs
    FOR EACH ROW EXECUTE FUNCTION mokyu_notify_health();
CREATE TRIGGER raw_notify AFTER UPDATE OF quarantine_code ON raw_sources
    FOR EACH ROW EXECUTE FUNCTION mokyu_notify_health();

UPDATE mokyu_meta SET schema_version=88 WHERE singleton;
