CREATE TABLE node_notices (
    node_id uuid NOT NULL,
    slot SMALLINT NOT NULL CHECK (slot BETWEEN 0 AND 1023),
    generation uuid NOT NULL,
    event_id uuid NOT NULL UNIQUE,
    scope TEXT NOT NULL CHECK (scope IN ('object','bucket','storage','reset')),
    resource_id uuid NOT NULL,
    object_key text NOT NULL,
    retry_at timestamptz,
    PRIMARY KEY (node_id,slot),
    FOREIGN KEY (node_id) REFERENCES notification_consumers(node_id) ON DELETE CASCADE
);

CREATE OR REPLACE FUNCTION mokyu_notify_change(change_scope text, resource uuid, object_name text DEFAULT '') RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    event uuid;
BEGIN
    IF (SELECT deployment_mode FROM mokyu_meta WHERE singleton)='single_control' THEN
        PERFORM pg_advisory_xact_lock_shared(734922709851088);
        INSERT INTO node_notices(node_id,slot,generation,event_id,scope,resource_id,object_key)
    SELECT c.node_id,(hashtextextended(resource::text || object_name,0) & 1023)::smallint,c.generation,gen_random_uuid(),change_scope,resource,object_name
    FROM notification_consumers c WHERE c.expires_at>clock_timestamp() AND (change_scope='storage' OR c.role<>'worker')
    ON CONFLICT (node_id,slot) DO UPDATE SET scope=CASE WHEN node_notices.generation<>excluded.generation THEN excluded.scope
        WHEN node_notices.scope=excluded.scope AND node_notices.resource_id=excluded.resource_id AND node_notices.object_key=excluded.object_key THEN excluded.scope
        WHEN node_notices.scope IN ('object','bucket') AND excluded.scope IN ('object','bucket') AND node_notices.resource_id=excluded.resource_id THEN 'bucket'
        ELSE 'reset' END,
        generation=excluded.generation,event_id=excluded.event_id,resource_id=excluded.resource_id,object_key=excluded.object_key,retry_at=NULL;
        RETURN;
    END IF;
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

CREATE FUNCTION mokyu_notify_tier() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP='DELETE' THEN
        PERFORM mokyu_notify_change('storage',OLD.home_storage);
    ELSIF TG_OP='INSERT' OR OLD.generation IS DISTINCT FROM NEW.generation THEN
        PERFORM mokyu_notify_change('storage',NEW.home_storage);
    END IF;
    RETURN NULL;
END
$$;
CREATE TRIGGER tier_location_notice AFTER INSERT OR UPDATE OF generation OR DELETE ON tier_locations
    FOR EACH ROW EXECUTE FUNCTION mokyu_notify_tier();

UPDATE mokyu_meta SET schema_version = 20261102201213;
