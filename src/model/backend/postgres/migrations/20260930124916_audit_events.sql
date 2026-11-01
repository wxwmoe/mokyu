CREATE TABLE audit_events (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    created_at timestamptz NOT NULL DEFAULT now(),
    finished_at timestamptz,
    actor_id uuid,
    actor_label text NOT NULL,
    token_id uuid,
    source text NOT NULL CHECK (source IN ('web','token','cli')),
    action text NOT NULL,
    target text NOT NULL DEFAULT '',
    project_id uuid,
    bucket_id uuid,
    outcome text NOT NULL DEFAULT 'unknown' CHECK (outcome IN ('unknown','succeeded','failed','partial')),
    request_id text,
    status integer,
    detail jsonb NOT NULL DEFAULT '{}'
);
CREATE INDEX audit_events_created ON audit_events(created_at,id);
CREATE INDEX audit_events_actor ON audit_events(actor_id,id);
CREATE INDEX audit_events_project ON audit_events(project_id,id);
CREATE INDEX audit_events_bucket ON audit_events(bucket_id,id);
CREATE INDEX audit_events_request ON audit_events(request_id) WHERE request_id IS NOT NULL;
UPDATE mokyu_meta SET schema_version=14;
