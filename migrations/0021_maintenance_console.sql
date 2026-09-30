CREATE TABLE maintenance_controls (
    kind text PRIMARY KEY CHECK (kind IN ('pack','reuse','reclaim','repack','range','gc','cleanup')),
    paused boolean NOT NULL DEFAULT false,
    last_scheduled_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);
INSERT INTO maintenance_controls(kind) VALUES ('pack'),('reuse'),('reclaim'),('repack'),('range'),('gc'),('cleanup');
UPDATE maintenance_controls SET paused=(SELECT gc_paused FROM mokyu_meta) WHERE kind='gc';
ALTER TABLE mokyu_meta ADD COLUMN pack_creation_paused boolean NOT NULL DEFAULT false;
ALTER TABLE tasks DROP CONSTRAINT tasks_kind_check;
ALTER TABLE tasks ADD CONSTRAINT tasks_kind_check CHECK(kind IN ('purge','sweep','integrity','pack','unpack','upload','cache_flush','gc','cleanup'));
ALTER TABLE tasks ADD COLUMN created_by text NOT NULL DEFAULT 'Unknown';
ALTER TABLE tasks ADD COLUMN source text NOT NULL DEFAULT 'unknown';
ALTER TABLE tasks ADD COLUMN started_at timestamptz;
CREATE INDEX tasks_kind_recent ON tasks((COALESCE(detail->>'kind',kind)),created_at DESC,id DESC);

CREATE TABLE maintenance_previews (
    id uuid PRIMARY KEY,
    actor_id uuid NOT NULL REFERENCES web_users ON DELETE CASCADE,
    token_id uuid REFERENCES api_tokens ON DELETE CASCADE,
    action text NOT NULL CHECK(action IN ('unpack','sweep')),
    parameters jsonb NOT NULL,
    fingerprint text NOT NULL,
    expires_at timestamptz NOT NULL DEFAULT now()+interval '10 minutes',
    task_id uuid REFERENCES tasks ON DELETE SET NULL
);
CREATE INDEX maintenance_previews_expiry ON maintenance_previews(expires_at);
UPDATE mokyu_meta SET schema_version=21;
