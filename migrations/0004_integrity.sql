ALTER TABLE tasks DROP CONSTRAINT tasks_kind_check;
ALTER TABLE tasks ADD CONSTRAINT tasks_kind_check CHECK (kind IN ('purge','sweep','integrity'));
CREATE INDEX tasks_work ON tasks(updated_at,id) WHERE state IN ('queued','running');

CREATE TABLE integrity_issues (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    task_id uuid NOT NULL REFERENCES tasks ON DELETE CASCADE,
    subject text NOT NULL,
    code text NOT NULL,
    chunk_id bigint,
    storage_id uuid,
    stream_id uuid,
    bucket_id uuid,
    object_key text,
    detail jsonb NOT NULL DEFAULT '{}',
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE(task_id,subject,code)
);
-- Subject identities are diagnostic snapshots and survive object/chunk deletion.
CREATE INDEX integrity_issues_page ON integrity_issues(task_id,id);
UPDATE gateway_meta SET schema_version=4;
