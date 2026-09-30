CREATE TABLE storage_insights (
    id text PRIMARY KEY,
    bucket_ids uuid[],
    requested_at timestamptz NOT NULL DEFAULT now(),
    as_of timestamptz,
    data jsonb,
    CHECK ((id='global')=(bucket_ids IS NULL))
);
INSERT INTO storage_insights(id) VALUES('global');
CREATE TABLE storage_history (
    scope_id text NOT NULL REFERENCES storage_insights ON DELETE CASCADE,
    at timestamptz NOT NULL,
    data jsonb NOT NULL,
    PRIMARY KEY(scope_id,at)
);
CREATE TABLE runtime_history (
    at timestamptz PRIMARY KEY,
    data jsonb NOT NULL
);
CREATE INDEX storage_history_expiry ON storage_history(at);
UPDATE mokyu_meta SET schema_version=20;
