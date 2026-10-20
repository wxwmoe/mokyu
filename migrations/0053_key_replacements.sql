CREATE TABLE key_replacements (
    id uuid PRIMARY KEY,
    actor text NOT NULL,
    plan jsonb NOT NULL,
    result jsonb,
    expires_at timestamptz NOT NULL
);
CREATE INDEX key_replacements_expiry ON key_replacements(expires_at);
UPDATE mokyu_meta SET schema_version=53;
