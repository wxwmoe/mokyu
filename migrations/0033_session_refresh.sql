-- Rolling browser refresh credentials are stored only as hashes.
CREATE TABLE session_refresh_tokens (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    session_id uuid NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
    family_id uuid NOT NULL,
    token_hash bytea NOT NULL UNIQUE CHECK (octet_length(token_hash) = 32),
    created_at timestamptz NOT NULL DEFAULT now(),
    last_used_at timestamptz NOT NULL DEFAULT now(),
    expires_at timestamptz NOT NULL,
    revoked_at timestamptz,
    replaced_by uuid REFERENCES session_refresh_tokens(id) ON DELETE SET NULL
);
CREATE UNIQUE INDEX session_refresh_active
    ON session_refresh_tokens(session_id)
    WHERE revoked_at IS NULL;
CREATE INDEX session_refresh_expiry
    ON session_refresh_tokens(expires_at)
    WHERE revoked_at IS NULL;

UPDATE mokyu_meta SET schema_version = 33;
