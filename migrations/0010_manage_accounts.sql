ALTER TABLE web_users
    ADD COLUMN display_name text NOT NULL DEFAULT '' CHECK (octet_length(display_name) <= 240),
    ADD COLUMN locale text CHECK (locale IN ('en','zh-CN','ja')),
    ADD COLUMN theme text CHECK (theme IN ('auto','light','dark')),
    ADD COLUMN avatar_email text NOT NULL DEFAULT '' CHECK (octet_length(avatar_email) <= 320),
    ADD COLUMN avatar_enabled boolean NOT NULL DEFAULT false,
    ADD COLUMN auth_revision bigint NOT NULL DEFAULT 0;

ALTER TABLE sessions
    ADD COLUMN id uuid NOT NULL DEFAULT gen_random_uuid() UNIQUE,
    ADD COLUMN created_at timestamptz NOT NULL DEFAULT now(),
    ADD COLUMN last_seen_at timestamptz NOT NULL DEFAULT now(),
    ADD COLUMN reauthenticated_at timestamptz NOT NULL DEFAULT now(),
    ADD COLUMN auth_revision bigint NOT NULL DEFAULT 0,
    ADD COLUMN user_agent text NOT NULL DEFAULT '' CHECK (octet_length(user_agent) <= 512);
CREATE INDEX sessions_user ON sessions(user_id,created_at DESC,id);

CREATE TABLE manage_setup (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    token_hash bytea NOT NULL CHECK (octet_length(token_hash) = 32),
    created_at timestamptz NOT NULL DEFAULT now()
);

UPDATE mokyu_meta SET schema_version=10;
