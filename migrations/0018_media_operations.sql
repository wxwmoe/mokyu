CREATE TABLE media_operations (
    id uuid PRIMARY KEY,
    user_id uuid NOT NULL REFERENCES web_users ON DELETE CASCADE,
    token_id uuid,
    client_id uuid NOT NULL,
    request_hash bytea NOT NULL CHECK (octet_length(request_hash)=32),
    result jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE NULLS NOT DISTINCT (user_id,token_id,client_id)
);
CREATE INDEX media_operations_cleanup ON media_operations(created_at,id);
UPDATE mokyu_meta SET schema_version=18;
