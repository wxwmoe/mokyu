ALTER TABLE credentials
    ADD COLUMN label text NOT NULL DEFAULT '' CHECK (octet_length(label)<=128),
    ADD COLUMN expires_at timestamptz,
    ADD COLUMN last_used_at timestamptz,
    ADD COLUMN created_by uuid REFERENCES web_users ON DELETE SET NULL;
CREATE TRIGGER credential_expiration BEFORE UPDATE OF expires_at ON credentials FOR EACH ROW EXECUTE FUNCTION advance_authorization_revision();

CREATE TABLE api_tokens (
    id uuid PRIMARY KEY,
    user_id uuid NOT NULL REFERENCES web_users ON DELETE CASCADE,
    label text NOT NULL CHECK (octet_length(label) BETWEEN 1 AND 128),
    token_hash bytea NOT NULL UNIQUE CHECK (octet_length(token_hash)=32),
    prefix text NOT NULL,
    system boolean NOT NULL DEFAULT false,
    auth_revision bigint NOT NULL,
    authorization_revision bigint NOT NULL DEFAULT 0,
    created_at timestamptz NOT NULL DEFAULT now(),
    expires_at timestamptz,
    last_used_at timestamptz,
    revoked_at timestamptz
);
CREATE INDEX api_tokens_user ON api_tokens(user_id,id);
CREATE TABLE token_grants (
    token_id uuid NOT NULL REFERENCES api_tokens ON DELETE CASCADE,
    bucket_id uuid NOT NULL REFERENCES buckets ON DELETE CASCADE,
    actions text[] NOT NULL CHECK (actions <@ ARRAY['bucket.list','object.read','object.write','object.delete','object.acl','bucket.settings','storage.inspect']::text[]),
    PRIMARY KEY(token_id,bucket_id)
);
CREATE INDEX token_grants_bucket ON token_grants(bucket_id,token_id);
CREATE VIEW token_bucket_access AS
SELECT t.id AS token_id,g.bucket_id,
    ARRAY(SELECT action FROM unnest(g.actions) AS action WHERE u.role='admin' OR action=ANY(COALESCE(a.actions,ARRAY[]::text[]))) AS actions
FROM api_tokens t JOIN web_users u ON u.id=t.user_id JOIN token_grants g ON g.token_id=t.id
LEFT JOIN user_bucket_access a ON a.user_id=t.user_id AND a.bucket_id=g.bucket_id;
CREATE TRIGGER token_authorization BEFORE UPDATE OF system,expires_at,revoked_at ON api_tokens FOR EACH ROW EXECUTE FUNCTION advance_authorization_revision();
CREATE FUNCTION invalidate_token_authorization() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    UPDATE api_tokens SET authorization_revision=authorization_revision+1 WHERE id=COALESCE(NEW.token_id,OLD.token_id);
    RETURN NULL;
END $$;
CREATE TRIGGER token_grant_authorization AFTER INSERT OR UPDATE OR DELETE ON token_grants FOR EACH ROW EXECUTE FUNCTION invalidate_token_authorization();
UPDATE mokyu_meta SET schema_version=13;
