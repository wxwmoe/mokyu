CREATE TABLE external_buckets (
    id uuid PRIMARY KEY,
    project_id uuid NOT NULL REFERENCES projects(id),
    name text NOT NULL CHECK (octet_length(name) BETWEEN 1 AND 128),
    connection jsonb NOT NULL CHECK (jsonb_typeof(connection)='object'),
    revision bigint NOT NULL DEFAULT 1 CHECK (revision>0),
    access_encrypted bytea NOT NULL,
    secret_encrypted bytea NOT NULL,
    enabled boolean NOT NULL DEFAULT true,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (project_id,name),
    UNIQUE (id,project_id)
);

CREATE TABLE member_external_grants (
    user_id uuid NOT NULL,
    project_id uuid NOT NULL,
    bucket_id uuid NOT NULL,
    actions text[] NOT NULL CHECK (actions<@ARRAY['bucket.list','object.read','object.write','object.delete']::text[]),
    PRIMARY KEY (user_id,bucket_id),
    FOREIGN KEY (user_id,project_id) REFERENCES project_members(user_id,project_id) ON DELETE CASCADE,
    FOREIGN KEY (bucket_id,project_id) REFERENCES external_buckets(id,project_id) ON DELETE CASCADE
);
CREATE INDEX member_external_bucket ON member_external_grants(bucket_id);
CREATE INDEX member_external_membership ON member_external_grants(user_id,project_id);

CREATE TABLE token_external_grants (
    token_id uuid NOT NULL REFERENCES api_tokens(id) ON DELETE CASCADE,
    bucket_id uuid NOT NULL REFERENCES external_buckets(id) ON DELETE CASCADE,
    actions text[] NOT NULL CHECK (actions<@ARRAY['bucket.list','object.read','object.write','object.delete']::text[]),
    PRIMARY KEY (token_id,bucket_id)
);
CREATE INDEX token_external_bucket ON token_external_grants(bucket_id);

CREATE TABLE external_tests (
    id uuid PRIMARY KEY REFERENCES tasks(id) ON DELETE CASCADE,
    external_id uuid REFERENCES external_buckets(id) ON DELETE SET NULL,
    revision bigint NOT NULL CHECK (revision>0),
    connection_encrypted bytea,
    node_id uuid,
    attempts integer NOT NULL DEFAULT 0 CHECK (attempts>=0),
    report jsonb NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(report)='object'),
    cleanup_required boolean NOT NULL DEFAULT true,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX external_tests_bucket ON external_tests(external_id,created_at DESC,id DESC);

CREATE VIEW user_external_access AS
SELECT m.user_id,b.id AS bucket_id,
    ARRAY(SELECT action FROM unnest(CASE m.role
        WHEN 'reader' THEN ARRAY['bucket.list','object.read']
        ELSE ARRAY['bucket.list','object.read','object.write','object.delete'] END) AS action
        WHERE m.scope='all' OR action=ANY(COALESCE(g.actions,ARRAY[]::text[]))) AS actions
FROM project_members m JOIN external_buckets b ON b.project_id=m.project_id
LEFT JOIN member_external_grants g ON g.user_id=m.user_id AND g.bucket_id=b.id;

CREATE VIEW token_external_access AS
SELECT t.id AS token_id,g.bucket_id,
    ARRAY(SELECT action FROM unnest(g.actions) AS action WHERE u.role='admin' OR action=ANY(COALESCE(a.actions,ARRAY[]::text[]))) AS actions
FROM api_tokens t JOIN web_users u ON u.id=t.user_id JOIN token_external_grants g ON g.token_id=t.id
LEFT JOIN user_external_access a ON a.user_id=t.user_id AND a.bucket_id=g.bucket_id;

CREATE TRIGGER external_member_authorization AFTER INSERT OR UPDATE OR DELETE ON member_external_grants
    FOR EACH ROW EXECUTE FUNCTION invalidate_member_authorization();
CREATE TRIGGER external_token_authorization AFTER INSERT OR UPDATE OR DELETE ON token_external_grants
    FOR EACH ROW EXECUTE FUNCTION invalidate_token_authorization();

UPDATE mokyu_meta SET schema_version=89 WHERE singleton;
