CREATE TABLE projects (
    id uuid PRIMARY KEY,
    name text NOT NULL UNIQUE CHECK (octet_length(name) BETWEEN 1 AND 128),
    description text NOT NULL DEFAULT '' CHECK (octet_length(description) <= 2000),
    builtin boolean NOT NULL DEFAULT false,
    allow_bucket_create boolean NOT NULL DEFAULT false,
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX projects_builtin ON projects(builtin) WHERE builtin;
INSERT INTO projects(id,name,builtin) VALUES('00000000-0000-0000-0000-000000000001','Default',true);
ALTER TABLE mokyu_meta ADD COLUMN project_management boolean NOT NULL DEFAULT false;
ALTER TABLE buckets ADD COLUMN project_id uuid NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001' REFERENCES projects;
CREATE INDEX buckets_project ON buckets(project_id,name);
ALTER TABLE buckets ADD UNIQUE(id,project_id);

ALTER TABLE web_users
    ADD COLUMN role text NOT NULL DEFAULT 'admin' CHECK (role IN ('admin','member')),
    ADD COLUMN authorization_revision bigint NOT NULL DEFAULT 0;
ALTER TABLE web_users ALTER COLUMN role SET DEFAULT 'member';
CREATE TABLE project_members (
    user_id uuid NOT NULL REFERENCES web_users ON DELETE CASCADE,
    project_id uuid NOT NULL REFERENCES projects ON DELETE CASCADE,
    role text NOT NULL CHECK (role IN ('reader','writer','maintainer')),
    scope text NOT NULL CHECK (scope IN ('all','selected')),
    PRIMARY KEY(user_id,project_id)
);
CREATE INDEX project_members_project ON project_members(project_id,user_id);
CREATE TABLE member_grants (
    user_id uuid NOT NULL,
    project_id uuid NOT NULL,
    bucket_id uuid NOT NULL,
    actions text[] NOT NULL CHECK (actions <@ ARRAY['bucket.list','object.read','object.write','object.delete','object.acl','bucket.settings','storage.inspect']::text[]),
    PRIMARY KEY(user_id,bucket_id),
    FOREIGN KEY(user_id,project_id) REFERENCES project_members ON DELETE CASCADE,
    FOREIGN KEY(bucket_id,project_id) REFERENCES buckets(id,project_id) ON DELETE CASCADE
);
CREATE INDEX member_grants_bucket ON member_grants(bucket_id,user_id);
CREATE VIEW user_bucket_access AS
SELECT m.user_id,b.id AS bucket_id,
    ARRAY(SELECT action FROM unnest(CASE m.role
        WHEN 'reader' THEN ARRAY['bucket.list','object.read','storage.inspect']
        WHEN 'writer' THEN ARRAY['bucket.list','object.read','object.write','object.delete','object.acl','storage.inspect']
        ELSE ARRAY['bucket.list','object.read','object.write','object.delete','object.acl','bucket.settings','storage.inspect'] END) AS action
        WHERE m.scope='all' OR action=ANY(COALESCE(g.actions,ARRAY[]::text[]))) AS actions
FROM project_members m JOIN buckets b ON b.project_id=m.project_id
LEFT JOIN member_grants g ON g.user_id=m.user_id AND g.bucket_id=b.id;
ALTER TABLE credentials
    ADD COLUMN project_id uuid NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001' REFERENCES projects,
    ADD COLUMN authorization_revision bigint NOT NULL DEFAULT 0;
CREATE INDEX credentials_project ON credentials(project_id);
ALTER TABLE grants ADD COLUMN actions text[];
UPDATE grants SET actions=CASE WHEN writable THEN ARRAY['bucket.list','object.read','object.write','object.delete','object.acl','storage.inspect'] ELSE ARRAY['bucket.list','object.read','storage.inspect'] END;
ALTER TABLE grants ALTER COLUMN actions SET NOT NULL;
ALTER TABLE grants ADD CHECK (actions <@ ARRAY['bucket.list','object.read','object.write','object.delete','object.acl','bucket.settings','storage.inspect']::text[]);
ALTER TABLE grants DROP COLUMN writable;

CREATE FUNCTION invalidate_member_authorization() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    UPDATE web_users SET authorization_revision=authorization_revision+1 WHERE id=COALESCE(NEW.user_id,OLD.user_id);
    RETURN NULL;
END $$;
CREATE TRIGGER member_authorization AFTER INSERT OR UPDATE OR DELETE ON project_members FOR EACH ROW EXECUTE FUNCTION invalidate_member_authorization();
CREATE TRIGGER grant_authorization AFTER INSERT OR UPDATE OR DELETE ON member_grants FOR EACH ROW EXECUTE FUNCTION invalidate_member_authorization();
CREATE FUNCTION invalidate_service_authorization() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    UPDATE credentials SET authorization_revision=authorization_revision+1 WHERE access_key=COALESCE(NEW.access_key,OLD.access_key);
    RETURN NULL;
END $$;
CREATE TRIGGER service_grant_authorization AFTER INSERT OR UPDATE OR DELETE ON grants FOR EACH ROW EXECUTE FUNCTION invalidate_service_authorization();
CREATE FUNCTION advance_authorization_revision() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    NEW.authorization_revision=OLD.authorization_revision+1;
    RETURN NEW;
END $$;
CREATE TRIGGER user_authorization BEFORE UPDATE OF role,enabled,password_hash ON web_users FOR EACH ROW EXECUTE FUNCTION advance_authorization_revision();
CREATE TRIGGER credential_authorization BEFORE UPDATE OF enabled,secret_encrypted,project_id ON credentials FOR EACH ROW EXECUTE FUNCTION advance_authorization_revision();

ALTER TABLE streams ADD COLUMN write_authorization jsonb NOT NULL DEFAULT '{"principal":{"kind":"local"},"resources":{}}';
UPDATE mokyu_meta SET schema_version=11;
