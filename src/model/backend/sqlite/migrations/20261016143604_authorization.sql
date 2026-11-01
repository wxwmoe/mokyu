CREATE TABLE IF NOT EXISTS projects (
    id BLOB CHECK (id IS NULL OR length(id)=16) NOT NULL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    description TEXT NOT NULL DEFAULT '',
    builtin INTEGER CHECK (builtin IN (0,1)) NOT NULL DEFAULT FALSE,
    allow_bucket_create INTEGER CHECK (allow_bucket_create IN (0,1)) NOT NULL DEFAULT FALSE,
    created_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec')*1000000 AS INTEGER)),
    CONSTRAINT projects_name CHECK (length(CAST(name AS BLOB)) BETWEEN 1 AND 128),
    CONSTRAINT projects_description CHECK (length(CAST(description AS BLOB))<=2000)
) STRICT, WITHOUT ROWID;

INSERT INTO projects(id,name,builtin) VALUES(X'00000000000000000000000000000001','Default',TRUE)
ON CONFLICT(id) DO NOTHING;

CREATE TABLE IF NOT EXISTS buckets (
    id BLOB CHECK (id IS NULL OR length(id)=16) NOT NULL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    state TEXT NOT NULL DEFAULT 'active',
    cors TEXT CHECK (json_valid(cors)) NOT NULL DEFAULT ('[]'),
    website_enabled INTEGER CHECK (website_enabled IN (0,1)) NOT NULL DEFAULT FALSE,
    index_document TEXT NOT NULL DEFAULT ('index.html'),
    error_document TEXT NOT NULL DEFAULT ('404.html'),
    created_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec')*1000000 AS INTEGER)),
    project_id BLOB CHECK (project_id IS NULL OR length(project_id)=16) NOT NULL,
    settings_revision INTEGER NOT NULL DEFAULT 0,
    uploads_paused INTEGER CHECK (uploads_paused IN (0,1)) NOT NULL DEFAULT FALSE,
    public_base_url TEXT NOT NULL DEFAULT (''),
    storage_id BLOB CHECK (storage_id IS NULL OR length(storage_id)=16) NOT NULL,
    UNIQUE (id,project_id),
    CONSTRAINT buckets_state CHECK (state IN ('active','purging')),
    CONSTRAINT buckets_project FOREIGN KEY (project_id) REFERENCES projects(id),
    CONSTRAINT buckets_storage FOREIGN KEY (storage_id) REFERENCES storages(id)
) STRICT, WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS project_members (
    user_id BLOB CHECK (user_id IS NULL OR length(user_id)=16) NOT NULL,
    project_id BLOB CHECK (project_id IS NULL OR length(project_id)=16) NOT NULL,
    role TEXT NOT NULL,
    scope TEXT NOT NULL,
    PRIMARY KEY (user_id,project_id),
    CONSTRAINT project_members_user FOREIGN KEY (user_id) REFERENCES web_users(id) ON DELETE CASCADE,
    CONSTRAINT project_members_project FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE CASCADE,
    CONSTRAINT project_members_role CHECK (role IN ('reader','writer','maintainer')),
    CONSTRAINT project_members_scope CHECK (scope IN ('all','selected'))
) STRICT, WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS member_bucket_grants (
    user_id BLOB CHECK (user_id IS NULL OR length(user_id)=16) NOT NULL,
    project_id BLOB CHECK (project_id IS NULL OR length(project_id)=16) NOT NULL,
    bucket_id BLOB CHECK (bucket_id IS NULL OR length(bucket_id)=16) NOT NULL,
    actions INTEGER NOT NULL,
    PRIMARY KEY (user_id,bucket_id),
    CONSTRAINT member_grants_actions CHECK (actions BETWEEN 0 AND 127),
    CONSTRAINT member_grants_member FOREIGN KEY (user_id,project_id) REFERENCES project_members(user_id,project_id) ON DELETE CASCADE,
    CONSTRAINT member_grants_bucket FOREIGN KEY (bucket_id,project_id) REFERENCES buckets(id,project_id) ON DELETE CASCADE
) STRICT, WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS s3_credentials (
    access_key_id TEXT NOT NULL PRIMARY KEY,
    secret_encrypted BLOB NOT NULL,
    enabled INTEGER CHECK (enabled IN (0,1)) NOT NULL DEFAULT TRUE,
    created_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec')*1000000 AS INTEGER)),
    project_id BLOB CHECK (project_id IS NULL OR length(project_id)=16) NOT NULL,
    authorization_revision INTEGER NOT NULL DEFAULT 0,
    label TEXT NOT NULL DEFAULT '',
    expires_at INTEGER,
    last_used_at INTEGER,
    created_by BLOB CHECK (created_by IS NULL OR length(created_by)=16),
    CONSTRAINT s3_credentials_label CHECK (length(CAST(label AS BLOB))<=128),
    CONSTRAINT s3_credentials_project FOREIGN KEY (project_id) REFERENCES projects(id),
    CONSTRAINT s3_credentials_creator FOREIGN KEY (created_by) REFERENCES web_users(id) ON DELETE SET NULL
) STRICT, WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS s3_credential_grants (
    access_key TEXT NOT NULL,
    bucket_id BLOB CHECK (bucket_id IS NULL OR length(bucket_id)=16) NOT NULL,
    actions INTEGER NOT NULL,
    PRIMARY KEY (access_key,bucket_id),
    CONSTRAINT credential_grants_actions CHECK (actions BETWEEN 0 AND 127),
    CONSTRAINT credential_grants_credential FOREIGN KEY (access_key) REFERENCES s3_credentials(access_key_id) ON DELETE CASCADE,
    CONSTRAINT credential_grants_bucket FOREIGN KEY (bucket_id) REFERENCES buckets(id) ON DELETE CASCADE
) STRICT, WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS token_bucket_grants (
    token_id BLOB CHECK (token_id IS NULL OR length(token_id)=16) NOT NULL,
    bucket_id BLOB CHECK (bucket_id IS NULL OR length(bucket_id)=16) NOT NULL,
    actions INTEGER NOT NULL,
    PRIMARY KEY (token_id,bucket_id),
    CONSTRAINT token_grants_actions CHECK (actions BETWEEN 0 AND 127),
    CONSTRAINT token_grants_token FOREIGN KEY (token_id) REFERENCES api_tokens(id) ON DELETE CASCADE,
    CONSTRAINT token_grants_bucket FOREIGN KEY (bucket_id) REFERENCES buckets(id) ON DELETE CASCADE
) STRICT, WITHOUT ROWID;

CREATE VIEW user_bucket_access AS
SELECT m.user_id,b.id AS bucket_id,
    (CASE m.role WHEN 'reader' THEN 67 WHEN 'writer' THEN 95 WHEN 'maintainer' THEN 127 ELSE 0 END)
    & IIF(m.scope='all',127,COALESCE(g.actions,0)) AS actions
FROM project_members m JOIN buckets b ON b.project_id=m.project_id
LEFT JOIN member_bucket_grants g ON g.user_id=m.user_id AND g.bucket_id=b.id;

CREATE VIEW token_bucket_access AS
SELECT t.id AS token_id,g.bucket_id,
    g.actions & IIF(u.role='admin',127,COALESCE(a.actions,0)) AS actions
FROM api_tokens t JOIN web_users u ON u.id=t.user_id
JOIN token_bucket_grants g ON g.token_id=t.id
LEFT JOIN user_bucket_access a ON a.user_id=t.user_id AND a.bucket_id=g.bucket_id;

CREATE INDEX buckets_project ON buckets(project_id);
CREATE INDEX buckets_storage ON buckets(storage_id);
CREATE INDEX project_members_project ON project_members(project_id);
CREATE INDEX member_grants_member ON member_bucket_grants(user_id,project_id);
CREATE INDEX member_grants_bucket ON member_bucket_grants(bucket_id,project_id);
CREATE INDEX s3_credentials_project ON s3_credentials(project_id);
CREATE INDEX s3_credentials_creator ON s3_credentials(created_by);
CREATE INDEX credential_grants_bucket ON s3_credential_grants(bucket_id);
CREATE INDEX token_grants_bucket ON token_bucket_grants(bucket_id);

UPDATE mokyu_meta SET schema_version=4 WHERE id=1;
