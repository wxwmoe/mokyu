CREATE TABLE external_buckets (
    id BLOB NOT NULL PRIMARY KEY CHECK (length(id)=16),
    project_id BLOB NOT NULL REFERENCES projects(id) CHECK (length(project_id)=16),
    name TEXT NOT NULL CHECK (length(CAST(name AS BLOB)) BETWEEN 1 AND 128),
    connection TEXT NOT NULL CHECK (json_valid(connection) AND json_type(connection)='object'),
    revision INTEGER NOT NULL DEFAULT 1 CHECK (revision>0),
    access_encrypted BLOB NOT NULL,
    secret_encrypted BLOB NOT NULL,
    enabled INTEGER NOT NULL DEFAULT 1 CHECK (enabled IN (0,1)),
    created_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec')*1000000 AS INTEGER)),
    updated_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec')*1000000 AS INTEGER)),
    UNIQUE (project_id,name),
    UNIQUE (id,project_id)
) STRICT, WITHOUT ROWID;

CREATE TABLE member_external_grants (
    user_id BLOB NOT NULL CHECK (length(user_id)=16),
    project_id BLOB NOT NULL CHECK (length(project_id)=16),
    bucket_id BLOB NOT NULL CHECK (length(bucket_id)=16),
    actions INTEGER NOT NULL CHECK (actions BETWEEN 0 AND 15),
    PRIMARY KEY (user_id,bucket_id),
    FOREIGN KEY (user_id,project_id) REFERENCES project_members(user_id,project_id) ON DELETE CASCADE,
    FOREIGN KEY (bucket_id,project_id) REFERENCES external_buckets(id,project_id) ON DELETE CASCADE
) STRICT, WITHOUT ROWID;
CREATE INDEX member_external_bucket ON member_external_grants(bucket_id);
CREATE INDEX member_external_membership ON member_external_grants(user_id,project_id);

CREATE TABLE token_external_grants (
    token_id BLOB NOT NULL REFERENCES api_tokens(id) ON DELETE CASCADE CHECK (length(token_id)=16),
    bucket_id BLOB NOT NULL REFERENCES external_buckets(id) ON DELETE CASCADE CHECK (length(bucket_id)=16),
    actions INTEGER NOT NULL CHECK (actions BETWEEN 0 AND 15),
    PRIMARY KEY (token_id,bucket_id)
) STRICT, WITHOUT ROWID;
CREATE INDEX token_external_bucket ON token_external_grants(bucket_id);

CREATE TABLE external_tests (
    id BLOB NOT NULL PRIMARY KEY REFERENCES tasks(id) ON DELETE CASCADE CHECK (length(id)=16),
    external_id BLOB REFERENCES external_buckets(id) ON DELETE SET NULL CHECK (external_id IS NULL OR length(external_id)=16),
    revision INTEGER NOT NULL CHECK (revision>0),
    connection_encrypted BLOB,
    node_id BLOB CHECK (node_id IS NULL OR length(node_id)=16),
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts>=0),
    report TEXT NOT NULL DEFAULT '{}' CHECK (json_valid(report) AND json_type(report)='object'),
    cleanup_required INTEGER NOT NULL DEFAULT 1 CHECK (cleanup_required IN (0,1)),
    created_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec')*1000000 AS INTEGER)),
    updated_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec')*1000000 AS INTEGER))
) STRICT, WITHOUT ROWID;
CREATE INDEX external_tests_bucket ON external_tests(external_id,created_at DESC,id DESC);

CREATE VIEW user_external_access AS
SELECT m.user_id,b.id AS bucket_id,
    (CASE m.role WHEN 'reader' THEN 3 WHEN 'writer' THEN 15 WHEN 'maintainer' THEN 15 ELSE 0 END)
    & IIF(m.scope='all',15,COALESCE(g.actions,0)) AS actions
FROM project_members m JOIN external_buckets b ON b.project_id=m.project_id
LEFT JOIN member_external_grants g ON g.user_id=m.user_id AND g.bucket_id=b.id;

CREATE VIEW token_external_access AS
SELECT t.id AS token_id,g.bucket_id,
    g.actions & IIF(u.role='admin',15,COALESCE(a.actions,0)) AS actions
FROM api_tokens t JOIN web_users u ON u.id=t.user_id
JOIN token_external_grants g ON g.token_id=t.id
LEFT JOIN user_external_access a ON a.user_id=t.user_id AND a.bucket_id=g.bucket_id;

UPDATE mokyu_meta SET schema_version=44 WHERE id=1;
