DROP VIEW token_external_access;
DROP VIEW user_external_access;

CREATE TABLE member_external_grants_expanded (
    user_id BLOB NOT NULL CHECK (length(user_id)=16),
    project_id BLOB NOT NULL CHECK (length(project_id)=16),
    bucket_id BLOB NOT NULL CHECK (length(bucket_id)=16),
    actions INTEGER NOT NULL CHECK (actions BETWEEN 0 AND 31),
    PRIMARY KEY (user_id,bucket_id),
    FOREIGN KEY (user_id,project_id) REFERENCES project_members(user_id,project_id) ON DELETE CASCADE,
    FOREIGN KEY (bucket_id,project_id) REFERENCES external_buckets(id,project_id) ON DELETE CASCADE
) STRICT, WITHOUT ROWID;
INSERT INTO member_external_grants_expanded SELECT * FROM member_external_grants;
DROP TABLE member_external_grants;
ALTER TABLE member_external_grants_expanded RENAME TO member_external_grants;
CREATE INDEX member_external_bucket ON member_external_grants(bucket_id);
CREATE INDEX member_external_membership ON member_external_grants(user_id,project_id);

CREATE TABLE token_external_grants_expanded (
    token_id BLOB NOT NULL REFERENCES api_tokens(id) ON DELETE CASCADE CHECK (length(token_id)=16),
    bucket_id BLOB NOT NULL REFERENCES external_buckets(id) ON DELETE CASCADE CHECK (length(bucket_id)=16),
    actions INTEGER NOT NULL CHECK (actions BETWEEN 0 AND 31),
    PRIMARY KEY (token_id,bucket_id)
) STRICT, WITHOUT ROWID;
INSERT INTO token_external_grants_expanded SELECT * FROM token_external_grants;
DROP TABLE token_external_grants;
ALTER TABLE token_external_grants_expanded RENAME TO token_external_grants;
CREATE INDEX token_external_bucket ON token_external_grants(bucket_id);

CREATE VIEW user_external_access AS
SELECT m.user_id,b.id AS bucket_id,
    (CASE m.role WHEN 'reader' THEN 3 WHEN 'writer' THEN 31 WHEN 'maintainer' THEN 31 ELSE 0 END)
    & IIF(m.scope='all',31,COALESCE(g.actions,0)) AS actions
FROM project_members m JOIN external_buckets b ON b.project_id=m.project_id
LEFT JOIN member_external_grants g ON g.user_id=m.user_id AND g.bucket_id=b.id;

CREATE VIEW token_external_access AS
SELECT t.id AS token_id,g.bucket_id,
    g.actions & IIF(u.role='admin',31,COALESCE(a.actions,0)) AS actions
FROM api_tokens t JOIN web_users u ON u.id=t.user_id
JOIN token_external_grants g ON g.token_id=t.id
LEFT JOIN user_external_access a ON a.user_id=t.user_id AND a.bucket_id=g.bucket_id;

UPDATE mokyu_meta SET schema_version=46 WHERE id=1;
