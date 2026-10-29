SET @mokyu_external_acl_ddl = CONCAT('ALTER TABLE member_external_grants ',
    IF(LOCATE('MariaDB',VERSION())>0, 'DROP CONSTRAINT ', 'DROP CHECK '),
    'member_external_actions, ADD CONSTRAINT member_external_actions CHECK (actions<=31)');
PREPARE mokyu_external_acl_statement FROM @mokyu_external_acl_ddl;
EXECUTE mokyu_external_acl_statement;
DEALLOCATE PREPARE mokyu_external_acl_statement;

SET @mokyu_external_acl_ddl = CONCAT('ALTER TABLE token_external_grants ',
    IF(LOCATE('MariaDB',VERSION())>0, 'DROP CONSTRAINT ', 'DROP CHECK '),
    'token_external_actions, ADD CONSTRAINT token_external_actions CHECK (actions<=31)');
PREPARE mokyu_external_acl_statement FROM @mokyu_external_acl_ddl;
EXECUTE mokyu_external_acl_statement;
DEALLOCATE PREPARE mokyu_external_acl_statement;

CREATE OR REPLACE VIEW user_external_access AS
SELECT m.user_id,b.id AS bucket_id,
    (CASE m.role WHEN 'reader' THEN 3 WHEN 'writer' THEN 31 WHEN 'maintainer' THEN 31 ELSE 0 END)
    & IF(m.scope='all',31,COALESCE(g.actions,0)) AS actions
FROM project_members m JOIN external_buckets b ON b.project_id=m.project_id
LEFT JOIN member_external_grants g ON g.user_id=m.user_id AND g.bucket_id=b.id;

CREATE OR REPLACE VIEW token_external_access AS
SELECT t.id AS token_id,g.bucket_id,
    g.actions & IF(u.role='admin',31,COALESCE(a.actions,0)) AS actions
FROM api_tokens t JOIN web_users u ON u.id=t.user_id
JOIN token_external_grants g ON g.token_id=t.id
LEFT JOIN user_external_access a ON a.user_id=t.user_id AND a.bucket_id=g.bucket_id;

UPDATE mokyu_meta SET schema_version=45 WHERE id=1;
