ALTER TABLE member_external_grants DROP CONSTRAINT member_external_grants_actions_check;
ALTER TABLE member_external_grants ADD CONSTRAINT member_external_grants_actions_check
    CHECK (actions<@ARRAY['bucket.list','object.read','object.write','object.delete','object.acl']::text[]);
ALTER TABLE token_external_grants DROP CONSTRAINT token_external_grants_actions_check;
ALTER TABLE token_external_grants ADD CONSTRAINT token_external_grants_actions_check
    CHECK (actions<@ARRAY['bucket.list','object.read','object.write','object.delete','object.acl']::text[]);

CREATE OR REPLACE VIEW user_external_access AS
SELECT m.user_id,b.id AS bucket_id,
    ARRAY(SELECT action FROM unnest(CASE m.role
        WHEN 'reader' THEN ARRAY['bucket.list','object.read']
        ELSE ARRAY['bucket.list','object.read','object.write','object.delete','object.acl'] END) AS action
        WHERE m.scope='all' OR action=ANY(COALESCE(g.actions,ARRAY[]::text[]))) AS actions
FROM project_members m JOIN external_buckets b ON b.project_id=m.project_id
LEFT JOIN member_external_grants g ON g.user_id=m.user_id AND g.bucket_id=b.id;

UPDATE mokyu_meta SET schema_version=91 WHERE singleton;
