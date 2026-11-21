ALTER TABLE buckets ADD COLUMN restore_job BLOB REFERENCES journal_recoveries(id);
CREATE INDEX buckets_restore_job ON buckets(restore_job) WHERE restore_job IS NOT NULL;
CREATE VIEW published_buckets AS SELECT * FROM buckets WHERE restore_job IS NULL;

ALTER TABLE journal_restore_buckets ADD COLUMN prepared_key BLOB;
ALTER TABLE journal_restore_buckets ADD COLUMN prepared_objects INTEGER NOT NULL DEFAULT 0;

DROP VIEW user_bucket_access;
CREATE VIEW user_bucket_access AS
SELECT m.user_id,b.id AS bucket_id,
    (CASE m.role WHEN 'reader' THEN 67 WHEN 'writer' THEN 479 WHEN 'maintainer' THEN 2047 ELSE 0 END)
    & IIF(m.scope='all',2047,COALESCE(g.actions,0)) AS actions
FROM project_members m JOIN published_buckets b ON b.project_id=m.project_id
LEFT JOIN member_bucket_grants g ON g.user_id=m.user_id AND g.bucket_id=b.id;

DROP TRIGGER objects_notice_insert;
CREATE TRIGGER objects_notice_insert AFTER INSERT ON objects
WHEN NEW.stream_id IS NOT NULL AND EXISTS (SELECT 1 FROM published_buckets WHERE id=NEW.bucket_id)
BEGIN
    INSERT INTO notice_changes VALUES('object',NEW.bucket_id,NEW.object_key);
END;

DROP TRIGGER objects_notice_delete;
CREATE TRIGGER objects_notice_delete AFTER DELETE ON objects
WHEN OLD.stream_id IS NOT NULL AND EXISTS (SELECT 1 FROM published_buckets WHERE id=OLD.bucket_id)
BEGIN
    INSERT INTO notice_changes VALUES('object',OLD.bucket_id,OLD.object_key);
END;

UPDATE mokyu_meta SET schema_version=20261121095744 WHERE id=1;
