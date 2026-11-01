SET @mokyu_audit_ddl = IF(LOCATE('MariaDB',VERSION())>0,
    'ALTER TABLE audit_events DROP CONSTRAINT audit_source, ADD CONSTRAINT audit_source CHECK (source IN (''web'',''token'',''cli'',''s3''))',
    'ALTER TABLE audit_events DROP CHECK audit_source, ADD CONSTRAINT audit_source CHECK (source IN (''web'',''token'',''cli'',''s3''))'
);
PREPARE mokyu_audit FROM @mokyu_audit_ddl;
EXECUTE mokyu_audit;
DEALLOCATE PREPARE mokyu_audit;
UPDATE mokyu_meta SET schema_version=20 WHERE id=1;
