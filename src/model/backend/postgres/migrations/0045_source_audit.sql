ALTER TABLE audit_events DROP CONSTRAINT audit_events_source_check;
ALTER TABLE audit_events ADD CONSTRAINT audit_events_source_check CHECK (source IN ('web','token','cli','s3'));
UPDATE mokyu_meta SET schema_version=45 WHERE singleton;
