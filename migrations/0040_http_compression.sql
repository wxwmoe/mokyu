ALTER TABLE streams ADD COLUMN http_compression jsonb NOT NULL DEFAULT '{}';
UPDATE mokyu_meta SET schema_version=40;
