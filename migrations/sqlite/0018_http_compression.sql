ALTER TABLE streams ADD COLUMN http_compression TEXT CHECK (json_valid(http_compression)) NOT NULL DEFAULT '{}';
UPDATE mokyu_meta SET schema_version=18 WHERE id=1;
