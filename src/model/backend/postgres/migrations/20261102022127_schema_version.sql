ALTER TABLE mokyu_meta ALTER COLUMN schema_version TYPE BIGINT;

UPDATE mokyu_meta SET schema_version = 20261102022127;
