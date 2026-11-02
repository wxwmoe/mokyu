ALTER TABLE mokyu_meta ADD COLUMN remote_work BOOLEAN NOT NULL DEFAULT false;

UPDATE mokyu_meta SET schema_version = 20261102190847;
