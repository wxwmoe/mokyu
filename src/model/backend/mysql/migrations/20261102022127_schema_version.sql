ALTER TABLE mokyu_meta MODIFY schema_version BIGINT NOT NULL;

UPDATE mokyu_meta SET schema_version = 20261102022127;
