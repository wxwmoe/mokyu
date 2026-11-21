ALTER TABLE raw_sources ADD COLUMN page_size BIGINT NOT NULL DEFAULT 1048576,
    ADD CONSTRAINT raw_source_page_size CHECK (page_size IN (262144,1048576));

UPDATE mokyu_meta SET schema_version=20261121095755 WHERE id=1;
