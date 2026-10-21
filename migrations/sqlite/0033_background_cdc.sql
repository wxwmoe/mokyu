ALTER TABLE raw_sources ADD COLUMN processing_task BLOB REFERENCES tasks(id) ON DELETE SET NULL;
CREATE INDEX raw_sources_processing ON raw_sources(processing_task);
UPDATE mokyu_meta SET schema_version=33 WHERE id=1;
