ALTER TABLE raw_sources ADD COLUMN processing_task UUID REFERENCES tasks(id) ON DELETE SET NULL;
CREATE INDEX raw_sources_processing ON raw_sources(processing_task);
UPDATE mokyu_meta SET schema_version=60 WHERE singleton;
