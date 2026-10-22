ALTER TABLE storage_profiles ADD COLUMN write_durability TEXT NOT NULL DEFAULT 'durable' CHECK(write_durability IN ('durable','buffered'));
CREATE INDEX raw_sources_sync ON raw_sources(id) WHERE durability='buffered' AND state='ready' AND quarantine_code IS NULL;
UPDATE mokyu_meta SET schema_version=62 WHERE singleton;
