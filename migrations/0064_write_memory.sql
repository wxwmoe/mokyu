ALTER TABLE storage_profiles DROP CONSTRAINT storage_profiles_write_durability_check;
ALTER TABLE storage_profiles ADD CONSTRAINT storage_profiles_write_durability_check CHECK(write_durability IN ('durable','buffered','volatile'));
ALTER TABLE chunk_encodings ADD COLUMN boot_id UUID CHECK(boot_id IS NULL OR sync_pending);
DROP INDEX raw_sources_sync;
CREATE INDEX raw_sources_sync ON raw_sources(id) WHERE durability IN ('buffered','volatile') AND state='ready' AND quarantine_code IS NULL;
UPDATE mokyu_meta SET schema_version=64 WHERE singleton;
