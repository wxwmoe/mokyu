ALTER TABLE storage_profiles RENAME COLUMN write_durability TO previous_write_durability;
ALTER TABLE storage_profiles ADD COLUMN write_durability TEXT NOT NULL DEFAULT 'durable' CHECK(write_durability IN ('durable','buffered','volatile'));
UPDATE storage_profiles SET write_durability=previous_write_durability;
ALTER TABLE storage_profiles DROP COLUMN previous_write_durability;
ALTER TABLE chunk_encodings ADD COLUMN boot_id BLOB CHECK(boot_id IS NULL OR (length(boot_id)=16 AND sync_pending));
DROP INDEX raw_sources_sync;
CREATE INDEX raw_sources_sync ON raw_sources(id) WHERE durability IN ('buffered','volatile') AND state='ready' AND quarantine_code IS NULL;
UPDATE mokyu_meta SET schema_version=36 WHERE id=1;
