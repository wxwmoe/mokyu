ALTER TABLE storage_profiles ADD COLUMN read_integrity TEXT NOT NULL DEFAULT 'inline' CONSTRAINT storage_read_integrity CHECK(read_integrity IN ('inline','background','off'));
UPDATE mokyu_meta SET schema_version=40 WHERE id=1;
