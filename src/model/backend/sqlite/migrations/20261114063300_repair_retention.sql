ALTER TABLE storage_repairs ADD COLUMN protected_until INTEGER NOT NULL DEFAULT 0 CHECK(protected_until>=0);
ALTER TABLE storage_repairs ADD COLUMN collecting INTEGER NOT NULL DEFAULT FALSE;

ALTER TABLE storage_journal_sources ADD COLUMN published_revision INTEGER NOT NULL DEFAULT 0 CHECK(published_revision>=0);

UPDATE mokyu_meta SET schema_version=20261114063300 WHERE id=1;
