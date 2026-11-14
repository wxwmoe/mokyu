ALTER TABLE journal_snapshots ADD COLUMN checkpoint_body BLOB;
ALTER TABLE journal_snapshots ADD COLUMN checkpoint_size INTEGER NOT NULL DEFAULT 0;
UPDATE journal_snapshots SET checkpoint_size=index_size;

UPDATE mokyu_meta SET schema_version=20261114013000 WHERE id=1;
