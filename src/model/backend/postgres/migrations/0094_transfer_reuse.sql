ALTER TABLE transfer_items ADD COLUMN reuse_target BOOLEAN NOT NULL DEFAULT FALSE;
CREATE INDEX transfer_items_receipt ON transfer_items(source_version,output_version) WHERE output_version IS NOT NULL AND error IS NULL AND state IN ('completed','skipped');

UPDATE mokyu_meta SET schema_version=94 WHERE singleton;
