ALTER TABLE transfer_items ADD COLUMN reuse_target BOOLEAN NOT NULL DEFAULT FALSE;
CREATE INDEX transfer_items_receipt ON transfer_items(source_version,output_version,state);

UPDATE mokyu_meta SET schema_version=47 WHERE id=1;
