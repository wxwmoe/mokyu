ALTER TABLE node_writes ADD COLUMN copy_source jsonb CHECK (copy_source IS NULL OR jsonb_typeof(copy_source)='object');
ALTER TABLE node_reads DROP CONSTRAINT node_reads_check;
ALTER TABLE node_reads ADD CONSTRAINT node_reads_check CHECK (end_bytes >= offset_bytes);
UPDATE mokyu_meta SET schema_version=79 WHERE singleton;
