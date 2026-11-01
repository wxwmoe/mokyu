ALTER TABLE node_write_batches ADD COLUMN packing jsonb CHECK (packing IS NULL OR (jsonb_typeof(packing)='object' AND octet_length(packing::text)<=262144));

UPDATE mokyu_meta SET schema_version=85 WHERE singleton;
