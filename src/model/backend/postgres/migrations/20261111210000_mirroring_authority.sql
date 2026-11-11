ALTER TABLE bucket_mirroring ADD COLUMN authority JSONB;
UPDATE mokyu_meta SET schema_version = 20261111210000;
