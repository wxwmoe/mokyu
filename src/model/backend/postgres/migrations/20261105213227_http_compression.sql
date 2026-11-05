ALTER TABLE storage_profiles ADD COLUMN http_compression_enabled BOOLEAN NOT NULL DEFAULT TRUE;

UPDATE storage_profiles SET file_delivery = 'application', file_delivery_prefix = NULL, revision = revision + 1
WHERE cdc_enabled AND file_delivery <> 'application';
UPDATE storage_profiles SET http_compression_enabled = FALSE, revision = revision + 1
WHERE file_delivery <> 'application';

UPDATE mokyu_meta SET schema_version = 20261105213227;
