ALTER TABLE storage_profiles ADD COLUMN file_delivery TEXT NOT NULL DEFAULT 'application' CONSTRAINT storage_file_delivery CHECK(file_delivery IN ('application','x_accel_redirect','x_sendfile'));
ALTER TABLE storage_profiles ADD COLUMN file_delivery_prefix TEXT CONSTRAINT storage_delivery_prefix CHECK((file_delivery='application' AND file_delivery_prefix IS NULL) OR (file_delivery<>'application' AND file_delivery_prefix IS NOT NULL));
UPDATE mokyu_meta SET schema_version=41 WHERE id=1;
