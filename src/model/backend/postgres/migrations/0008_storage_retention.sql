-- NULL means the remote write time is unknown; never infer it from allocation time.
ALTER TABLE chunk_locations ADD COLUMN stored_at timestamptz;
ALTER TABLE packs ADD COLUMN stored_at timestamptz;
UPDATE gateway_meta SET schema_version=8;
