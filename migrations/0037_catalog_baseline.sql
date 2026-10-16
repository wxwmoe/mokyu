-- New and empty legacy databases finish their catalog before serving requests.
DROP INDEX IF EXISTS objects_catalog_size;
DROP INDEX IF EXISTS objects_catalog_modified;
DROP INDEX IF EXISTS objects_catalog_category;
DROP INDEX IF EXISTS objects_catalog_search;
CREATE INDEX objects_catalog_size ON objects(bucket_id,catalog_size,object_key)
    WHERE stream_id IS NOT NULL;
CREATE INDEX objects_catalog_modified ON objects(bucket_id,catalog_modified,object_key)
    WHERE stream_id IS NOT NULL;
CREATE INDEX objects_catalog_category ON objects(bucket_id,catalog_category,object_key)
    WHERE stream_id IS NOT NULL;
CREATE INDEX objects_catalog_search ON objects USING gin ((object_key COLLATE "default") gin_trgm_ops)
    WHERE stream_id IS NOT NULL;
DROP TABLE catalog_build;
UPDATE mokyu_meta SET schema_version=37;
