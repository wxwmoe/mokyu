ALTER TABLE storage_objects ADD CONSTRAINT storage_object_path_size CHECK(octet_length(path)<=1024);
CREATE INDEX storage_objects_listing ON storage_objects(namespace_id,path COLLATE "C") WHERE head IS NOT NULL;
UPDATE mokyu_meta SET schema_version=20261113211629 WHERE singleton;
