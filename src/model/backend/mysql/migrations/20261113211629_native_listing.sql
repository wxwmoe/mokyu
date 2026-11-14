ALTER TABLE storage_objects MODIFY COLUMN path VARBINARY(1024) NOT NULL,
    ADD INDEX storage_objects_listing(namespace_id,path);
UPDATE mokyu_meta SET schema_version=20261113211629 WHERE id=1;
