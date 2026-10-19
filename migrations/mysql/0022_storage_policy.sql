CREATE TABLE storage_maintenance (
    storage_id binary(16) NOT NULL,
    policy varchar(16) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    last_scheduled_at datetime(6) NOT NULL,
    PRIMARY KEY(storage_id,policy),
    CONSTRAINT storage_maintenance_storage_fk FOREIGN KEY(storage_id) REFERENCES storages(id) ON DELETE CASCADE
) ENGINE=InnoDB;

ALTER TABLE storage_profiles ADD COLUMN multipart_idle_timeout varchar(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '24h';
ALTER TABLE storage_profiles ADD COLUMN min_free_space varchar(32) CHARACTER SET ascii COLLATE ascii_bin;

UPDATE storage_profiles SET multipart_idle_timeout=(SELECT JSON_UNQUOTE(JSON_EXTRACT(value,'$.idle_timeout')) FROM runtime_settings WHERE section='uploads'),revision=revision+1
WHERE EXISTS(SELECT 1 FROM runtime_settings WHERE section='uploads' AND JSON_CONTAINS_PATH(value,'one','$.idle_timeout'));
UPDATE runtime_settings SET value=JSON_REMOVE(value,'$.idle_timeout'),revision=revision+1,updated_at=UTC_TIMESTAMP(6)
WHERE section='uploads' AND JSON_CONTAINS_PATH(value,'one','$.idle_timeout');

UPDATE mokyu_meta SET schema_version=22 WHERE id=1;
