CREATE TABLE storage_maintenance (
    storage_id BLOB NOT NULL REFERENCES storages(id) ON DELETE CASCADE,
    policy TEXT NOT NULL,
    last_scheduled_at INTEGER NOT NULL,
    PRIMARY KEY(storage_id,policy)
) STRICT;

ALTER TABLE storage_profiles ADD COLUMN multipart_idle_timeout TEXT NOT NULL DEFAULT '24h';
ALTER TABLE storage_profiles ADD COLUMN min_free_space TEXT;

UPDATE storage_profiles SET multipart_idle_timeout=(SELECT json_extract(value,'$.idle_timeout') FROM runtime_settings WHERE section='uploads'),revision=revision+1
WHERE EXISTS(SELECT 1 FROM runtime_settings WHERE section='uploads' AND json_type(value,'$.idle_timeout') IS NOT NULL);
UPDATE runtime_settings SET value=json_remove(value,'$.idle_timeout'),revision=revision+1,updated_at=CAST(unixepoch('subsec')*1000000 AS INTEGER)
WHERE section='uploads' AND json_type(value,'$.idle_timeout') IS NOT NULL;

UPDATE mokyu_meta SET schema_version=23 WHERE id=1;
