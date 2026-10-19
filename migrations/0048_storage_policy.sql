CREATE TABLE storage_maintenance (
    storage_id uuid NOT NULL REFERENCES storages(id) ON DELETE CASCADE,
    policy text NOT NULL,
    last_scheduled_at timestamptz NOT NULL,
    PRIMARY KEY(storage_id,policy)
);

ALTER TABLE storage_profiles ADD COLUMN multipart_idle_timeout text NOT NULL DEFAULT '24h';
ALTER TABLE storage_profiles ADD COLUMN min_free_space text;

UPDATE storage_profiles SET multipart_idle_timeout=(SELECT value->>'idle_timeout' FROM runtime_settings WHERE section='uploads'),revision=revision+1
WHERE EXISTS(SELECT 1 FROM runtime_settings WHERE section='uploads' AND value ? 'idle_timeout');
UPDATE runtime_settings SET value=value-'idle_timeout',revision=revision+1,updated_at=now()
WHERE section='uploads' AND value ? 'idle_timeout';

UPDATE mokyu_meta SET schema_version=48 WHERE singleton;
