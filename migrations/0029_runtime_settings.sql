-- Runtime settings are stored as validated deployment-wide groups.
CREATE TABLE runtime_settings (
    section text PRIMARY KEY CHECK (section IN (
        'web', 's3', 'storage', 'requests', 'uploads', 'cache',
        'compression', 'packing', 'garbage_collection', 'retention',
        'statistics', 'integrity'
    )),
    value jsonb NOT NULL CHECK (jsonb_typeof(value) = 'object'),
    revision bigint NOT NULL DEFAULT 1 CHECK (revision > 0),
    updated_at timestamptz NOT NULL DEFAULT now(),
    updated_by uuid REFERENCES web_users(id) ON DELETE SET NULL
);

ALTER TABLE stores
    ADD COLUMN read_concurrency integer CHECK (read_concurrency IS NULL OR read_concurrency > 0),
    ADD COLUMN upload_concurrency integer CHECK (upload_concurrency IS NULL OR upload_concurrency > 0),
    ADD COLUMN control_concurrency integer CHECK (control_concurrency IS NULL OR control_concurrency > 0);

UPDATE mokyu_meta SET schema_version=29;
