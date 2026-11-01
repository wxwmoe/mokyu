CREATE TABLE web_uploads (
    upload_id uuid PRIMARY KEY REFERENCES uploads ON DELETE CASCADE,
    user_id uuid REFERENCES web_users ON DELETE SET NULL,
    client_id uuid NOT NULL,
    request_hash bytea NOT NULL CHECK (octet_length(request_hash)=32),
    file_name text NOT NULL CHECK (octet_length(file_name) BETWEEN 1 AND 1024),
    expected_size bigint NOT NULL CHECK (expected_size BETWEEN 0 AND 53687091200000),
    part_size bigint NOT NULL CHECK (part_size BETWEEN 5242880 AND 5368709120),
    modified_at bigint CHECK (modified_at>=0),
    expected_stream uuid,
    UNIQUE(user_id,client_id)
);
ALTER TABLE uploads ADD COLUMN durable_at timestamptz;
CREATE INDEX uploads_transfers ON uploads(created_at DESC,id);
CREATE INDEX uploads_bucket_transfers ON uploads(bucket_id,created_at DESC,id);
UPDATE mokyu_meta SET schema_version=16;
