ALTER TABLE chunk_locations ADD COLUMN algorithm text;
ALTER TABLE chunk_locations ADD COLUMN key_id text;
UPDATE chunk_locations l SET algorithm=c.algorithm,key_id=c.key_id FROM chunks c WHERE c.id=l.chunk_id;
ALTER TABLE chunk_locations ALTER COLUMN algorithm SET NOT NULL;
ALTER TABLE chunk_locations ALTER COLUMN key_id SET NOT NULL;
ALTER TABLE chunk_locations ADD CHECK(algorithm IN ('none','aes-256-gcm','chacha20-poly1305'));

CREATE TABLE pending_uploads (
    chunk_id bigint PRIMARY KEY REFERENCES chunks(id),
    stream_id uuid REFERENCES streams(id) ON DELETE SET NULL,
    offset_bytes bigint NOT NULL CHECK (offset_bytes >= 0),
    cache_size bigint NOT NULL CHECK (cache_size > 0),
    cache_compressed boolean NOT NULL,
    source_pack bigint REFERENCES packs(id),
    created_at timestamptz NOT NULL DEFAULT now(),
    next_retry_at timestamptz NOT NULL,
    attempts integer NOT NULL DEFAULT 0,
    last_error text,
    owner_task uuid REFERENCES tasks(id) ON DELETE SET NULL
);
CREATE INDEX pending_uploads_due ON pending_uploads(next_retry_at,chunk_id) WHERE owner_task IS NULL;
CREATE INDEX pending_uploads_retry ON pending_uploads(next_retry_at,owner_task) WHERE owner_task IS NOT NULL;
CREATE INDEX pending_uploads_stream ON pending_uploads(stream_id,offset_bytes);
CREATE INDEX pending_uploads_source ON pending_uploads(source_pack) WHERE source_pack IS NOT NULL;
CREATE INDEX pending_uploads_task ON pending_uploads(owner_task) WHERE owner_task IS NOT NULL;
CREATE TABLE cache_pins (
    chunk_id bigint NOT NULL REFERENCES pending_uploads(chunk_id) ON DELETE CASCADE,
    pin_type text NOT NULL CHECK (pin_type IN ('upload','pack')),
    owner_id uuid NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY(chunk_id,pin_type,owner_id)
);
ALTER TABLE streams ADD COLUMN upload_cache_bypass boolean NOT NULL DEFAULT false;
ALTER TABLE tasks DROP CONSTRAINT tasks_kind_check;
ALTER TABLE tasks ADD CONSTRAINT tasks_kind_check CHECK (kind IN ('purge','sweep','integrity','pack','unpack','upload','cache_flush'));
UPDATE gateway_meta SET schema_version=6;
