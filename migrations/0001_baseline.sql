-- 0.0.1 baseline. Published migrations are immutable; add a new numbered file for changes.
CREATE TABLE gateway_meta (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    schema_version integer NOT NULL,
    deployment_id uuid NOT NULL,
    backend_identity text NOT NULL,
    backend_initialized boolean NOT NULL DEFAULT false,
    gc_paused boolean NOT NULL DEFAULT false,
    maintenance boolean NOT NULL DEFAULT false,
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE key_fingerprints (
    key_id text PRIMARY KEY,
    algorithm text NOT NULL,
    fingerprint bytea NOT NULL CHECK (octet_length(fingerprint) = 32)
);
CREATE TABLE buckets (
    id uuid PRIMARY KEY,
    name text COLLATE "C" NOT NULL UNIQUE,
    state text NOT NULL DEFAULT 'active' CHECK (state IN ('active','purging')),
    cors jsonb NOT NULL DEFAULT '[]',
    website_enabled boolean NOT NULL DEFAULT false,
    index_document text NOT NULL DEFAULT 'index.html',
    error_document text NOT NULL DEFAULT '404.html',
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE credentials (
    access_key text PRIMARY KEY,
    secret_encrypted bytea NOT NULL,
    enabled boolean NOT NULL DEFAULT true,
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE grants (
    access_key text NOT NULL REFERENCES credentials ON DELETE CASCADE,
    bucket_id uuid NOT NULL REFERENCES buckets ON DELETE CASCADE,
    writable boolean NOT NULL,
    PRIMARY KEY (access_key,bucket_id)
);
CREATE TABLE domains (
    host text PRIMARY KEY,
    bucket_id uuid NOT NULL REFERENCES buckets ON DELETE CASCADE
);
CREATE TABLE streams (
    id uuid PRIMARY KEY,
    bucket_id uuid NOT NULL REFERENCES buckets,
    object_key text COLLATE "C" NOT NULL,
    kind text NOT NULL CHECK (kind IN ('object','part')),
    state text NOT NULL CHECK (state IN ('writing','ready','retired','abandoned')),
    size bigint NOT NULL DEFAULT 0 CHECK (size >= 0),
    etag text NOT NULL DEFAULT '',
    metadata jsonb NOT NULL DEFAULT '{}',
    public_read boolean NOT NULL DEFAULT false,
    checksums jsonb NOT NULL DEFAULT '{}',
    created_at timestamptz NOT NULL DEFAULT now(),
    touched_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX streams_cleanup ON streams(state,touched_at,id);
CREATE TABLE objects (
    bucket_id uuid NOT NULL REFERENCES buckets,
    key text COLLATE "C" NOT NULL CHECK (octet_length(key) BETWEEN 1 AND 1024),
    stream_id uuid UNIQUE REFERENCES streams,
    write_epoch uuid NOT NULL,
    PRIMARY KEY(bucket_id,key)
);
CREATE TABLE chunks (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    storage_id uuid NOT NULL UNIQUE,
    owner_stream uuid REFERENCES streams ON DELETE SET NULL,
    hash bytea NOT NULL CHECK (octet_length(hash) = 32),
    raw_size integer NOT NULL CHECK (raw_size BETWEEN 1 AND 4194304),
    stored_size integer CHECK (stored_size BETWEEN 1 AND 4194320),
    algorithm text NOT NULL CHECK (algorithm IN ('none','aes-256-gcm','chacha20-poly1305')),
    key_id text NOT NULL,
    compressed boolean NOT NULL DEFAULT false,
    nonce bytea,
    format integer NOT NULL DEFAULT 1 CHECK (format = 1),
    state text NOT NULL CHECK (state IN ('preparing','uploading','ready','failed','deleting','deleted')),
    created_at timestamptz NOT NULL DEFAULT now(),
    unreferenced_at timestamptz,
    deleted_at timestamptz,
    CHECK ((algorithm = 'none' AND nonce IS NULL AND key_id = '') OR
           (algorithm <> 'none' AND (nonce IS NULL OR octet_length(nonce) = 12) AND key_id <> ''))
);
CREATE UNIQUE INDEX chunks_dedup ON chunks(hash,raw_size,algorithm,key_id) WHERE state IN ('preparing','uploading','ready');
CREATE INDEX chunks_gc ON chunks(unreferenced_at,id) WHERE state IN ('ready','failed','deleting');
CREATE INDEX chunks_owner ON chunks(owner_stream) WHERE owner_stream IS NOT NULL;
CREATE TABLE fragments (
    id uuid PRIMARY KEY,
    owner_stream uuid REFERENCES streams ON DELETE SET NULL,
    sealed boolean NOT NULL DEFAULT false,
    size integer NOT NULL CHECK (size BETWEEN 1 AND 4194304),
    hash bytea NOT NULL CHECK (octet_length(hash) = 32),
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE extents (
    stream_id uuid NOT NULL REFERENCES streams ON DELETE CASCADE,
    offset_bytes bigint NOT NULL CHECK (offset_bytes >= 0),
    length integer NOT NULL CHECK (length BETWEEN 1 AND 4194304),
    chunk_id bigint REFERENCES chunks,
    fragment_id uuid REFERENCES fragments,
    source_offset integer NOT NULL DEFAULT 0 CHECK (source_offset >= 0 AND source_offset < 4194304),
    CHECK ((chunk_id IS NULL) <> (fragment_id IS NULL)),
    CHECK (source_offset + length <= 4194304),
    PRIMARY KEY(stream_id,offset_bytes)
);
CREATE INDEX extents_chunk ON extents(chunk_id) WHERE chunk_id IS NOT NULL;
CREATE INDEX extents_fragment ON extents(fragment_id) WHERE fragment_id IS NOT NULL;
CREATE INDEX fragments_owner ON fragments(owner_stream) WHERE owner_stream IS NOT NULL;
CREATE TABLE uploads (
    id uuid PRIMARY KEY,
    bucket_id uuid NOT NULL REFERENCES buckets,
    object_key text COLLATE "C" NOT NULL,
    access_key text NOT NULL,
    state text NOT NULL DEFAULT 'active' CHECK (state IN ('active','completing','completed','aborted')),
    metadata jsonb NOT NULL DEFAULT '{}',
    public_read boolean NOT NULL DEFAULT false,
    checksum_algorithm text,
    checksum_type text,
    manifest_hash text,
    result jsonb,
    output_stream uuid REFERENCES streams ON DELETE SET NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    touched_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX uploads_expiry ON uploads(state,touched_at,id);
CREATE INDEX uploads_list ON uploads(bucket_id,object_key,id);
CREATE TABLE parts (
    upload_id uuid NOT NULL REFERENCES uploads ON DELETE CASCADE,
    part_number integer NOT NULL CHECK (part_number BETWEEN 1 AND 10000),
    stream_id uuid REFERENCES streams,
    write_epoch uuid NOT NULL,
    PRIMARY KEY(upload_id,part_number)
);
CREATE TABLE web_users (
    id uuid PRIMARY KEY,
    username text NOT NULL UNIQUE,
    password_hash text NOT NULL,
    enabled boolean NOT NULL DEFAULT true,
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE sessions (
    token_hash bytea PRIMARY KEY CHECK (octet_length(token_hash) = 32),
    user_id uuid NOT NULL REFERENCES web_users ON DELETE CASCADE,
    csrf_hash bytea NOT NULL CHECK (octet_length(csrf_hash) = 32),
    expires_at timestamptz NOT NULL
);
CREATE INDEX sessions_expiry ON sessions(expires_at);
CREATE TABLE tasks (
    id uuid PRIMARY KEY,
    kind text NOT NULL CHECK (kind IN ('purge','sweep')),
    bucket_id uuid REFERENCES buckets ON DELETE SET NULL,
    state text NOT NULL CHECK (state IN ('queued','running','paused','completed','failed')),
    cursor text,
    processed bigint NOT NULL DEFAULT 0,
    detail jsonb NOT NULL DEFAULT '{}',
    error text,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX tasks_active ON tasks(state,created_at);

INSERT INTO gateway_meta(schema_version,deployment_id,backend_identity)
VALUES (1, gen_random_uuid(), current_setting('media_gateway.backend_identity'));
