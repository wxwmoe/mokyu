CREATE TABLE tasks (
    id BLOB CHECK (id IS NULL OR length(id)=16) NOT NULL PRIMARY KEY,
    task_type TEXT NOT NULL,
    bucket_id BLOB CHECK (bucket_id IS NULL OR length(bucket_id)=16),
    state TEXT NOT NULL,
    progress_cursor TEXT,
    processed INTEGER NOT NULL DEFAULT 0,
    detail TEXT CHECK (json_valid(detail)) NOT NULL DEFAULT (JSON_OBJECT()),
    policy TEXT GENERATED ALWAYS AS (COALESCE(NULLIF(json_extract(detail,'$.policy'),'null'),task_type)) VIRTUAL,
    error TEXT,
    created_by_label TEXT NOT NULL DEFAULT 'Unknown',
    source TEXT NOT NULL DEFAULT 'unknown',
    execution_token BLOB CHECK (execution_token IS NULL OR length(execution_token)=16),
    created_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec')*1000000 AS INTEGER)),
    updated_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec')*1000000 AS INTEGER)),
    started_at INTEGER,
    CONSTRAINT tasks_bucket FOREIGN KEY (bucket_id) REFERENCES buckets(id) ON DELETE SET NULL,
    CONSTRAINT tasks_state CHECK (state IN ('queued','running','paused','completed','failed')),
    CONSTRAINT tasks_type CHECK (task_type IN ('purge','sweep','integrity','pack','unpack','upload','cache_flush','gc','cleanup','storage_migration','rewrite','usage')),
    CONSTRAINT tasks_processed CHECK (processed>=0)
) STRICT, WITHOUT ROWID;

CREATE INDEX tasks_work ON tasks(state,updated_at,id);
CREATE INDEX tasks_list ON tasks(created_at DESC,id DESC);
CREATE INDEX tasks_state_list ON tasks(state,created_at DESC,id DESC);
CREATE INDEX tasks_policy_recent ON tasks(policy,created_at DESC,id DESC);
CREATE INDEX tasks_bucket_id_fk ON tasks(bucket_id);

CREATE TABLE streams (
    id BLOB CHECK (id IS NULL OR length(id)=16) NOT NULL PRIMARY KEY,
    bucket_id BLOB CHECK (bucket_id IS NULL OR length(bucket_id)=16) NOT NULL,
    object_key BLOB NOT NULL,
    stream_type TEXT NOT NULL,
    state TEXT NOT NULL,
    size INTEGER NOT NULL DEFAULT 0,
    etag TEXT NOT NULL DEFAULT '',
    metadata TEXT CHECK (json_valid(metadata)) NOT NULL DEFAULT (JSON_OBJECT()),
    public_read INTEGER CHECK (public_read IN (0,1)) NOT NULL DEFAULT FALSE,
    checksums TEXT CHECK (json_valid(checksums)) NOT NULL DEFAULT (JSON_OBJECT()),
    upload_cache_bypass INTEGER CHECK (upload_cache_bypass IN (0,1)) NOT NULL DEFAULT FALSE,
    write_authorization TEXT CHECK (json_valid(write_authorization)) NOT NULL,
    owner_task_id BLOB CHECK (owner_task_id IS NULL OR length(owner_task_id)=16),
    owner_execution_token BLOB CHECK (owner_execution_token IS NULL OR length(owner_execution_token)=16),
    created_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec')*1000000 AS INTEGER)),
    touched_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec')*1000000 AS INTEGER)),
    CONSTRAINT streams_bucket FOREIGN KEY (bucket_id) REFERENCES buckets(id),
    CONSTRAINT streams_owner FOREIGN KEY (owner_task_id) REFERENCES tasks(id) ON DELETE SET NULL,
    CONSTRAINT streams_key CHECK (length(CAST(object_key AS BLOB)) BETWEEN 1 AND 1024),
    CONSTRAINT streams_type CHECK (stream_type IN ('object','part')),
    CONSTRAINT streams_state CHECK (state IN ('writing','ready','retired','abandoned')),
    CONSTRAINT streams_size CHECK (size>=0)
) STRICT, WITHOUT ROWID;

CREATE INDEX streams_cleanup ON streams(state,touched_at,id);
CREATE INDEX streams_writing ON streams(bucket_id,object_key,state);
CREATE INDEX streams_owner_task_id_fk ON streams(owner_task_id);

CREATE TABLE packs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    location_id BLOB CHECK (location_id IS NULL OR length(location_id)=16) NOT NULL UNIQUE,
    storage_id BLOB CHECK (storage_id IS NULL OR length(storage_id)=16) NOT NULL,
    encryption_algorithm TEXT NOT NULL,
    block_key_id BLOB CHECK (block_key_id IS NULL OR length(block_key_id)=16),
    raw_size INTEGER NOT NULL,
    stored_size INTEGER,
    pack_compressed INTEGER CHECK (pack_compressed IN (0,1)) NOT NULL DEFAULT FALSE,
    nonce BLOB,
    digest BLOB CHECK (digest IS NULL OR length(digest)=32),
    member_count INTEGER NOT NULL,
    state TEXT NOT NULL,
    owner_task BLOB CHECK (owner_task IS NULL OR length(owner_task)=16),
    pack_fingerprint TEXT CHECK (json_valid(pack_fingerprint)) NOT NULL,
    created_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec')*1000000 AS INTEGER)),
    unreferenced_at INTEGER,
    deleted_at INTEGER,
    range_checked_at INTEGER,
    stored_at INTEGER,
    CONSTRAINT packs_storage FOREIGN KEY (storage_id) REFERENCES storages(id),
    CONSTRAINT packs_key FOREIGN KEY (block_key_id) REFERENCES managed_keys(id),
    CONSTRAINT packs_owner FOREIGN KEY (owner_task) REFERENCES tasks(id) ON DELETE SET NULL,
    CONSTRAINT packs_algorithm CHECK (encryption_algorithm IN ('none','aes-256-gcm','chacha20-poly1305')),
    CONSTRAINT packs_encryption CHECK ((encryption_algorithm='none' AND block_key_id IS NULL) OR (encryption_algorithm<>'none' AND block_key_id IS NOT NULL)),
    CONSTRAINT packs_size CHECK (raw_size>0 AND (stored_size IS NULL OR stored_size>0)),
    CONSTRAINT packs_members CHECK (member_count BETWEEN 2 AND 4096),
    CONSTRAINT packs_nonce CHECK (nonce IS NULL OR length(CAST(nonce AS BLOB))=12),
    CONSTRAINT packs_state CHECK (state IN ('preparing','ready','retired','deleting','deleted'))
) STRICT;

CREATE UNIQUE INDEX packs_storage_id ON packs(storage_id,id);
CREATE INDEX packs_gc ON packs(state,unreferenced_at,id);
CREATE INDEX packs_block_key_id_fk ON packs(block_key_id);
CREATE INDEX packs_owner_task_fk ON packs(owner_task);

CREATE TABLE chunks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    storage_id BLOB CHECK (storage_id IS NULL OR length(storage_id)=16) NOT NULL,
    owner_stream BLOB CHECK (owner_stream IS NULL OR length(owner_stream)=16),
    hash BLOB CHECK (hash IS NULL OR length(hash)=32) NOT NULL,
    raw_size INTEGER NOT NULL,
    standalone_size INTEGER,
    encryption_algorithm TEXT NOT NULL,
    block_key_id BLOB CHECK (block_key_id IS NULL OR length(block_key_id)=16),
    standalone_compressed INTEGER CHECK (standalone_compressed IN (0,1)) NOT NULL DEFAULT FALSE,
    nonce BLOB,
    format INTEGER NOT NULL DEFAULT 1,
    state TEXT NOT NULL,
    current_encoding_id INTEGER NOT NULL DEFAULT 0,
    pack_id INTEGER,
    created_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec')*1000000 AS INTEGER)),
    unreferenced_at INTEGER,
    deleted_at INTEGER,
    reference_changed_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec')*1000000 AS INTEGER)),
    split_at INTEGER,
    range_split_at INTEGER,
    repack_after INTEGER,
    CONSTRAINT chunks_storage FOREIGN KEY (storage_id) REFERENCES storages(id),
    CONSTRAINT chunks_owner FOREIGN KEY (owner_stream) REFERENCES streams(id) ON DELETE SET NULL,
    CONSTRAINT chunks_key FOREIGN KEY (block_key_id) REFERENCES managed_keys(id),
    CONSTRAINT chunks_pack FOREIGN KEY (storage_id,pack_id) REFERENCES packs(storage_id,id),
    CONSTRAINT chunks_algorithm CHECK (encryption_algorithm IN ('none','aes-256-gcm','chacha20-poly1305')),
    CONSTRAINT chunks_encryption CHECK ((encryption_algorithm='none' AND block_key_id IS NULL) OR (encryption_algorithm<>'none' AND block_key_id IS NOT NULL)),
    CONSTRAINT chunks_raw_size CHECK (raw_size BETWEEN 1 AND 4194304),
    CONSTRAINT chunks_stored_size CHECK (standalone_size IS NULL OR standalone_size BETWEEN 1 AND 4194320),
    CONSTRAINT chunks_format CHECK (format=1),
    CONSTRAINT chunks_state CHECK (state IN ('preparing','uploading','ready','failed','deleting','deleted')),
    CONSTRAINT chunks_encoding CHECK (current_encoding_id>0 OR state='preparing')
) STRICT;

CREATE UNIQUE INDEX chunks_storage_id ON chunks(storage_id,id);
CREATE INDEX chunks_gc ON chunks(storage_id,state,unreferenced_at,id);
CREATE INDEX chunks_deleted ON chunks(state,deleted_at,id);
CREATE INDEX chunks_pack_id ON chunks(pack_id);
CREATE INDEX chunks_repack ON chunks(repack_after,id);
CREATE INDEX chunks_owner_stream_fk ON chunks(owner_stream);
CREATE INDEX chunks_block_key_id_fk ON chunks(block_key_id);

CREATE TABLE chunk_encodings (
    encoding_id INTEGER PRIMARY KEY AUTOINCREMENT,
    chunk_id INTEGER NOT NULL,
    location_id BLOB CHECK (location_id IS NULL OR length(location_id)=16) NOT NULL UNIQUE,
    storage_id BLOB CHECK (storage_id IS NULL OR length(storage_id)=16) NOT NULL,
    standalone_size INTEGER,
    standalone_compressed INTEGER CHECK (standalone_compressed IN (0,1)) NOT NULL DEFAULT FALSE,
    nonce BLOB,
    state TEXT NOT NULL,
    owner_task BLOB CHECK (owner_task IS NULL OR length(owner_task)=16),
    encryption_algorithm TEXT NOT NULL,
    block_key_id BLOB CHECK (block_key_id IS NULL OR length(block_key_id)=16),
    encoding_fingerprint TEXT CHECK (json_valid(encoding_fingerprint)) NOT NULL,
    created_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec')*1000000 AS INTEGER)),
    unreferenced_at INTEGER,
    deleted_at INTEGER,
    stored_at INTEGER,
    CONSTRAINT encodings_chunk FOREIGN KEY (storage_id,chunk_id) REFERENCES chunks(storage_id,id) ON DELETE CASCADE,
    CONSTRAINT encodings_owner FOREIGN KEY (owner_task) REFERENCES tasks(id) ON DELETE SET NULL,
    CONSTRAINT encodings_key FOREIGN KEY (block_key_id) REFERENCES managed_keys(id),
    CONSTRAINT encodings_algorithm CHECK (encryption_algorithm IN ('none','aes-256-gcm','chacha20-poly1305')),
    CONSTRAINT encodings_encryption CHECK ((encryption_algorithm='none' AND block_key_id IS NULL) OR (encryption_algorithm<>'none' AND block_key_id IS NOT NULL)),
    CONSTRAINT encodings_size CHECK (standalone_size IS NULL OR standalone_size BETWEEN 1 AND 4194320),
    CONSTRAINT encodings_nonce CHECK (nonce IS NULL OR length(CAST(nonce AS BLOB))=12),
    CONSTRAINT encodings_state CHECK (state IN ('uploading','ready','retired','deleting','deleted'))
) STRICT;

CREATE INDEX encodings_chunk_id ON chunk_encodings(chunk_id);
CREATE INDEX encodings_gc ON chunk_encodings(state,unreferenced_at,encoding_id);
CREATE INDEX chunk_encodings_owner_task_fk ON chunk_encodings(owner_task);
CREATE INDEX chunk_encodings_block_key_id_fk ON chunk_encodings(block_key_id);

CREATE TABLE pending_uploads (
    chunk_id INTEGER NOT NULL PRIMARY KEY,
    stream_id BLOB CHECK (stream_id IS NULL OR length(stream_id)=16),
    offset_bytes INTEGER NOT NULL,
    staged_size INTEGER NOT NULL,
    cache_key TEXT,
    source_pack_id INTEGER,
    owner_task BLOB CHECK (owner_task IS NULL OR length(owner_task)=16),
    created_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec')*1000000 AS INTEGER)),
    next_retry_at INTEGER NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    CONSTRAINT pending_chunk FOREIGN KEY (chunk_id) REFERENCES chunks(id),
    CONSTRAINT pending_stream FOREIGN KEY (stream_id) REFERENCES streams(id) ON DELETE SET NULL,
    CONSTRAINT pending_pack FOREIGN KEY (source_pack_id) REFERENCES packs(id),
    CONSTRAINT pending_owner FOREIGN KEY (owner_task) REFERENCES tasks(id) ON DELETE SET NULL,
    CONSTRAINT pending_size CHECK (staged_size>0),
    CONSTRAINT pending_offset CHECK (offset_bytes>=0),
    CONSTRAINT pending_attempts CHECK (attempts>=0)
) STRICT, WITHOUT ROWID;

CREATE INDEX pending_due ON pending_uploads(owner_task,next_retry_at,chunk_id);
CREATE INDEX pending_stream_offset ON pending_uploads(stream_id,offset_bytes);
CREATE INDEX pending_cache_key ON pending_uploads(cache_key);
CREATE INDEX pending_uploads_source_pack_id_fk ON pending_uploads(source_pack_id);

CREATE UNIQUE INDEX chunks_dedup ON chunks(storage_id,hash,raw_size) WHERE state IN ('preparing','uploading','ready');
CREATE UNIQUE INDEX encodings_ready ON chunk_encodings(chunk_id) WHERE state='ready';

UPDATE mokyu_meta SET schema_version=6 WHERE id=1;
