CREATE TEMP TABLE mokyu_identity_guard (
    empty_store INTEGER NOT NULL CHECK(empty_store=0)
);
INSERT INTO mokyu_identity_guard VALUES (EXISTS(SELECT 1 FROM cache_pins) OR EXISTS(SELECT 1 FROM chunk_encodings) OR EXISTS(SELECT 1 FROM chunks) OR EXISTS(SELECT 1 FROM extents) OR EXISTS(SELECT 1 FROM fragments) OR EXISTS(SELECT 1 FROM multipart_files) OR EXISTS(SELECT 1 FROM objects) OR EXISTS(SELECT 1 FROM pack_maintenance) OR EXISTS(SELECT 1 FROM packs) OR EXISTS(SELECT 1 FROM parts) OR EXISTS(SELECT 1 FROM pending_uploads) OR EXISTS(SELECT 1 FROM quota_reservations) OR EXISTS(SELECT 1 FROM quota_writes) OR EXISTS(SELECT 1 FROM raw_processing) OR EXISTS(SELECT 1 FROM raw_ranges) OR EXISTS(SELECT 1 FROM raw_sources) OR EXISTS(SELECT 1 FROM streams) OR EXISTS(SELECT 1 FROM uploads) OR EXISTS(SELECT 1 FROM web_uploads) OR EXISTS(SELECT 1 FROM integrity_issues WHERE stream_id IS NOT NULL));
DROP TABLE mokyu_identity_guard;

-- Diagnostics without an object identity survive an empty-store upgrade.
CREATE TEMP TABLE mokyu_identity_issues AS SELECT * FROM integrity_issues;

DROP TABLE extents;
DROP TABLE quota_writes;
DROP TABLE fragments;
DROP TABLE web_uploads;
DROP TABLE raw_ranges;
DROP TABLE raw_processing;
DROP TABLE quota_reservations;
DROP TABLE pending_uploads;
DROP TABLE parts;
DROP TABLE multipart_files;
DROP TABLE chunk_encodings;
DROP TABLE cache_pins;
DROP TABLE uploads;
DROP TABLE raw_sources;
DROP TABLE pack_maintenance;
DROP TABLE objects;
DROP TABLE chunks;
DROP TABLE streams;
DROP TABLE integrity_issues;

CREATE TABLE integrity_issues (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id BLOB CHECK (task_id IS NULL OR length(task_id)=16) NOT NULL,
    subject TEXT NOT NULL,
    code TEXT NOT NULL,
    chunk_id INTEGER,
    location_id BLOB CHECK (location_id IS NULL OR length(location_id)=16),
    stream_id INTEGER,
    bucket_id BLOB CHECK (bucket_id IS NULL OR length(bucket_id)=16),
    object_key BLOB,
    detail TEXT CHECK (json_valid(detail)) NOT NULL DEFAULT (JSON_OBJECT()),
    created_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec')*1000000 AS INTEGER)),
    CONSTRAINT integrity_issues_task FOREIGN KEY (task_id) REFERENCES tasks(id) ON DELETE CASCADE
) STRICT;

CREATE TABLE streams (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
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
    touched_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec')*1000000 AS INTEGER)), http_compression TEXT CHECK (json_valid(http_compression)) NOT NULL DEFAULT '{}', payload_type TEXT NOT NULL DEFAULT 'chunks' CHECK (payload_type IN ('chunks','raw')), payload_revision INTEGER NOT NULL DEFAULT 1 CHECK (payload_revision>0),
    CONSTRAINT streams_bucket FOREIGN KEY (bucket_id) REFERENCES buckets(id),
    CONSTRAINT streams_owner FOREIGN KEY (owner_task_id) REFERENCES tasks(id) ON DELETE SET NULL,
    CONSTRAINT streams_key CHECK (length(CAST(object_key AS BLOB)) BETWEEN 1 AND 1024),
    CONSTRAINT streams_type CHECK (stream_type IN ('object','part')),
    CONSTRAINT streams_state CHECK (state IN ('writing','ready','retired','abandoned')),
    CONSTRAINT streams_size CHECK (size>=0)
) STRICT;

CREATE TABLE chunks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    storage_id BLOB CHECK (storage_id IS NULL OR length(storage_id)=16) NOT NULL,
    owner_stream INTEGER,
    hash BLOB CHECK (hash IS NULL OR length(hash)=32) NOT NULL,
    raw_size INTEGER NOT NULL,
    standalone_size INTEGER,
    encryption_algorithm TEXT NOT NULL,
    block_key_id BLOB CHECK (block_key_id IS NULL OR length(block_key_id)=16),
    standalone_compressed INTEGER CHECK (standalone_compressed IN (0,1)) NOT NULL DEFAULT FALSE,
    nonce BLOB,
    uuid BLOB NOT NULL CHECK(length(uuid)=16),
    format INTEGER NOT NULL DEFAULT 2,
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
    CONSTRAINT chunks_format CHECK (format=2),
    CONSTRAINT chunks_state CHECK (state IN ('preparing','uploading','ready','failed','deleting','deleted')),
    CONSTRAINT chunks_encoding CHECK (current_encoding_id>0 OR state='preparing')
) STRICT;

CREATE TABLE objects (
    bucket_id BLOB CHECK (bucket_id IS NULL OR length(bucket_id)=16) NOT NULL,
    object_key BLOB NOT NULL,
    stream_id INTEGER,
    write_token INTEGER NOT NULL,
    catalog_size INTEGER,
    catalog_modified INTEGER,
    catalog_type TEXT,
    catalog_category TEXT,
    catalog_public INTEGER CHECK (catalog_public IN (0,1)),
    PRIMARY KEY (bucket_id,object_key),
    CONSTRAINT objects_bucket FOREIGN KEY (bucket_id) REFERENCES buckets(id),
    CONSTRAINT objects_stream FOREIGN KEY (stream_id) REFERENCES streams(id),
    CONSTRAINT objects_key CHECK (length(CAST(object_key AS BLOB)) BETWEEN 1 AND 1024)
) STRICT, WITHOUT ROWID;

CREATE TABLE pack_maintenance (
    stream_id INTEGER NOT NULL PRIMARY KEY,
    progress_cursor INTEGER NOT NULL DEFAULT 0,
    generation INTEGER NOT NULL DEFAULT 1,
    next_check_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec')*1000000 AS INTEGER)),
    updated_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec')*1000000 AS INTEGER)),
    reason TEXT NOT NULL DEFAULT 'pack',
    CONSTRAINT pack_maintenance_stream FOREIGN KEY (stream_id) REFERENCES streams(id) ON DELETE CASCADE,
    CONSTRAINT pack_maintenance_reason CHECK (reason IN ('pack','repack')),
    CONSTRAINT pack_maintenance_progress CHECK (progress_cursor>=0 AND generation>0)
) STRICT, WITHOUT ROWID;

CREATE TABLE raw_sources (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    location_id BLOB NOT NULL UNIQUE CHECK(length(location_id)=16),
    storage_id BLOB NOT NULL REFERENCES storages(id),
    owner_stream INTEGER REFERENCES streams(id) ON DELETE SET NULL,
    medium TEXT NOT NULL DEFAULT 'local' CHECK (medium IN ('local','memory','shared_s3')),
    durability TEXT NOT NULL DEFAULT 'durable' CHECK (durability IN ('durable','buffered','volatile')),
    boot_id BLOB CHECK (boot_id IS NULL OR length(boot_id)=16),
    state TEXT NOT NULL DEFAULT 'writing' CHECK (state IN ('writing','ready','unavailable','deleting')),
    size INTEGER NOT NULL DEFAULT 0 CHECK (size>=0),
    digest BLOB CHECK (digest IS NULL OR length(digest)=16),
    content_hash BLOB CHECK (content_hash IS NULL OR length(content_hash)=32),
    upload_id TEXT,
    revision INTEGER NOT NULL DEFAULT 1 CHECK (revision>0),
    profile_revision INTEGER NOT NULL CHECK (profile_revision>0),
    quarantined_at INTEGER,
    quarantine_code TEXT,
    created_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec')*1000000 AS INTEGER)),
    unreferenced_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec')*1000000 AS INTEGER)),
    CHECK (state<>'ready' OR digest IS NOT NULL),
    CHECK ((medium='memory')=(boot_id IS NOT NULL))
) STRICT;

CREATE TABLE uploads (
    id BLOB CHECK (id IS NULL OR length(id)=16) NOT NULL PRIMARY KEY,
    bucket_id BLOB CHECK (bucket_id IS NULL OR length(bucket_id)=16) NOT NULL,
    object_key BLOB NOT NULL,
    access_key TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'active',
    metadata TEXT CHECK (json_valid(metadata)) NOT NULL DEFAULT (JSON_OBJECT()),
    public_read INTEGER CHECK (public_read IN (0,1)) NOT NULL DEFAULT FALSE,
    checksum_algorithm TEXT,
    checksum_type TEXT,
    manifest_hash TEXT,
    result TEXT CHECK (json_valid(result)),
    output_stream INTEGER,
    created_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec')*1000000 AS INTEGER)),
    touched_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec')*1000000 AS INTEGER)),
    durable_at INTEGER,
    CONSTRAINT uploads_bucket FOREIGN KEY (bucket_id) REFERENCES buckets(id),
    CONSTRAINT uploads_output FOREIGN KEY (output_stream) REFERENCES streams(id),
    CONSTRAINT uploads_key CHECK (length(CAST(object_key AS BLOB)) BETWEEN 1 AND 1024),
    CONSTRAINT uploads_state CHECK (state IN ('active','completing','completed','aborted'))
) STRICT, WITHOUT ROWID;

CREATE TABLE cache_pins (
    chunk_id INTEGER NOT NULL,
    pin_type TEXT NOT NULL,
    owner_id INTEGER NOT NULL,
    created_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec')*1000000 AS INTEGER)),
    PRIMARY KEY (chunk_id,pin_type,owner_id),
    CONSTRAINT cache_pins_chunk FOREIGN KEY (chunk_id) REFERENCES chunks(id),
    CONSTRAINT cache_pins_type CHECK (pin_type IN ('upload','pack'))
) STRICT, WITHOUT ROWID;

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
    stored_at INTEGER, quarantined_at INTEGER, quarantine_code TEXT CONSTRAINT encodings_quarantine CHECK ((quarantined_at IS NULL) = (quarantine_code IS NULL)), quarantine_revision INTEGER NOT NULL DEFAULT 0 CONSTRAINT encodings_quarantine_revision CHECK (quarantine_revision >= 0), owner_stream INTEGER
    REFERENCES streams(id) ON DELETE RESTRICT
    CONSTRAINT encodings_owner_exclusive CHECK (owner_task IS NULL OR owner_stream IS NULL), recovery_pending INTEGER NOT NULL DEFAULT FALSE CHECK (recovery_pending IN (0,1)), nonce_id INTEGER,
    CONSTRAINT encodings_chunk FOREIGN KEY (storage_id,chunk_id) REFERENCES chunks(storage_id,id) ON DELETE CASCADE,
    CONSTRAINT encodings_owner FOREIGN KEY (owner_task) REFERENCES tasks(id) ON DELETE SET NULL,
    CONSTRAINT encodings_key FOREIGN KEY (block_key_id) REFERENCES managed_keys(id),
    CONSTRAINT encodings_algorithm CHECK (encryption_algorithm IN ('none','aes-256-gcm','chacha20-poly1305')),
    CONSTRAINT encodings_encryption CHECK ((encryption_algorithm='none' AND block_key_id IS NULL) OR (encryption_algorithm<>'none' AND block_key_id IS NOT NULL)),
    CONSTRAINT encodings_size CHECK (standalone_size IS NULL OR standalone_size BETWEEN 1 AND 4194320),
    CONSTRAINT encodings_nonce CHECK (nonce IS NULL OR length(CAST(nonce AS BLOB))=12),
    CONSTRAINT encodings_state CHECK (state IN ('uploading','ready','retired','deleting','deleted'))
) STRICT;

CREATE TABLE multipart_files (
    id BLOB NOT NULL PRIMARY KEY CHECK (length(id)=16),
    upload_id BLOB REFERENCES uploads(id) ON DELETE SET NULL,
    storage_id BLOB NOT NULL REFERENCES storages(id),
    source_id INTEGER UNIQUE REFERENCES raw_sources(id),
    reclaim_pending INTEGER NOT NULL DEFAULT 0 CHECK (reclaim_pending IN (0,1)),
    state TEXT NOT NULL DEFAULT 'open' CHECK (state IN ('open','sealed','deleting'))
) STRICT, WITHOUT ROWID;

CREATE TABLE parts (
    upload_id BLOB CHECK (upload_id IS NULL OR length(upload_id)=16) NOT NULL,
    part_number INTEGER NOT NULL,
    stream_id INTEGER,
    write_token INTEGER NOT NULL,
    quota_size INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (upload_id,part_number),
    CONSTRAINT parts_upload FOREIGN KEY (upload_id) REFERENCES uploads(id) ON DELETE CASCADE,
    CONSTRAINT parts_stream FOREIGN KEY (stream_id) REFERENCES streams(id),
    CONSTRAINT parts_number CHECK (part_number BETWEEN 1 AND 10000),
    CONSTRAINT parts_quota_size CHECK (quota_size>=0)
) STRICT, WITHOUT ROWID;

CREATE TABLE pending_uploads (
    chunk_id INTEGER NOT NULL PRIMARY KEY,
    stream_id INTEGER,
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

CREATE TABLE quota_reservations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    bucket_id BLOB CHECK (bucket_id IS NULL OR length(bucket_id)=16) NOT NULL,
    object_key BLOB NOT NULL,
    stream_id INTEGER UNIQUE,
    upload_id BLOB CHECK (upload_id IS NULL OR length(upload_id)=16) UNIQUE,
    result_stream_id INTEGER UNIQUE,
    closed INTEGER CHECK (closed IN (0,1)) NOT NULL DEFAULT FALSE,
    replaced_stream_id INTEGER,
    credit_bytes INTEGER NOT NULL DEFAULT 0,
    logical_bytes INTEGER NOT NULL DEFAULT 0,
    inflight_bytes INTEGER NOT NULL DEFAULT 0,
    CONSTRAINT reservations_bucket FOREIGN KEY (bucket_id) REFERENCES buckets(id),
    CONSTRAINT reservations_stream FOREIGN KEY (stream_id) REFERENCES streams(id),
    CONSTRAINT reservations_upload FOREIGN KEY (upload_id) REFERENCES uploads(id),
    CONSTRAINT reservations_result FOREIGN KEY (result_stream_id) REFERENCES streams(id) ON DELETE SET NULL,
    CONSTRAINT reservations_owner CHECK ((stream_id IS NULL)<>(upload_id IS NULL)),
    CONSTRAINT reservations_bytes CHECK (credit_bytes>=0 AND logical_bytes>=0 AND inflight_bytes>=0)
) STRICT;

CREATE TABLE raw_processing (
    stream_id INTEGER PRIMARY KEY REFERENCES streams(id) ON DELETE CASCADE,
    source_id INTEGER NOT NULL REFERENCES raw_sources(id) ON DELETE CASCADE,
    payload_revision INTEGER NOT NULL,
    content_hash BLOB CHECK (content_hash IS NULL OR length(content_hash)=32),
    source_revision INTEGER,
    token BLOB,
    lease_expires_at INTEGER,
    next_attempt_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec')*1000000 AS INTEGER)),
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT
) STRICT;

CREATE TABLE raw_ranges (
    stream_id INTEGER NOT NULL REFERENCES streams(id) ON DELETE CASCADE,
    offset_bytes INTEGER NOT NULL CHECK (offset_bytes>=0),
    length INTEGER NOT NULL CHECK (length>=0),
    source_id INTEGER NOT NULL REFERENCES raw_sources(id),
    source_offset INTEGER NOT NULL DEFAULT 0 CHECK (source_offset>=0),
    PRIMARY KEY (stream_id,offset_bytes)
) STRICT, WITHOUT ROWID;

CREATE TABLE web_uploads (
    upload_id BLOB CHECK (upload_id IS NULL OR length(upload_id)=16) NOT NULL PRIMARY KEY,
    user_id BLOB CHECK (user_id IS NULL OR length(user_id)=16),
    client_id BLOB CHECK (client_id IS NULL OR length(client_id)=16) NOT NULL,
    request_hash BLOB CHECK (request_hash IS NULL OR length(request_hash)=32) NOT NULL,
    file_name TEXT NOT NULL,
    expected_size INTEGER NOT NULL,
    part_size INTEGER NOT NULL,
    modified_at INTEGER,
    expected_stream_id INTEGER,
    CONSTRAINT web_uploads_upload FOREIGN KEY (upload_id) REFERENCES uploads(id) ON DELETE CASCADE,
    CONSTRAINT web_uploads_user FOREIGN KEY (user_id) REFERENCES web_users(id) ON DELETE SET NULL,
    CONSTRAINT web_uploads_size CHECK (expected_size BETWEEN 0 AND 53687091200000),
    CONSTRAINT web_uploads_part_size CHECK (part_size BETWEEN 5242880 AND 5368709120),
    CONSTRAINT web_uploads_name CHECK (length(CAST(file_name AS BLOB)) BETWEEN 1 AND 1024),
    CONSTRAINT web_uploads_modified CHECK (modified_at IS NULL OR modified_at>=0)
) STRICT, WITHOUT ROWID;

CREATE TABLE fragments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    location_id BLOB NOT NULL UNIQUE CHECK(length(location_id)=16),
    owner_stream INTEGER,
    sealed INTEGER CHECK (sealed IN (0,1)) NOT NULL DEFAULT FALSE,
    size INTEGER NOT NULL,
    hash BLOB CHECK (hash IS NULL OR length(hash)=32) NOT NULL,
    created_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec')*1000000 AS INTEGER)), file_id BLOB REFERENCES multipart_files(id), file_offset INTEGER CHECK ((file_id IS NULL)=(file_offset IS NULL) AND (file_offset IS NULL OR file_offset>=0)),
    CONSTRAINT fragments_stream FOREIGN KEY (owner_stream) REFERENCES streams(id) ON DELETE SET NULL,
    CONSTRAINT fragments_size CHECK (size BETWEEN 1 AND 4194304)
) STRICT;

CREATE TABLE quota_writes (
    stream_id INTEGER NOT NULL PRIMARY KEY,
    reservation_id INTEGER NOT NULL,
    allocated_bytes INTEGER NOT NULL DEFAULT 0,
    contribution_bytes INTEGER NOT NULL DEFAULT 0,
    CONSTRAINT quota_writes_stream FOREIGN KEY (stream_id) REFERENCES streams(id),
    CONSTRAINT quota_writes_reservation FOREIGN KEY (reservation_id) REFERENCES quota_reservations(id) ON DELETE CASCADE,
    CONSTRAINT quota_writes_bytes CHECK (allocated_bytes>=0 AND contribution_bytes>=0)
) STRICT, WITHOUT ROWID;

CREATE TABLE extents (
    stream_id INTEGER NOT NULL,
    offset_bytes INTEGER NOT NULL,
    length INTEGER NOT NULL,
    chunk_id INTEGER,
    fragment_id INTEGER,
    source_offset INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (stream_id,offset_bytes),
    CONSTRAINT extents_stream FOREIGN KEY (stream_id) REFERENCES streams(id) ON DELETE CASCADE,
    CONSTRAINT extents_chunk FOREIGN KEY (chunk_id) REFERENCES chunks(id),
    CONSTRAINT extents_fragment FOREIGN KEY (fragment_id) REFERENCES fragments(id),
    CONSTRAINT extents_source CHECK ((chunk_id IS NULL)<>(fragment_id IS NULL)),
    CONSTRAINT extents_length CHECK (length BETWEEN 1 AND 4194304),
    CONSTRAINT extents_offset CHECK (offset_bytes>=0),
    CONSTRAINT extents_source_offset CHECK (source_offset>=0 AND source_offset+length<=4194304)
) STRICT, WITHOUT ROWID;

CREATE INDEX cache_pins_owner ON cache_pins(owner_id,chunk_id);
CREATE INDEX chunk_encodings_block_key_id_fk ON chunk_encodings(block_key_id);
CREATE INDEX chunk_encodings_owner_task_fk ON chunk_encodings(owner_task);
CREATE INDEX chunks_block_key_id_fk ON chunks(block_key_id);
CREATE UNIQUE INDEX chunks_dedup ON chunks(storage_id,hash,raw_size) WHERE state IN ('preparing','uploading','ready');
CREATE INDEX chunks_deleted ON chunks(state,deleted_at,id);
CREATE INDEX chunks_gc ON chunks(storage_id,state,unreferenced_at,id);
CREATE INDEX chunks_owner_stream_fk ON chunks(owner_stream);
CREATE INDEX chunks_pack_id ON chunks(pack_id);
CREATE INDEX chunks_repack ON chunks(repack_after,id);
CREATE UNIQUE INDEX chunks_storage_id ON chunks(storage_id,id);
CREATE INDEX encodings_chunk_id ON chunk_encodings(chunk_id);
CREATE INDEX encodings_gc ON chunk_encodings(state,unreferenced_at,encoding_id);
CREATE INDEX encodings_owner_stream ON chunk_encodings(owner_stream) WHERE owner_stream IS NOT NULL;
CREATE INDEX encodings_quarantined ON chunk_encodings (storage_id,encoding_id) WHERE quarantined_at IS NOT NULL;
CREATE UNIQUE INDEX encodings_ready ON chunk_encodings(chunk_id) WHERE state='ready';
CREATE INDEX encodings_recovery ON chunk_encodings(storage_id,unreferenced_at,encoding_id) WHERE recovery_pending;
CREATE INDEX extents_chunk_id_fk ON extents(chunk_id);
CREATE INDEX extents_fragment_id_fk ON extents(fragment_id);
CREATE INDEX fragments_cleanup ON fragments(created_at,id);
CREATE INDEX fragments_file ON fragments(file_id,file_offset) WHERE file_id IS NOT NULL;
CREATE INDEX fragments_owner_stream_fk ON fragments(owner_stream);
CREATE INDEX integrity_issues_code ON integrity_issues(task_id,code,id);
CREATE INDEX integrity_issues_page ON integrity_issues(task_id,id);
CREATE UNIQUE INDEX integrity_issues_subject ON integrity_issues(task_id,subject,code);
CREATE INDEX multipart_files_reclaim ON multipart_files(storage_id,id) WHERE reclaim_pending;
CREATE INDEX multipart_files_upload ON multipart_files(upload_id,state);
CREATE INDEX objects_catalog_modified ON objects(bucket_id,catalog_modified,object_key);
CREATE INDEX objects_catalog_size ON objects(bucket_id,catalog_size,object_key);
CREATE INDEX objects_stream_id_fk ON objects(stream_id);
CREATE INDEX pack_maintenance_due ON pack_maintenance(reason,next_check_at,stream_id);
CREATE INDEX parts_stream_id_fk ON parts(stream_id);
CREATE INDEX pending_cache_key ON pending_uploads(cache_key);
CREATE INDEX pending_due ON pending_uploads(owner_task,next_retry_at,chunk_id);
CREATE INDEX pending_stream_offset ON pending_uploads(stream_id,offset_bytes);
CREATE INDEX pending_uploads_source_pack_id_fk ON pending_uploads(source_pack_id);
CREATE INDEX quota_writes_reservation_id_fk ON quota_writes(reservation_id);
CREATE INDEX raw_processing_due ON raw_processing(next_attempt_at,stream_id);
CREATE INDEX raw_ranges_source ON raw_ranges(source_id);
CREATE INDEX raw_sources_cleanup ON raw_sources(storage_id,state,unreferenced_at,id);
CREATE UNIQUE INDEX raw_sources_content ON raw_sources(storage_id,content_hash,size);
CREATE INDEX raw_sources_owner ON raw_sources(owner_stream) WHERE owner_stream IS NOT NULL;
CREATE INDEX reservations_replaced ON quota_reservations(bucket_id,object_key,replaced_stream_id);
CREATE INDEX streams_cleanup ON streams(state,touched_at,id);
CREATE INDEX streams_owner_task_id_fk ON streams(owner_task_id);
CREATE INDEX streams_writing ON streams(bucket_id,object_key,state);
CREATE INDEX uploads_cleanup ON uploads(state,touched_at,id);
CREATE INDEX uploads_listing ON uploads(bucket_id,object_key,id);
CREATE INDEX uploads_output_stream_fk ON uploads(output_stream);
CREATE UNIQUE INDEX web_uploads_client ON web_uploads(user_id,client_id);
CREATE TRIGGER chunk_encodings_nonce_insert BEFORE INSERT ON chunk_encodings WHEN NOT ((NEW.encryption_algorithm='none' AND NEW.nonce_id IS NULL AND NEW.nonce IS NULL AND NEW.block_key_id IS NULL) OR (NEW.encryption_algorithm<>'none' AND NEW.block_key_id IS NOT NULL AND ((NEW.nonce IS NULL AND NEW.nonce_id IS NULL AND NEW.state<>'ready') OR (NEW.nonce IS NOT NULL AND NEW.nonce_id IS NOT NULL AND NEW.nonce_id>0)))) BEGIN SELECT RAISE(ABORT,'invalid nonce metadata'); END;
CREATE TRIGGER chunk_encodings_nonce_update BEFORE UPDATE ON chunk_encodings WHEN NOT ((NEW.encryption_algorithm='none' AND NEW.nonce_id IS NULL AND NEW.nonce IS NULL AND NEW.block_key_id IS NULL) OR (NEW.encryption_algorithm<>'none' AND NEW.block_key_id IS NOT NULL AND ((NEW.nonce IS NULL AND NEW.nonce_id IS NULL AND NEW.state<>'ready') OR (NEW.nonce IS NOT NULL AND NEW.nonce_id IS NOT NULL AND NEW.nonce_id>0)))) BEGIN SELECT RAISE(ABORT,'invalid nonce metadata'); END;

INSERT INTO integrity_issues SELECT * FROM mokyu_identity_issues;
DROP TABLE mokyu_identity_issues;

UPDATE mokyu_meta SET schema_version=26;
