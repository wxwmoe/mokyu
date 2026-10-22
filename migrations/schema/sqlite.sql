-- Current sqlite schema, matching migration 0026.
-- The initializer records schema_version and migration checksums after this script succeeds.

CREATE TABLE access_state (
    id INTEGER NOT NULL PRIMARY KEY,
    coverage_since INTEGER,
    flushed_at INTEGER,
    CONSTRAINT access_state_singleton CHECK (id=1)
) STRICT, WITHOUT ROWID;

CREATE TABLE api_tokens (
    id BLOB NOT NULL PRIMARY KEY CHECK(length(id)=16),
    user_id BLOB NOT NULL REFERENCES web_users(id) ON DELETE CASCADE,
    label TEXT NOT NULL CHECK(octet_length(label) BETWEEN 1 AND 128),
    token_hash BLOB NOT NULL UNIQUE CHECK(length(token_hash)=32),
    prefix TEXT NOT NULL,
    system_access INTEGER NOT NULL DEFAULT 0 CHECK(system_access IN (0,1)),
    auth_revision INTEGER NOT NULL,
    authorization_revision INTEGER NOT NULL DEFAULT 0,
    created_at INTEGER NOT NULL DEFAULT(CAST(unixepoch('subsec')*1000000 AS INTEGER)),
    expires_at INTEGER,
    last_used_at INTEGER,
    revoked_at INTEGER
) STRICT, WITHOUT ROWID;

CREATE TABLE "audit_events" (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at INTEGER NOT NULL DEFAULT(CAST(unixepoch('subsec')*1000000 AS INTEGER)),
    finished_at INTEGER,
    actor_id BLOB,
    actor_label TEXT NOT NULL,
    token_id BLOB,
    source TEXT NOT NULL CHECK(source IN ('web','token','cli','s3')),
    action TEXT NOT NULL,
    target TEXT NOT NULL DEFAULT '',
    project_id BLOB,
    bucket_id BLOB,
    outcome TEXT NOT NULL DEFAULT 'unknown' CHECK(outcome IN ('unknown','succeeded','failed','partial')),
    request_id TEXT,
    http_status INTEGER,
    detail TEXT NOT NULL DEFAULT '{}' CHECK(json_valid(detail))
) STRICT;

CREATE TABLE bucket_domains (
    host TEXT NOT NULL PRIMARY KEY,
    bucket_id BLOB CHECK (bucket_id IS NULL OR length(bucket_id)=16) NOT NULL,
    CONSTRAINT bucket_domains_bucket FOREIGN KEY (bucket_id) REFERENCES buckets(id) ON DELETE CASCADE
) STRICT, WITHOUT ROWID;

CREATE TABLE buckets (
    id BLOB CHECK (id IS NULL OR length(id)=16) NOT NULL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    state TEXT NOT NULL DEFAULT 'active',
    cors TEXT CHECK (json_valid(cors)) NOT NULL DEFAULT ('[]'),
    website_enabled INTEGER CHECK (website_enabled IN (0,1)) NOT NULL DEFAULT FALSE,
    index_document TEXT NOT NULL DEFAULT ('index.html'),
    error_document TEXT NOT NULL DEFAULT ('404.html'),
    created_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec')*1000000 AS INTEGER)),
    project_id BLOB CHECK (project_id IS NULL OR length(project_id)=16) NOT NULL,
    settings_revision INTEGER NOT NULL DEFAULT 0,
    uploads_paused INTEGER CHECK (uploads_paused IN (0,1)) NOT NULL DEFAULT FALSE,
    public_base_url TEXT NOT NULL DEFAULT (''),
    storage_id BLOB CHECK (storage_id IS NULL OR length(storage_id)=16) NOT NULL,
    UNIQUE (id,project_id),
    CONSTRAINT buckets_state CHECK (state IN ('active','purging')),
    CONSTRAINT buckets_project FOREIGN KEY (project_id) REFERENCES projects(id),
    CONSTRAINT buckets_storage FOREIGN KEY (storage_id) REFERENCES storages(id)
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

CREATE TABLE chunk_access_stats (
    chunk_id INTEGER NOT NULL PRIMARY KEY,
    read_count INTEGER NOT NULL DEFAULT 0,
    range_read_count INTEGER NOT NULL DEFAULT 0,
    read_bytes INTEGER NOT NULL DEFAULT 0,
    last_read_at INTEGER NOT NULL,
    CONSTRAINT chunk_access_stats_chunk FOREIGN KEY (chunk_id) REFERENCES chunks(id) ON DELETE CASCADE,
    CONSTRAINT chunk_access_stats_counts CHECK (read_count>=0 AND range_read_count>=0 AND read_bytes>=0)
) STRICT, WITHOUT ROWID;

CREATE TABLE chunk_access_windows (
    chunk_id INTEGER NOT NULL,
    window_start INTEGER NOT NULL,
    read_count INTEGER NOT NULL DEFAULT 0,
    range_read_count INTEGER NOT NULL DEFAULT 0,
    read_bytes INTEGER NOT NULL DEFAULT 0,
    range_origin_reads INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (chunk_id,window_start),
    CONSTRAINT chunk_access_source FOREIGN KEY (chunk_id) REFERENCES chunks(id) ON DELETE CASCADE
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
    CONSTRAINT encodings_owner_exclusive CHECK (owner_task IS NULL OR owner_stream IS NULL), recovery_pending INTEGER NOT NULL DEFAULT FALSE CHECK (recovery_pending IN (0,1)),
    CONSTRAINT encodings_chunk FOREIGN KEY (storage_id,chunk_id) REFERENCES chunks(storage_id,id) ON DELETE CASCADE,
    CONSTRAINT encodings_owner FOREIGN KEY (owner_task) REFERENCES tasks(id) ON DELETE SET NULL,
    CONSTRAINT encodings_key FOREIGN KEY (block_key_id) REFERENCES managed_keys(id),
    CONSTRAINT encodings_algorithm CHECK (encryption_algorithm IN ('none','aes-256-gcm','chacha20-poly1305')),
    CONSTRAINT encodings_encryption CHECK ((encryption_algorithm='none' AND block_key_id IS NULL) OR (encryption_algorithm<>'none' AND block_key_id IS NOT NULL)),
    CONSTRAINT encodings_size CHECK (standalone_size IS NULL OR standalone_size BETWEEN 1 AND 4194320),
    CONSTRAINT encodings_nonce CHECK (nonce IS NULL OR length(CAST(nonce AS BLOB))=12),
    CONSTRAINT encodings_state CHECK (state IN ('uploading','ready','retired','deleting','deleted'))
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

CREATE TABLE fragments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    location_id BLOB NOT NULL UNIQUE CHECK(length(location_id)=16),
    owner_stream INTEGER,
    sealed INTEGER CHECK (sealed IN (0,1)) NOT NULL DEFAULT FALSE,
    size INTEGER NOT NULL,
    hash BLOB CHECK (hash IS NULL OR length(hash)=32) NOT NULL,
    created_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec')*1000000 AS INTEGER)),
    CONSTRAINT fragments_stream FOREIGN KEY (owner_stream) REFERENCES streams(id) ON DELETE SET NULL,
    CONSTRAINT fragments_size CHECK (size BETWEEN 1 AND 4194304)
) STRICT;

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

CREATE TABLE integrity_packs (
    task_id BLOB CHECK (task_id IS NULL OR length(task_id)=16) NOT NULL,
    pack_id INTEGER NOT NULL,
    error_code TEXT,
    PRIMARY KEY (task_id,pack_id),
    CONSTRAINT integrity_packs_task FOREIGN KEY (task_id) REFERENCES tasks(id) ON DELETE CASCADE,
    CONSTRAINT integrity_packs_pack FOREIGN KEY (pack_id) REFERENCES packs(id) ON DELETE CASCADE
) STRICT, WITHOUT ROWID;

CREATE TABLE maintenance_controls (
    policy TEXT NOT NULL PRIMARY KEY,
    paused INTEGER CHECK (paused IN (0,1)) NOT NULL DEFAULT FALSE,
    last_scheduled_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec')*1000000 AS INTEGER)),
    updated_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec')*1000000 AS INTEGER)),
    CONSTRAINT maintenance_controls_policy CHECK (policy IN ('pack','reuse','reclaim','repack','range','gc','cleanup'))
) STRICT, WITHOUT ROWID;

CREATE TABLE maintenance_previews (
    id BLOB CHECK (id IS NULL OR length(id)=16) NOT NULL PRIMARY KEY,
    actor_id BLOB CHECK (actor_id IS NULL OR length(actor_id)=16) NOT NULL,
    token_id BLOB CHECK (token_id IS NULL OR length(token_id)=16),
    action TEXT NOT NULL,
    parameters TEXT CHECK (json_valid(parameters)) NOT NULL,
    fingerprint TEXT NOT NULL,
    expires_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec')*1000000 AS INTEGER)+600000000),
    task_id BLOB CHECK (task_id IS NULL OR length(task_id)=16),
    CONSTRAINT maintenance_previews_actor FOREIGN KEY (actor_id) REFERENCES web_users(id) ON DELETE CASCADE,
    CONSTRAINT maintenance_previews_token FOREIGN KEY (token_id) REFERENCES api_tokens(id) ON DELETE CASCADE,
    CONSTRAINT maintenance_previews_task FOREIGN KEY (task_id) REFERENCES tasks(id) ON DELETE SET NULL,
    CONSTRAINT maintenance_previews_action CHECK (action IN ('unpack','sweep','quarantine','recheck'))
) STRICT, WITHOUT ROWID;

CREATE TABLE managed_keys (
    id BLOB CHECK (id IS NULL OR length(id)=16) NOT NULL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    purpose TEXT NOT NULL,
    key_algorithm TEXT NOT NULL DEFAULT 'raw',
    secret_encrypted BLOB,
    fingerprint BLOB CHECK (fingerprint IS NULL OR length(fingerprint)=32) NOT NULL,
    created_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec')*1000000 AS INTEGER)),
    revoked_at INTEGER,
    CONSTRAINT managed_keys_name CHECK (length(CAST(name AS BLOB)) BETWEEN 1 AND 128),
    CONSTRAINT managed_keys_purpose CHECK (purpose IN ('access','signing','block')),
    CONSTRAINT managed_keys_algorithm CHECK (key_algorithm IN ('raw','aes-256-gcm','chacha20-poly1305'))
) STRICT, WITHOUT ROWID;

CREATE TABLE media_operations (
    id BLOB CHECK (id IS NULL OR length(id)=16) NOT NULL PRIMARY KEY,
    user_id BLOB CHECK (user_id IS NULL OR length(user_id)=16) NOT NULL,
    token_id BLOB CHECK (token_id IS NULL OR length(token_id)=16),
    client_id BLOB CHECK (client_id IS NULL OR length(client_id)=16) NOT NULL,
    request_hash BLOB CHECK (request_hash IS NULL OR length(request_hash)=32) NOT NULL,
    result TEXT CHECK (json_valid(result)),
    created_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec')*1000000 AS INTEGER)),
    CONSTRAINT media_operations_user FOREIGN KEY (user_id) REFERENCES web_users(id) ON DELETE CASCADE
) STRICT, WITHOUT ROWID;

CREATE TABLE member_bucket_grants (
    user_id BLOB CHECK (user_id IS NULL OR length(user_id)=16) NOT NULL,
    project_id BLOB CHECK (project_id IS NULL OR length(project_id)=16) NOT NULL,
    bucket_id BLOB CHECK (bucket_id IS NULL OR length(bucket_id)=16) NOT NULL,
    actions INTEGER NOT NULL,
    PRIMARY KEY (user_id,bucket_id),
    CONSTRAINT member_grants_actions CHECK (actions BETWEEN 0 AND 127),
    CONSTRAINT member_grants_member FOREIGN KEY (user_id,project_id) REFERENCES project_members(user_id,project_id) ON DELETE CASCADE,
    CONSTRAINT member_grants_bucket FOREIGN KEY (bucket_id,project_id) REFERENCES buckets(id,project_id) ON DELETE CASCADE
) STRICT, WITHOUT ROWID;

CREATE TABLE mokyu_meta (
    id INTEGER PRIMARY KEY CHECK(id=1),
    schema_version INTEGER NOT NULL CHECK(schema_version>0),
    deployment_id BLOB NOT NULL DEFAULT(randomblob(16)) CHECK(length(deployment_id)=16),
    deployment_key_fingerprint BLOB CHECK(length(deployment_key_fingerprint)=32),
    gc_paused INTEGER NOT NULL DEFAULT 0 CHECK(gc_paused IN (0,1)),
    maintenance INTEGER NOT NULL DEFAULT 0 CHECK(maintenance IN (0,1)),
    project_management INTEGER NOT NULL DEFAULT 0 CHECK(project_management IN (0,1)),
    pack_creation_paused INTEGER NOT NULL DEFAULT 0 CHECK(pack_creation_paused IN (0,1))
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

CREATE TABLE pack_access_windows (
    pack_id INTEGER NOT NULL,
    window_start INTEGER NOT NULL,
    downloads INTEGER NOT NULL DEFAULT 0,
    downloaded_bytes INTEGER NOT NULL DEFAULT 0,
    partial_downloads INTEGER NOT NULL DEFAULT 0,
    useful_bytes INTEGER NOT NULL DEFAULT 0,
    partial_bytes INTEGER NOT NULL DEFAULT 0,
    updated_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec')*1000000 AS INTEGER)),
    PRIMARY KEY (pack_id,window_start),
    CONSTRAINT pack_access_source FOREIGN KEY (pack_id) REFERENCES packs(id) ON DELETE CASCADE
) STRICT, WITHOUT ROWID;

CREATE TABLE pack_changes (
    pack_id INTEGER NOT NULL PRIMARY KEY,
    generation INTEGER NOT NULL DEFAULT 1,
    next_check_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec')*1000000 AS INTEGER)),
    reason TEXT NOT NULL DEFAULT 'reuse',
    CONSTRAINT pack_changes_pack FOREIGN KEY (pack_id) REFERENCES packs(id) ON DELETE CASCADE,
    CONSTRAINT pack_changes_reason CHECK (reason IN ('reuse','reclaim')),
    CONSTRAINT pack_changes_generation CHECK (generation>0)
) STRICT, WITHOUT ROWID;

CREATE TABLE pack_inputs (
    task_id BLOB CHECK (task_id IS NULL OR length(task_id)=16) NOT NULL,
    chunk_id INTEGER NOT NULL,
    created_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec')*1000000 AS INTEGER)),
    PRIMARY KEY (task_id,chunk_id),
    CONSTRAINT pack_inputs_task FOREIGN KEY (task_id) REFERENCES tasks(id) ON DELETE CASCADE,
    CONSTRAINT pack_inputs_source FOREIGN KEY (chunk_id) REFERENCES chunks(id)
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

CREATE TABLE pack_member_access_windows (
    pack_id INTEGER NOT NULL,
    window_start INTEGER NOT NULL,
    chunk_id INTEGER NOT NULL,
    downloads INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (pack_id,window_start,chunk_id),
    CONSTRAINT pack_member_access_pack FOREIGN KEY (pack_id) REFERENCES packs(id) ON DELETE CASCADE,
    CONSTRAINT pack_member_access_chunk FOREIGN KEY (chunk_id) REFERENCES chunks(id) ON DELETE CASCADE
) STRICT, WITHOUT ROWID;

CREATE TABLE pack_members (
    pack_id INTEGER NOT NULL,
    ordinal INTEGER NOT NULL,
    chunk_id INTEGER NOT NULL,
    offset_bytes INTEGER NOT NULL,
    PRIMARY KEY (pack_id,ordinal),
    CONSTRAINT members_pack FOREIGN KEY (pack_id) REFERENCES packs(id) ON DELETE CASCADE,
    CONSTRAINT members_chunk FOREIGN KEY (chunk_id) REFERENCES chunks(id),
    CONSTRAINT members_ordinal CHECK (ordinal BETWEEN 0 AND 4095),
    CONSTRAINT members_offset CHECK (offset_bytes>=0)
) STRICT, WITHOUT ROWID;

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
    stored_at INTEGER, quarantined_at INTEGER, quarantine_code TEXT CONSTRAINT packs_quarantine CHECK ((quarantined_at IS NULL) = (quarantine_code IS NULL)), quarantine_revision INTEGER NOT NULL DEFAULT 0 CONSTRAINT packs_quarantine_revision CHECK (quarantine_revision >= 0),
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

CREATE TABLE project_members (
    user_id BLOB CHECK (user_id IS NULL OR length(user_id)=16) NOT NULL,
    project_id BLOB CHECK (project_id IS NULL OR length(project_id)=16) NOT NULL,
    role TEXT NOT NULL,
    scope TEXT NOT NULL,
    PRIMARY KEY (user_id,project_id),
    CONSTRAINT project_members_user FOREIGN KEY (user_id) REFERENCES web_users(id) ON DELETE CASCADE,
    CONSTRAINT project_members_project FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE CASCADE,
    CONSTRAINT project_members_role CHECK (role IN ('reader','writer','maintainer')),
    CONSTRAINT project_members_scope CHECK (scope IN ('all','selected'))
) STRICT, WITHOUT ROWID;

CREATE TABLE projects (
    id BLOB CHECK (id IS NULL OR length(id)=16) NOT NULL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    description TEXT NOT NULL DEFAULT '',
    builtin INTEGER CHECK (builtin IN (0,1)) NOT NULL DEFAULT FALSE,
    allow_bucket_create INTEGER CHECK (allow_bucket_create IN (0,1)) NOT NULL DEFAULT FALSE,
    created_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec')*1000000 AS INTEGER)),
    CONSTRAINT projects_name CHECK (length(CAST(name AS BLOB)) BETWEEN 1 AND 128),
    CONSTRAINT projects_description CHECK (length(CAST(description AS BLOB))<=2000)
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

CREATE TABLE quota_scopes (
    scope TEXT NOT NULL,
    scope_id BLOB CHECK (length(scope_id)=16) NOT NULL,
    used_bytes INTEGER NOT NULL DEFAULT 0,
    reserved_bytes INTEGER NOT NULL DEFAULT 0,
    inflight_bytes INTEGER NOT NULL DEFAULT 0,
    object_count INTEGER NOT NULL DEFAULT 0,
    bucket_count INTEGER NOT NULL DEFAULT 0,
    logical_bytes_limit INTEGER,
    inflight_bytes_limit INTEGER,
    bucket_count_limit INTEGER,
    PRIMARY KEY (scope,scope_id),
    CONSTRAINT quota_scope CHECK (scope IN ('project','bucket')),
    CONSTRAINT quota_bytes CHECK (used_bytes>=0 AND reserved_bytes>=0 AND inflight_bytes>=0),
    CONSTRAINT quota_counts CHECK (object_count>=0 AND bucket_count>=0),
    CONSTRAINT quota_limits CHECK ((logical_bytes_limit IS NULL OR logical_bytes_limit>=0) AND (inflight_bytes_limit IS NULL OR inflight_bytes_limit>=0) AND (bucket_count_limit IS NULL OR bucket_count_limit>=0))
) STRICT, WITHOUT ROWID;

CREATE TABLE quota_writes (
    stream_id INTEGER NOT NULL PRIMARY KEY,
    reservation_id INTEGER NOT NULL,
    allocated_bytes INTEGER NOT NULL DEFAULT 0,
    contribution_bytes INTEGER NOT NULL DEFAULT 0,
    CONSTRAINT quota_writes_stream FOREIGN KEY (stream_id) REFERENCES streams(id),
    CONSTRAINT quota_writes_reservation FOREIGN KEY (reservation_id) REFERENCES quota_reservations(id) ON DELETE CASCADE,
    CONSTRAINT quota_writes_bytes CHECK (allocated_bytes>=0 AND contribution_bytes>=0)
) STRICT, WITHOUT ROWID;

CREATE TABLE runtime_samples (
    sampled_at INTEGER NOT NULL PRIMARY KEY,
    data TEXT CHECK (json_valid(data)) NOT NULL
) STRICT, WITHOUT ROWID;

CREATE TABLE runtime_settings (
    section TEXT NOT NULL PRIMARY KEY,
    value TEXT NOT NULL CHECK(json_valid(value)),
    revision INTEGER NOT NULL DEFAULT 1 CHECK(revision>0),
    updated_at INTEGER NOT NULL DEFAULT(CAST(unixepoch('subsec')*1000000 AS INTEGER)),
    is_default INTEGER NOT NULL DEFAULT 1 CHECK(is_default IN (0,1))
) STRICT, WITHOUT ROWID;

CREATE TABLE s3_credential_grants (
    access_key TEXT NOT NULL,
    bucket_id BLOB CHECK (bucket_id IS NULL OR length(bucket_id)=16) NOT NULL,
    actions INTEGER NOT NULL,
    PRIMARY KEY (access_key,bucket_id),
    CONSTRAINT credential_grants_actions CHECK (actions BETWEEN 0 AND 127),
    CONSTRAINT credential_grants_credential FOREIGN KEY (access_key) REFERENCES s3_credentials(access_key_id) ON DELETE CASCADE,
    CONSTRAINT credential_grants_bucket FOREIGN KEY (bucket_id) REFERENCES buckets(id) ON DELETE CASCADE
) STRICT, WITHOUT ROWID;

CREATE TABLE s3_credentials (
    access_key_id TEXT NOT NULL PRIMARY KEY,
    secret_encrypted BLOB NOT NULL,
    enabled INTEGER CHECK (enabled IN (0,1)) NOT NULL DEFAULT TRUE,
    created_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec')*1000000 AS INTEGER)),
    project_id BLOB CHECK (project_id IS NULL OR length(project_id)=16) NOT NULL,
    authorization_revision INTEGER NOT NULL DEFAULT 0,
    label TEXT NOT NULL DEFAULT '',
    expires_at INTEGER,
    last_used_at INTEGER,
    created_by BLOB CHECK (created_by IS NULL OR length(created_by)=16),
    CONSTRAINT s3_credentials_label CHECK (length(CAST(label AS BLOB))<=128),
    CONSTRAINT s3_credentials_project FOREIGN KEY (project_id) REFERENCES projects(id),
    CONSTRAINT s3_credentials_creator FOREIGN KEY (created_by) REFERENCES web_users(id) ON DELETE SET NULL
) STRICT, WITHOUT ROWID;

CREATE TABLE session_refresh_tokens (
    id BLOB NOT NULL PRIMARY KEY CHECK(length(id)=16),
    session_id BLOB NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
    family_id BLOB NOT NULL CHECK(length(family_id)=16),
    token_hash BLOB NOT NULL UNIQUE CHECK(length(token_hash)=32),
    created_at INTEGER NOT NULL DEFAULT(CAST(unixepoch('subsec')*1000000 AS INTEGER)),
    last_used_at INTEGER NOT NULL DEFAULT(CAST(unixepoch('subsec')*1000000 AS INTEGER)),
    expires_at INTEGER NOT NULL,
    revoked_at INTEGER,
    replaced_by BLOB REFERENCES session_refresh_tokens(id) ON DELETE SET NULL
) STRICT, WITHOUT ROWID;

CREATE TABLE sessions (
    id BLOB NOT NULL PRIMARY KEY CHECK(length(id)=16),
    token_hash BLOB NOT NULL UNIQUE CHECK(length(token_hash)=32),
    user_id BLOB NOT NULL REFERENCES web_users(id) ON DELETE CASCADE,
    csrf_hash BLOB NOT NULL CHECK(length(csrf_hash)=32),
    expires_at INTEGER NOT NULL,
    created_at INTEGER NOT NULL DEFAULT(CAST(unixepoch('subsec')*1000000 AS INTEGER)),
    last_seen_at INTEGER NOT NULL DEFAULT(CAST(unixepoch('subsec')*1000000 AS INTEGER)),
    reauthenticated_at INTEGER NOT NULL DEFAULT(CAST(unixepoch('subsec')*1000000 AS INTEGER)),
    auth_revision INTEGER NOT NULL DEFAULT 0,
    user_agent TEXT NOT NULL DEFAULT '' CHECK(octet_length(user_agent)<=512)
) STRICT, WITHOUT ROWID;

CREATE TABLE setup_tokens (
    id INTEGER PRIMARY KEY CHECK(id=1),
    token_hash BLOB NOT NULL CHECK(length(token_hash)=32),
    created_at INTEGER NOT NULL DEFAULT(CAST(unixepoch('subsec')*1000000 AS INTEGER)),
    expires_at INTEGER NOT NULL
) STRICT;

CREATE TABLE storage_profiles (
    storage_id BLOB CHECK (storage_id IS NULL OR length(storage_id)=16) NOT NULL PRIMARY KEY,
    revision INTEGER NOT NULL DEFAULT 1,
    connect_timeout TEXT NOT NULL DEFAULT '5s',
    request_timeout TEXT NOT NULL DEFAULT '30s',
    max_retries INTEGER NOT NULL DEFAULT 3,
    retry_timeout TEXT NOT NULL DEFAULT '120s',
    priority_aging TEXT NOT NULL DEFAULT '30s',
    min_storage_duration TEXT NOT NULL DEFAULT '0s',
    unreferenced_grace TEXT NOT NULL DEFAULT '48h',
    deleted_chunk_retention TEXT NOT NULL DEFAULT '7d',
    upload_retention TEXT NOT NULL DEFAULT '24h',
    cache_enabled INTEGER CHECK (cache_enabled IN (0,1)) NOT NULL DEFAULT FALSE,
    cache_soft_limit TEXT,
    cache_entries INTEGER,
    upload_cache INTEGER CHECK (upload_cache IN (0,1)) NOT NULL DEFAULT FALSE,
    upload_cache_size TEXT,
    compression_enabled INTEGER CHECK (compression_enabled IN (0,1)) NOT NULL DEFAULT FALSE,
    compression_strategy TEXT NOT NULL DEFAULT 'always',
    compression_level INTEGER NOT NULL DEFAULT 6,
    compression_sample_level INTEGER NOT NULL DEFAULT 3,
    compression_min_savings_percent INTEGER NOT NULL DEFAULT 2,
    compression_min_savings_bytes INTEGER NOT NULL DEFAULT 256,
    pack_enabled INTEGER CHECK (pack_enabled IN (0,1)) NOT NULL DEFAULT FALSE,
    pack_max_size TEXT NOT NULL DEFAULT '32MiB',
    pack_upload_flush_timeout TEXT NOT NULL DEFAULT '60s',
    pack_interval TEXT NOT NULL DEFAULT '1h',
    pack_reuse_interval TEXT NOT NULL DEFAULT '5m',
    pack_reclaim_interval TEXT NOT NULL DEFAULT '30m',
    pack_repack_interval TEXT NOT NULL DEFAULT '24h',
    pack_repack_cooldown TEXT NOT NULL DEFAULT '1h',
    pack_reclaim_min_savings_bytes TEXT NOT NULL DEFAULT '4MiB',
    pack_range_enabled INTEGER CHECK (pack_range_enabled IN (0,1)) NOT NULL DEFAULT FALSE,
    pack_range_interval TEXT NOT NULL DEFAULT '15m',
    pack_range_window TEXT NOT NULL DEFAULT '24h',
    pack_range_min_downloads INTEGER NOT NULL DEFAULT 8,
    pack_range_min_savings_percent INTEGER NOT NULL DEFAULT 50,
    pack_range_min_savings_bytes TEXT NOT NULL DEFAULT '64MiB',
    pack_range_repack_after TEXT NOT NULL DEFAULT '30d',
    pack_range_repack_window TEXT NOT NULL DEFAULT '7d',
    pack_range_repack_retry_interval TEXT NOT NULL DEFAULT '7d',
    pack_concurrency INTEGER NOT NULL DEFAULT 1,
    integrity_concurrency INTEGER NOT NULL DEFAULT 1,
    integrity_rps INTEGER,
    integrity_bandwidth TEXT,
    integrity_batch_timeout TEXT NOT NULL DEFAULT '30s',
    read_concurrency INTEGER,
    upload_concurrency INTEGER,
    control_concurrency INTEGER,
    pack_byte_budget TEXT,
    integrity_byte_budget TEXT,
    maintenance_concurrency INTEGER,
    maintenance_byte_budget TEXT,
    migration_concurrency INTEGER,
    migration_byte_budget TEXT,
    updated_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec')*1000000 AS INTEGER)), cdc_enabled INTEGER NOT NULL DEFAULT 1 CHECK (cdc_enabled IN (0,1)), deduplication_enabled INTEGER NOT NULL DEFAULT 0 CHECK (deduplication_enabled IN (0,1)), multipart_idle_timeout TEXT NOT NULL DEFAULT '24h', min_free_space TEXT, write_processing TEXT NOT NULL DEFAULT 'inline',
    CONSTRAINT storage_profiles_storage FOREIGN KEY (storage_id) REFERENCES storages(id) ON DELETE CASCADE,
    CONSTRAINT storage_profiles_revision CHECK (revision>0),
    CONSTRAINT storage_profiles_max_retries CHECK (max_retries BETWEEN 0 AND 100),
    CONSTRAINT storage_profiles_cache_entries CHECK (cache_entries IS NULL OR cache_entries>0),
    CONSTRAINT storage_profiles_compression_strategy CHECK (compression_strategy IN ('never','always','sample')),
    CONSTRAINT storage_profiles_compression_percent CHECK (compression_min_savings_percent BETWEEN 0 AND 100),
    CONSTRAINT storage_profiles_compression_bytes CHECK (compression_min_savings_bytes>=0),
    CONSTRAINT storage_profiles_pack_concurrency CHECK (pack_concurrency>0),
    CONSTRAINT storage_profiles_pack_downloads CHECK (pack_range_min_downloads>0),
    CONSTRAINT storage_profiles_pack_percent CHECK (pack_range_min_savings_percent BETWEEN 1 AND 100),
    CONSTRAINT storage_profiles_integrity_concurrency CHECK (integrity_concurrency>0),
    CONSTRAINT storage_profiles_integrity_rps CHECK (integrity_rps IS NULL OR integrity_rps>0),
    CONSTRAINT storage_profiles_read CHECK (read_concurrency IS NULL OR read_concurrency>0),
    CONSTRAINT storage_profiles_upload CHECK (upload_concurrency IS NULL OR upload_concurrency>0),
    CONSTRAINT storage_profiles_control CHECK (control_concurrency IS NULL OR control_concurrency>0),
    CONSTRAINT storage_profiles_maintenance CHECK (maintenance_concurrency IS NULL OR maintenance_concurrency>0),
    CONSTRAINT storage_profiles_migration CHECK (migration_concurrency IS NULL OR migration_concurrency>0)
) STRICT, WITHOUT ROWID;

CREATE TABLE storages (
    id BLOB CHECK (id IS NULL OR length(id)=16) NOT NULL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    type TEXT NOT NULL,
    path TEXT,
    endpoint_url TEXT,
    remote_bucket_name TEXT,
    region TEXT,
    prefix TEXT NOT NULL DEFAULT (''),
    access_key_encrypted BLOB,
    secret_key_encrypted BLOB,
    identity_marker TEXT CHECK (json_valid(identity_marker)) NOT NULL,
    enabled INTEGER CHECK (enabled IN (0,1)) NOT NULL DEFAULT TRUE,
    block_key_id BLOB CHECK (block_key_id IS NULL OR length(block_key_id)=16),
    created_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec')*1000000 AS INTEGER)),
    updated_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec')*1000000 AS INTEGER)),
    CONSTRAINT storages_name CHECK (length(CAST(name AS BLOB)) BETWEEN 1 AND 128),
    CONSTRAINT storages_type CHECK (type IN ('local','s3')),
    CONSTRAINT storages_connection CHECK (
        (type='local' AND path IS NOT NULL AND endpoint_url IS NULL AND remote_bucket_name IS NULL AND region IS NULL AND access_key_encrypted IS NULL AND secret_key_encrypted IS NULL)
        OR (type='s3' AND path IS NULL AND endpoint_url IS NOT NULL AND remote_bucket_name IS NOT NULL AND region IS NOT NULL AND access_key_encrypted IS NOT NULL AND secret_key_encrypted IS NOT NULL)
    ),
    CONSTRAINT storages_key FOREIGN KEY (block_key_id) REFERENCES managed_keys(id)
) STRICT, WITHOUT ROWID;

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

CREATE TABLE token_bucket_grants (
    token_id BLOB CHECK (token_id IS NULL OR length(token_id)=16) NOT NULL,
    bucket_id BLOB CHECK (bucket_id IS NULL OR length(bucket_id)=16) NOT NULL,
    actions INTEGER NOT NULL,
    PRIMARY KEY (token_id,bucket_id),
    CONSTRAINT token_grants_actions CHECK (actions BETWEEN 0 AND 127),
    CONSTRAINT token_grants_token FOREIGN KEY (token_id) REFERENCES api_tokens(id) ON DELETE CASCADE,
    CONSTRAINT token_grants_bucket FOREIGN KEY (bucket_id) REFERENCES buckets(id) ON DELETE CASCADE
) STRICT, WITHOUT ROWID;

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

CREATE TABLE usage_buckets (
    scope_id TEXT NOT NULL,
    bucket_id BLOB CHECK (bucket_id IS NULL OR length(bucket_id)=16) NOT NULL,
    PRIMARY KEY (scope_id,bucket_id),
    CONSTRAINT usage_buckets_scope FOREIGN KEY (scope_id) REFERENCES usage_summary(id) ON DELETE CASCADE
) STRICT, WITHOUT ROWID;

CREATE TABLE usage_history (
    scope_id TEXT NOT NULL,
    snapshot_at INTEGER NOT NULL,
    data TEXT CHECK (json_valid(data)) NOT NULL,
    PRIMARY KEY (scope_id,snapshot_at),
    CONSTRAINT usage_history_scope FOREIGN KEY (scope_id) REFERENCES usage_summary(id) ON DELETE CASCADE
) STRICT, WITHOUT ROWID;

CREATE TABLE usage_summary (
    id TEXT NOT NULL PRIMARY KEY,
    requested_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec')*1000000 AS INTEGER)),
    as_of INTEGER,
    data TEXT CHECK (json_valid(data))
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

CREATE TABLE web_users (
    id BLOB NOT NULL PRIMARY KEY CHECK(length(id)=16),
    username TEXT NOT NULL UNIQUE CHECK(octet_length(username) BETWEEN 1 AND 64),
    password_hash TEXT NOT NULL,
    enabled INTEGER NOT NULL DEFAULT 1 CHECK(enabled IN (0,1)),
    created_at INTEGER NOT NULL DEFAULT(CAST(unixepoch('subsec')*1000000 AS INTEGER)),
    display_name TEXT NOT NULL DEFAULT '' CHECK(octet_length(display_name)<=240),
    locale TEXT CHECK(locale IN ('en','zh-CN','ja')),
    theme TEXT CHECK(theme IN ('auto','light','dark')),
    avatar_email TEXT NOT NULL DEFAULT '' CHECK(octet_length(avatar_email)<=320),
    avatar_enabled INTEGER NOT NULL DEFAULT 0 CHECK(avatar_enabled IN (0,1)),
    auth_revision INTEGER NOT NULL DEFAULT 0,
    role TEXT NOT NULL DEFAULT 'member' CHECK(role IN ('admin','member')),
    authorization_revision INTEGER NOT NULL DEFAULT 0,
    must_change_password INTEGER NOT NULL DEFAULT 0 CHECK(must_change_password IN (0,1))
) STRICT, WITHOUT ROWID;

CREATE INDEX api_tokens_user ON api_tokens(user_id);

CREATE INDEX audit_actor ON audit_events(actor_id,id);

CREATE INDEX audit_created ON audit_events(created_at,id);

CREATE INDEX audit_security ON audit_events(actor_id,action,outcome,created_at);

CREATE INDEX bucket_domains_bucket_id_fk ON bucket_domains(bucket_id);

CREATE INDEX buckets_project ON buckets(project_id);

CREATE INDEX buckets_storage ON buckets(storage_id);

CREATE INDEX cache_pins_owner ON cache_pins(owner_id,chunk_id);

CREATE INDEX chunk_access_expiry ON chunk_access_windows(window_start);

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

CREATE INDEX credential_grants_bucket ON s3_credential_grants(bucket_id);

CREATE INDEX encodings_chunk_id ON chunk_encodings(chunk_id);

CREATE INDEX encodings_gc ON chunk_encodings(state,unreferenced_at,encoding_id);

CREATE UNIQUE INDEX encodings_ready ON chunk_encodings(chunk_id) WHERE state='ready';

CREATE INDEX extents_chunk_id_fk ON extents(chunk_id);

CREATE INDEX extents_fragment_id_fk ON extents(fragment_id);

CREATE INDEX fragments_cleanup ON fragments(created_at,id);

CREATE INDEX fragments_owner_stream_fk ON fragments(owner_stream);

CREATE INDEX integrity_issues_code ON integrity_issues(task_id,code,id);

CREATE INDEX integrity_issues_page ON integrity_issues(task_id,id);

CREATE UNIQUE INDEX integrity_issues_subject ON integrity_issues(task_id,subject,code);

CREATE INDEX integrity_packs_pack_id_fk ON integrity_packs(pack_id);

CREATE INDEX maintenance_previews_expiry ON maintenance_previews(expires_at);

CREATE INDEX maintenance_previews_owner ON maintenance_previews(actor_id,token_id,action,task_id);

CREATE INDEX maintenance_previews_task_id_fk ON maintenance_previews(task_id);

CREATE INDEX maintenance_previews_token_id_fk ON maintenance_previews(token_id);

CREATE INDEX media_operations_cleanup ON media_operations(created_at,id);

CREATE UNIQUE INDEX media_operations_token_client ON media_operations(user_id,token_id,client_id) WHERE token_id IS NOT NULL;

CREATE UNIQUE INDEX media_operations_user_client ON media_operations(user_id,client_id) WHERE token_id IS NULL;

CREATE INDEX media_operations_user_id_fk ON media_operations(user_id);

CREATE INDEX member_grants_bucket ON member_bucket_grants(bucket_id,project_id);

CREATE INDEX member_grants_member ON member_bucket_grants(user_id,project_id);

CREATE INDEX objects_catalog_modified ON objects(bucket_id,catalog_modified,object_key);

CREATE INDEX objects_catalog_size ON objects(bucket_id,catalog_size,object_key);

CREATE INDEX objects_stream_id_fk ON objects(stream_id);

CREATE INDEX pack_access_expiry ON pack_access_windows(window_start);

CREATE INDEX pack_changes_due ON pack_changes(reason,next_check_at,pack_id);

CREATE UNIQUE INDEX pack_inputs_chunk ON pack_inputs(chunk_id);

CREATE INDEX pack_maintenance_due ON pack_maintenance(reason,next_check_at,stream_id);

CREATE INDEX pack_member_access_expiry ON pack_member_access_windows(window_start);

CREATE INDEX pack_member_access_windows_chunk_id_fk ON pack_member_access_windows(chunk_id);

CREATE UNIQUE INDEX pack_members_chunk ON pack_members(pack_id,chunk_id);

CREATE INDEX pack_members_chunk_id_fk ON pack_members(chunk_id);

CREATE INDEX packs_block_key_id_fk ON packs(block_key_id);

CREATE INDEX packs_gc ON packs(state,unreferenced_at,id);

CREATE INDEX packs_owner_task_fk ON packs(owner_task);

CREATE UNIQUE INDEX packs_storage_id ON packs(storage_id,id);

CREATE INDEX parts_stream_id_fk ON parts(stream_id);

CREATE INDEX pending_cache_key ON pending_uploads(cache_key);

CREATE INDEX pending_due ON pending_uploads(owner_task,next_retry_at,chunk_id);

CREATE INDEX pending_stream_offset ON pending_uploads(stream_id,offset_bytes);

CREATE INDEX pending_uploads_source_pack_id_fk ON pending_uploads(source_pack_id);

CREATE INDEX project_members_project ON project_members(project_id);

CREATE INDEX quota_writes_reservation_id_fk ON quota_writes(reservation_id);

CREATE UNIQUE INDEX refresh_active ON session_refresh_tokens(session_id) WHERE revoked_at IS NULL;

CREATE INDEX refresh_expiry ON session_refresh_tokens(revoked_at,expires_at);

CREATE INDEX refresh_session ON session_refresh_tokens(session_id);

CREATE INDEX refresh_successor ON session_refresh_tokens(replaced_by) WHERE replaced_by IS NOT NULL;

CREATE INDEX reservations_replaced ON quota_reservations(bucket_id,object_key,replaced_stream_id);

CREATE INDEX s3_credentials_creator ON s3_credentials(created_by);

CREATE INDEX s3_credentials_project ON s3_credentials(project_id);

CREATE INDEX sessions_expiry ON sessions(expires_at);

CREATE INDEX sessions_user_created ON sessions(user_id,created_at DESC,id DESC);

CREATE INDEX storages_block_key ON storages(block_key_id);

CREATE INDEX streams_cleanup ON streams(state,touched_at,id);

CREATE INDEX streams_owner_task_id_fk ON streams(owner_task_id);

CREATE INDEX streams_writing ON streams(bucket_id,object_key,state);

CREATE INDEX tasks_bucket_id_fk ON tasks(bucket_id);

CREATE INDEX tasks_list ON tasks(created_at DESC,id DESC);

CREATE INDEX tasks_policy_recent ON tasks(policy,created_at DESC,id DESC);

CREATE INDEX tasks_state_list ON tasks(state,created_at DESC,id DESC);

CREATE INDEX tasks_work ON tasks(state,updated_at,id);

CREATE VIEW token_bucket_access AS
SELECT t.id AS token_id,g.bucket_id,
    g.actions & IIF(u.role='admin',127,COALESCE(a.actions,0)) AS actions
FROM api_tokens t JOIN web_users u ON u.id=t.user_id
JOIN token_bucket_grants g ON g.token_id=t.id
LEFT JOIN user_bucket_access a ON a.user_id=t.user_id AND a.bucket_id=g.bucket_id;

CREATE INDEX token_grants_bucket ON token_bucket_grants(bucket_id);

CREATE INDEX uploads_cleanup ON uploads(state,touched_at,id);

CREATE INDEX uploads_listing ON uploads(bucket_id,object_key,id);

CREATE INDEX uploads_output_stream_fk ON uploads(output_stream);

CREATE INDEX usage_history_expiry ON usage_history(snapshot_at);

CREATE INDEX usage_requested ON usage_summary(requested_at,id);

CREATE VIEW user_bucket_access AS
SELECT m.user_id,b.id AS bucket_id,
    (CASE m.role WHEN 'reader' THEN 67 WHEN 'writer' THEN 95 WHEN 'maintainer' THEN 127 ELSE 0 END)
    & IIF(m.scope='all',127,COALESCE(g.actions,0)) AS actions
FROM project_members m JOIN buckets b ON b.project_id=m.project_id
LEFT JOIN member_bucket_grants g ON g.user_id=m.user_id AND g.bucket_id=b.id;

CREATE UNIQUE INDEX web_uploads_client ON web_uploads(user_id,client_id);

INSERT INTO projects(id,name,builtin) VALUES(X'00000000000000000000000000000001','Default',1);
INSERT INTO quota_scopes(scope,scope_id) VALUES('project',X'00000000000000000000000000000001');
INSERT INTO access_state(id,coverage_since,flushed_at) VALUES(1,CAST(unixepoch('subsec')*1000000 AS INTEGER),CAST(unixepoch('subsec')*1000000 AS INTEGER));
INSERT INTO maintenance_controls(policy) VALUES('gc'),('pack'),('reuse'),('reclaim'),('repack'),('range'),('cleanup');
INSERT INTO usage_summary(id) VALUES('global');

CREATE INDEX encodings_quarantined ON chunk_encodings (storage_id,encoding_id) WHERE quarantined_at IS NOT NULL;

CREATE INDEX packs_quarantined ON packs (storage_id,id) WHERE quarantined_at IS NOT NULL;

CREATE INDEX encodings_owner_stream ON chunk_encodings(owner_stream) WHERE owner_stream IS NOT NULL;
CREATE INDEX encodings_recovery ON chunk_encodings(storage_id,unreferenced_at,encoding_id) WHERE recovery_pending;

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
CREATE INDEX raw_sources_cleanup ON raw_sources(storage_id,state,unreferenced_at,id);
CREATE INDEX raw_sources_owner ON raw_sources(owner_stream) WHERE owner_stream IS NOT NULL;
CREATE TABLE raw_ranges (
    stream_id INTEGER NOT NULL REFERENCES streams(id) ON DELETE CASCADE,
    offset_bytes INTEGER NOT NULL CHECK (offset_bytes>=0),
    length INTEGER NOT NULL CHECK (length>=0),
    source_id INTEGER NOT NULL REFERENCES raw_sources(id),
    source_offset INTEGER NOT NULL DEFAULT 0 CHECK (source_offset>=0),
    PRIMARY KEY (stream_id,offset_bytes)
) STRICT, WITHOUT ROWID;
CREATE INDEX raw_ranges_source ON raw_ranges(source_id);

CREATE UNIQUE INDEX raw_sources_content ON raw_sources(storage_id,content_hash,size);

CREATE TABLE multipart_files (
    id BLOB NOT NULL PRIMARY KEY CHECK (length(id)=16),
    upload_id BLOB REFERENCES uploads(id) ON DELETE SET NULL,
    storage_id BLOB NOT NULL REFERENCES storages(id),
    source_id INTEGER UNIQUE REFERENCES raw_sources(id),
    reclaim_pending INTEGER NOT NULL DEFAULT 0 CHECK (reclaim_pending IN (0,1)),
    state TEXT NOT NULL DEFAULT 'open' CHECK (state IN ('open','sealed','deleting'))
) STRICT, WITHOUT ROWID;
CREATE INDEX multipart_files_upload ON multipart_files(upload_id,state);
CREATE INDEX multipart_files_reclaim ON multipart_files(storage_id,id) WHERE reclaim_pending;
ALTER TABLE fragments ADD COLUMN file_id BLOB REFERENCES multipart_files(id);
ALTER TABLE fragments ADD COLUMN file_offset INTEGER CHECK ((file_id IS NULL)=(file_offset IS NULL) AND (file_offset IS NULL OR file_offset>=0));
CREATE INDEX fragments_file ON fragments(file_id,file_offset) WHERE file_id IS NOT NULL;

CREATE TABLE storage_maintenance (
    storage_id BLOB NOT NULL REFERENCES storages(id) ON DELETE CASCADE,
    policy TEXT NOT NULL,
    last_scheduled_at INTEGER NOT NULL,
    PRIMARY KEY(storage_id,policy)
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
CREATE INDEX raw_processing_due ON raw_processing(next_attempt_at,stream_id);

CREATE TABLE nonce_counters (
    block_key_id BLOB NOT NULL REFERENCES managed_keys(id),
    type_tag INTEGER NOT NULL CHECK (type_tag BETWEEN 0 AND 31),
    reserved_id INTEGER NOT NULL DEFAULT 0 CHECK (reserved_id>=0),
    PRIMARY KEY (block_key_id,type_tag)
) STRICT;

CREATE UNIQUE INDEX managed_keys_fingerprint ON managed_keys(fingerprint);

ALTER TABLE chunk_encodings ADD COLUMN nonce_id INTEGER;
UPDATE chunk_encodings SET nonce_id=encoding_id WHERE nonce IS NOT NULL;
CREATE TRIGGER chunk_encodings_nonce_insert BEFORE INSERT ON chunk_encodings WHEN NOT ((NEW.encryption_algorithm='none' AND NEW.nonce_id IS NULL AND NEW.nonce IS NULL AND NEW.block_key_id IS NULL) OR (NEW.encryption_algorithm<>'none' AND NEW.block_key_id IS NOT NULL AND ((NEW.nonce IS NULL AND NEW.nonce_id IS NULL AND NEW.state<>'ready') OR (NEW.nonce IS NOT NULL AND NEW.nonce_id IS NOT NULL AND NEW.nonce_id>0)))) BEGIN SELECT RAISE(ABORT,'invalid nonce metadata'); END;
CREATE TRIGGER chunk_encodings_nonce_update BEFORE UPDATE ON chunk_encodings WHEN NOT ((NEW.encryption_algorithm='none' AND NEW.nonce_id IS NULL AND NEW.nonce IS NULL AND NEW.block_key_id IS NULL) OR (NEW.encryption_algorithm<>'none' AND NEW.block_key_id IS NOT NULL AND ((NEW.nonce IS NULL AND NEW.nonce_id IS NULL AND NEW.state<>'ready') OR (NEW.nonce IS NOT NULL AND NEW.nonce_id IS NOT NULL AND NEW.nonce_id>0)))) BEGIN SELECT RAISE(ABORT,'invalid nonce metadata'); END;

ALTER TABLE packs ADD COLUMN nonce_id INTEGER;
UPDATE packs SET nonce_id=id WHERE nonce IS NOT NULL;
CREATE TRIGGER packs_nonce_insert BEFORE INSERT ON packs WHEN NOT ((NEW.encryption_algorithm='none' AND NEW.nonce_id IS NULL AND NEW.nonce IS NULL AND NEW.block_key_id IS NULL) OR (NEW.encryption_algorithm<>'none' AND NEW.block_key_id IS NOT NULL AND ((NEW.nonce IS NULL AND NEW.nonce_id IS NULL AND NEW.state<>'ready') OR (NEW.nonce IS NOT NULL AND NEW.nonce_id IS NOT NULL AND NEW.nonce_id>0)))) BEGIN SELECT RAISE(ABORT,'invalid nonce metadata'); END;
CREATE TRIGGER packs_nonce_update BEFORE UPDATE ON packs WHEN NOT ((NEW.encryption_algorithm='none' AND NEW.nonce_id IS NULL AND NEW.nonce IS NULL AND NEW.block_key_id IS NULL) OR (NEW.encryption_algorithm<>'none' AND NEW.block_key_id IS NOT NULL AND ((NEW.nonce IS NULL AND NEW.nonce_id IS NULL AND NEW.state<>'ready') OR (NEW.nonce IS NOT NULL AND NEW.nonce_id IS NOT NULL AND NEW.nonce_id>0)))) BEGIN SELECT RAISE(ABORT,'invalid nonce metadata'); END;

INSERT INTO nonce_counters(block_key_id,type_tag,reserved_id) SELECT id,0,COALESCE((SELECT seq FROM sqlite_sequence WHERE name='chunk_encodings'),0) FROM managed_keys WHERE purpose='block';
INSERT INTO nonce_counters(block_key_id,type_tag,reserved_id) SELECT id,1,COALESCE((SELECT seq FROM sqlite_sequence WHERE name='packs'),0) FROM managed_keys WHERE purpose='block';

UPDATE mokyu_meta SET schema_version=25 WHERE id=1;

CREATE TABLE nonce_observations (
    block_key_id BLOB NOT NULL PRIMARY KEY REFERENCES managed_keys(id),
    data TEXT NOT NULL CHECK(json_valid(data)),
    revision INTEGER NOT NULL CHECK(revision > 0)
) WITHOUT ROWID;

CREATE INDEX chunks_pending_key ON chunks(block_key_id) WHERE state IN ('preparing','uploading');

CREATE TABLE key_replacements (
    id BLOB PRIMARY KEY,
    actor TEXT NOT NULL,
    plan TEXT NOT NULL CHECK(json_valid(plan)),
    result TEXT CHECK(result IS NULL OR json_valid(result)),
    expires_at INTEGER NOT NULL
) WITHOUT ROWID;
CREATE INDEX key_replacements_expiry ON key_replacements(expires_at);

ALTER TABLE raw_sources ADD COLUMN pending_upload INTEGER NOT NULL DEFAULT false;
CREATE TABLE raw_uploads (
    source_id INTEGER PRIMARY KEY REFERENCES raw_sources(id) ON DELETE CASCADE,
    token BLOB,
    deduplicate INTEGER NOT NULL,
    background INTEGER NOT NULL,
    content_hash BLOB,
    next_attempt_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec')*1000000 AS INTEGER)),
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT
);
CREATE INDEX raw_uploads_due ON raw_uploads(next_attempt_at,source_id);

ALTER TABLE storage_profiles ADD COLUMN upload_cache_overflow TEXT NOT NULL DEFAULT 'fallback' CONSTRAINT storage_upload_overflow_check CHECK (upload_cache_overflow IN ('fallback','reject'));

ALTER TABLE uploads ADD COLUMN completion_token BLOB;
ALTER TABLE uploads ADD COLUMN completion_expires_at INTEGER;
ALTER TABLE pending_uploads ADD COLUMN encoding_policy TEXT CHECK (encoding_policy IS NULL OR json_valid(encoding_policy));
ALTER TABLE raw_sources ADD COLUMN processing_task BLOB REFERENCES tasks(id) ON DELETE SET NULL;
CREATE INDEX raw_sources_processing ON raw_sources(processing_task);
UPDATE mokyu_meta SET schema_version=33 WHERE id=1;

ALTER TABLE storage_profiles ADD COLUMN write_durability TEXT NOT NULL DEFAULT 'durable' CHECK(write_durability IN ('durable','buffered','volatile'));
CREATE INDEX raw_sources_sync ON raw_sources(id) WHERE durability IN ('buffered','volatile') AND state='ready' AND quarantine_code IS NULL;
UPDATE mokyu_meta SET schema_version=34 WHERE id=1;

ALTER TABLE chunk_encodings ADD COLUMN sync_pending INTEGER NOT NULL DEFAULT FALSE CHECK(sync_pending IN (0,1));
CREATE INDEX encodings_sync ON chunk_encodings(chunk_id) WHERE sync_pending AND state IN ('ready','uploading') AND quarantined_at IS NULL;

ALTER TABLE chunk_encodings ADD COLUMN boot_id BLOB CHECK(boot_id IS NULL OR (length(boot_id)=16 AND sync_pending));
