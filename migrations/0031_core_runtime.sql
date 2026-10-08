-- Final 0.0.4 runtime, storage, node, and task schema.
-- Earlier draft revisions are folded into this single append-only migration.

-- 0034_runtime_scope.sql
-- Keep instance-owned resource settings out of deployment-wide runtime rows.
UPDATE runtime_settings
SET value = value - 'thumbnail_cache_size'
WHERE section = 'web';

UPDATE runtime_settings
SET value = value - 'context_idle_timeout'
WHERE section = 'compression';

UPDATE runtime_settings
SET value = value - 'maintenance_concurrency'
WHERE section = 'packing';

UPDATE runtime_settings
SET value = (value - 'upload_cache_timeout') || jsonb_build_object('upload_flush_timeout', value->'upload_cache_timeout')
WHERE section = 'packing' AND value ? 'upload_cache_timeout';

UPDATE runtime_settings
SET value = (value - 'range_optimization') || jsonb_build_object('range_enabled', value->'range_optimization')
WHERE section = 'packing' AND value ? 'range_optimization';

UPDATE runtime_settings
SET value = (value - 'unreferenced_grace') || jsonb_build_object('grace_period', value->'unreferenced_grace')
WHERE section = 'garbage_collection' AND value ? 'unreferenced_grace';

UPDATE runtime_settings
SET value = (value
    - 'deleted_chunk_retention' - 'upload_retention' - 'task_retention' - 'audit_retention')
    || jsonb_build_object(
        'deleted_chunks', value->'deleted_chunk_retention',
        'uploads', value->'upload_retention',
        'tasks', value->'task_retention',
        'audit', value->'audit_retention'
    )
WHERE section = 'retention';

UPDATE runtime_settings
SET value = (value
    - 'storage_sample_interval' - 'runtime_sample_interval')
    || jsonb_build_object(
        'storage_interval', value->'storage_sample_interval',
        'runtime_interval', value->'runtime_sample_interval'
    )
WHERE section = 'statistics';

UPDATE runtime_settings
SET value = value - 'concurrency' - 'requests_per_second' - 'bandwidth'
WHERE section = 'integrity';


-- 0035_node_access.sql
-- Direct PostgreSQL node registration replaces join tokens and the single control lease.
ALTER TABLE mokyu_meta
    ADD COLUMN deployment_mode_revision bigint NOT NULL DEFAULT 1
        CHECK (deployment_mode_revision > 0);

ALTER TABLE nodes DROP CONSTRAINT IF EXISTS nodes_check;
ALTER TABLE nodes DROP CONSTRAINT IF EXISTS nodes_kind_check;
ALTER TABLE nodes ADD CONSTRAINT nodes_kind_check
    CHECK (kind IN ('control','worker','data'));
ALTER TABLE nodes DROP COLUMN IF EXISTS advertise_url;

DROP TABLE IF EXISTS node_join_tokens;
DROP TABLE IF EXISTS control_leases;


-- 0036_runtime_storage_profiles.sql
-- Final deployment runtime sections and typed storage policy foundations.
ALTER TABLE runtime_settings DROP CONSTRAINT IF EXISTS runtime_settings_section_check;

UPDATE runtime_settings
SET value = jsonb_build_object(
    'origin', COALESCE(value->'origin', '"http://localhost:9002"'::jsonb),
    'secure_cookie', COALESCE(value->'secure_cookie', 'true'::jsonb),
    'session_lifetime', COALESCE(value->'session_lifetime', '"12h"'::jsonb),
    'gravatar_base_url', COALESCE(value->'gravatar_base_url', value->'avatar_url', '"https://www.gravatar.com/avatar/"'::jsonb)
)
WHERE section='web';

INSERT INTO runtime_settings(section, value)
VALUES ('web', '{"origin":"http://localhost:9002","secure_cookie":true,"session_lifetime":"12h","gravatar_base_url":"https://www.gravatar.com/avatar/"}'::jsonb)
ON CONFLICT (section) DO NOTHING;

INSERT INTO runtime_settings(section, value)
SELECT 'requests', jsonb_build_object(
    's3_domain', COALESCE((SELECT value->'domain' FROM runtime_settings WHERE section='s3'), 'null'::jsonb),
    's3_region', COALESCE((SELECT value->'signature_region' FROM runtime_settings WHERE section='s3'), '"us-east-1"'::jsonb),
    's3_chunk_limit', COALESCE((SELECT value->'max_stream_chunk_size' FROM runtime_settings WHERE section='s3'), '"8MiB"'::jsonb),
    'read_idle_timeout', COALESCE((SELECT value->'read_idle_timeout' FROM runtime_settings WHERE section='requests'), '"120s"'::jsonb),
    'upload_idle_timeout', COALESCE((SELECT value->'upload_idle_timeout' FROM runtime_settings WHERE section='requests'), '"120s"'::jsonb)
)
ON CONFLICT (section) DO UPDATE SET value=EXCLUDED.value;

INSERT INTO runtime_settings(section, value)
SELECT 'uploads', jsonb_build_object(
    'idle_timeout', COALESCE((SELECT value->'idle_timeout' FROM runtime_settings WHERE section='uploads'), '"24h"'::jsonb),
    'sweep_interval', COALESCE((SELECT value->'sweep_interval' FROM runtime_settings WHERE section='uploads'), '"5m"'::jsonb),
    'client_idle_timeout', COALESCE((SELECT value->'client_idle_timeout' FROM runtime_settings WHERE section='uploads'), '"120s"'::jsonb),
    'max_active_uploads', COALESCE((SELECT value->'max_active_uploads' FROM runtime_settings WHERE section='uploads'), (SELECT value->'max_active' FROM runtime_settings WHERE section='uploads'), 'null'::jsonb),
    'staging_storage_id', 'null'::jsonb,
    'staging_bytes_limit', 'null'::jsonb
)
ON CONFLICT (section) DO UPDATE SET value=EXCLUDED.value;

INSERT INTO runtime_settings(section, value)
SELECT 'metrics', jsonb_build_object(
    'scope_limit', COALESCE((SELECT value->'scope_limit' FROM runtime_settings WHERE section='statistics'), '64'::jsonb),
    'storage_interval', COALESCE((SELECT value->'storage_interval' FROM runtime_settings WHERE section='statistics'), (SELECT value->'storage_sample_interval' FROM runtime_settings WHERE section='statistics'), '"15m"'::jsonb),
    'storage_retention', COALESCE((SELECT value->'storage_retention' FROM runtime_settings WHERE section='statistics'), '"7d"'::jsonb),
    'runtime_interval', COALESCE((SELECT value->'runtime_interval' FROM runtime_settings WHERE section='statistics'), (SELECT value->'runtime_sample_interval' FROM runtime_settings WHERE section='statistics'), '"1m"'::jsonb),
    'runtime_retention', COALESCE((SELECT value->'runtime_retention' FROM runtime_settings WHERE section='statistics'), '"24h"'::jsonb),
    'refresh_interval', COALESCE((SELECT value->'refresh_interval' FROM runtime_settings WHERE section='statistics'), '"15m"'::jsonb),
    'query_timeout', COALESCE((SELECT value->'query_timeout' FROM runtime_settings WHERE section='statistics'), '"2m"'::jsonb),
    'access_flush_interval', COALESCE((SELECT value->'access_flush_interval' FROM runtime_settings WHERE section='statistics'), '"1m"'::jsonb),
    'access_retention', COALESCE((SELECT value->'access_retention' FROM runtime_settings WHERE section='statistics'), '"14d"'::jsonb)
)
ON CONFLICT (section) DO UPDATE SET value=EXCLUDED.value;

INSERT INTO runtime_settings(section, value)
SELECT 'maintenance', jsonb_build_object(
    'interval', COALESCE((SELECT value->'interval' FROM runtime_settings WHERE section='garbage_collection'), '"30m"'::jsonb),
    'batch_size', COALESCE((SELECT value->'batch_size' FROM runtime_settings WHERE section='garbage_collection'), '1000'::jsonb),
    'max_duration', COALESCE((SELECT value->'max_duration' FROM runtime_settings WHERE section='retention'), '"5s"'::jsonb),
    'task_retention', COALESCE((SELECT value->'tasks' FROM runtime_settings WHERE section='retention'), '"30d"'::jsonb),
    'audit_retention', COALESCE((SELECT value->'audit' FROM runtime_settings WHERE section='retention'), '"90d"'::jsonb)
)
ON CONFLICT (section) DO UPDATE SET value=EXCLUDED.value;

DELETE FROM runtime_settings
WHERE section IN ('s3','storage','cache','compression','packing','garbage_collection','retention','statistics','integrity');
ALTER TABLE runtime_settings ADD CONSTRAINT runtime_settings_section_check
    CHECK (section IN ('web','requests','uploads','metrics','maintenance'));

ALTER TABLE store_permits RENAME TO storage_permits;
ALTER INDEX store_permits_active RENAME TO storage_permits_active;
ALTER TABLE storage_permits RENAME COLUMN store_id TO storage_id;
ALTER TABLE storage_permits DROP CONSTRAINT IF EXISTS store_permits_direction_check;
ALTER TABLE storage_permits ADD CONSTRAINT storage_permits_direction_check
    CHECK (direction IN ('read','upload','control','pack','integrity','maintenance','migration','staging'));

CREATE TABLE storage_profiles (
    storage_id uuid PRIMARY KEY REFERENCES stores(id) ON DELETE CASCADE,
    revision bigint NOT NULL DEFAULT 1 CHECK (revision > 0),
    connect_timeout text NOT NULL DEFAULT '5s',
    request_timeout text NOT NULL DEFAULT '30s',
    max_retries integer NOT NULL DEFAULT 3 CHECK (max_retries BETWEEN 0 AND 100),
    retry_timeout text NOT NULL DEFAULT '120s',
    priority_aging text NOT NULL DEFAULT '30s',
    min_storage_duration text NOT NULL DEFAULT '0s',
    unreferenced_grace text NOT NULL DEFAULT '48h',
    deleted_chunk_retention text NOT NULL DEFAULT '7d',
    upload_retention text NOT NULL DEFAULT '24h',
    cache_enabled boolean NOT NULL DEFAULT false,
    cache_soft_limit text,
    cache_entries bigint CHECK (cache_entries IS NULL OR cache_entries > 0),
    upload_cache boolean NOT NULL DEFAULT false,
    upload_cache_size text,
    compression_enabled boolean NOT NULL DEFAULT false,
    compression_strategy text NOT NULL DEFAULT 'always' CHECK (compression_strategy IN ('never','always','sample','file_type')),
    compression_level integer NOT NULL DEFAULT 6,
    compression_sample_level integer NOT NULL DEFAULT 3,
    compression_min_savings_percent integer NOT NULL DEFAULT 2 CHECK (compression_min_savings_percent BETWEEN 0 AND 100),
    compression_min_savings_bytes bigint NOT NULL DEFAULT 256 CHECK (compression_min_savings_bytes >= 0),
    pack_enabled boolean NOT NULL DEFAULT false,
    pack_max_size text NOT NULL DEFAULT '32MiB',
    pack_upload_flush_timeout text NOT NULL DEFAULT '60s',
    pack_interval text NOT NULL DEFAULT '1h',
    pack_reuse_interval text NOT NULL DEFAULT '5m',
    pack_reclaim_interval text NOT NULL DEFAULT '30m',
    pack_repack_interval text NOT NULL DEFAULT '24h',
    pack_repack_cooldown text NOT NULL DEFAULT '1h',
    pack_reclaim_min_savings_bytes text NOT NULL DEFAULT '4MiB',
    pack_range_enabled boolean NOT NULL DEFAULT true,
    pack_range_interval text NOT NULL DEFAULT '15m',
    pack_range_window text NOT NULL DEFAULT '24h',
    pack_range_min_downloads integer NOT NULL DEFAULT 8 CHECK (pack_range_min_downloads > 0),
    pack_range_min_savings_percent integer NOT NULL DEFAULT 50 CHECK (pack_range_min_savings_percent BETWEEN 1 AND 100),
    pack_range_min_savings_bytes text NOT NULL DEFAULT '64MiB',
    pack_range_repack_after text NOT NULL DEFAULT '30d',
    pack_range_repack_window text NOT NULL DEFAULT '7d',
    pack_range_repack_retry_interval text NOT NULL DEFAULT '7d',
    pack_concurrency integer NOT NULL DEFAULT 1 CHECK (pack_concurrency > 0),
    integrity_concurrency integer NOT NULL DEFAULT 1 CHECK (integrity_concurrency > 0),
    integrity_rps bigint CHECK (integrity_rps IS NULL OR integrity_rps > 0),
    integrity_bandwidth text,
    integrity_batch_timeout text NOT NULL DEFAULT '30s',
    read_concurrency integer CHECK (read_concurrency IS NULL OR read_concurrency > 0),
    upload_concurrency integer CHECK (upload_concurrency IS NULL OR upload_concurrency > 0),
    control_concurrency integer CHECK (control_concurrency IS NULL OR control_concurrency > 0),
    pack_byte_budget text,
    integrity_byte_budget text,
    maintenance_concurrency integer CHECK (maintenance_concurrency IS NULL OR maintenance_concurrency > 0),
    maintenance_byte_budget text,
    migration_concurrency integer CHECK (migration_concurrency IS NULL OR migration_concurrency > 0),
    migration_byte_budget text,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

INSERT INTO storage_profiles(storage_id, cache_enabled, upload_cache, compression_enabled, pack_enabled)
SELECT id, (type='s3'), (type='s3'), (type='s3'), (type='s3')
FROM stores
ON CONFLICT (storage_id) DO NOTHING;

CREATE TABLE storage_rate_state (
    storage_id uuid NOT NULL REFERENCES stores(id) ON DELETE CASCADE,
    metric text NOT NULL CHECK (metric IN ('integrity_rps','integrity_bandwidth')),
    tokens numeric(30,6) NOT NULL DEFAULT 0 CHECK (tokens >= 0),
    replenished_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (storage_id, metric)
);


-- 0037_storage_schema_vocabulary.sql
-- Final storage vocabulary and physical encoding identities. Previous migrations stay immutable.

CREATE EXTENSION IF NOT EXISTS pgcrypto;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM chunks c
        JOIN chunk_locations l ON l.id = c.encoding_id
        WHERE c.storage_id IS DISTINCT FROM l.storage_id
    ) THEN
        RAISE EXCEPTION 'chunk location mapping is inconsistent before storage vocabulary migration';
    END IF;
    IF EXISTS (
        SELECT 1
        FROM chunks c
        WHERE c.key_id <> ''
          AND NOT EXISTS (
              SELECT 1 FROM managed_keys k
              WHERE k.name = c.key_id AND k.purpose = 'block' AND k.revoked_at IS NULL
          )
    ) OR EXISTS (
        SELECT 1
        FROM chunk_locations c
        WHERE c.key_id <> ''
          AND NOT EXISTS (
              SELECT 1 FROM managed_keys k
              WHERE k.name = c.key_id AND k.purpose = 'block' AND k.revoked_at IS NULL
          )
    ) OR EXISTS (
        SELECT 1
        FROM packs p
        WHERE p.key_id <> ''
          AND NOT EXISTS (
              SELECT 1 FROM managed_keys k
              WHERE k.name = p.key_id AND k.purpose = 'block' AND k.revoked_at IS NULL
          )
    ) THEN
        RAISE EXCEPTION 'physical block key cannot be resolved to an active managed block key';
    END IF;
END
$$;

-- Remove historical indexes/checks that still depend on the text key column.
DROP INDEX IF EXISTS chunks_dedup;
DO $$
DECLARE
    row record;
BEGIN
    FOR row IN
        SELECT c.relname, con.conname
        FROM pg_constraint con
        JOIN pg_class c ON c.oid = con.conrelid
        WHERE c.relname IN ('chunks','chunk_locations','packs')
          AND pg_get_constraintdef(con.oid) ILIKE '%key_id%'
    LOOP
        EXECUTE format('ALTER TABLE %I DROP CONSTRAINT %I', row.relname, row.conname);
    END LOOP;
END
$$;

ALTER TABLE stores RENAME TO storages;
ALTER TABLE admin_setup RENAME TO setup_tokens;
ALTER TABLE quota_accounts RENAME TO quota_scopes;
ALTER TABLE chunk_locations RENAME TO chunk_encodings;
ALTER INDEX chunk_locations_current RENAME TO chunk_encodings_current;
ALTER INDEX chunk_locations_gc RENAME TO chunk_encodings_gc;

ALTER TABLE storages RENAME COLUMN current_key_id TO block_key_id;
ALTER TABLE storages RENAME COLUMN remote_bucket TO remote_bucket_name;
ALTER TABLE buckets RENAME COLUMN store_id TO storage_id;

ALTER TABLE chunks RENAME COLUMN storage_id TO legacy_location_id;
ALTER TABLE chunks RENAME COLUMN store_id TO storage_id;
ALTER TABLE chunks RENAME COLUMN encoding_id TO current_encoding_id;
ALTER TABLE chunks RENAME COLUMN algorithm TO encryption_algorithm;
ALTER TABLE chunks RENAME COLUMN stored_size TO standalone_size;
ALTER TABLE chunks RENAME COLUMN compressed TO standalone_compressed;

ALTER TABLE chunk_encodings RENAME COLUMN id TO encoding_id;
ALTER TABLE chunk_encodings RENAME COLUMN storage_id TO location_id;
ALTER TABLE chunk_encodings RENAME COLUMN store_id TO storage_id;
ALTER TABLE chunk_encodings RENAME COLUMN algorithm TO encryption_algorithm;
ALTER TABLE chunk_encodings RENAME COLUMN stored_size TO standalone_size;
ALTER TABLE chunk_encodings RENAME COLUMN compressed TO standalone_compressed;

ALTER TABLE packs RENAME COLUMN storage_id TO location_id;
ALTER TABLE packs RENAME COLUMN store_id TO storage_id;
ALTER TABLE packs RENAME COLUMN algorithm TO encryption_algorithm;
ALTER TABLE packs RENAME COLUMN compressed TO pack_compressed;

ALTER TABLE quota_scopes RENAME COLUMN kind TO scope;
ALTER TABLE quota_scopes RENAME COLUMN id TO scope_id;
ALTER TABLE quota_scopes RENAME COLUMN byte_limit TO logical_bytes_limit;
ALTER TABLE quota_scopes RENAME COLUMN inflight_limit TO inflight_bytes_limit;
ALTER TABLE quota_scopes RENAME COLUMN bucket_limit TO bucket_count_limit;

ALTER TABLE managed_keys RENAME COLUMN algorithm TO key_algorithm;

-- Finish the domain vocabulary cleanup in one schema revision. These names are
-- deliberately explicit so logical objects, task roles and credentials do not
-- reuse the storage/location words above.
ALTER TABLE objects RENAME COLUMN key TO object_key;
ALTER TABLE objects RENAME COLUMN write_epoch TO write_token;
ALTER TABLE parts RENAME COLUMN write_epoch TO write_token;
ALTER TABLE streams RENAME COLUMN kind TO stream_type;
ALTER TABLE streams RENAME COLUMN owner_task TO owner_task_id;
ALTER TABLE streams RENAME COLUMN owner_lease TO owner_lease_token;
ALTER TABLE tasks RENAME COLUMN kind TO task_type;
ALTER TABLE tasks RENAME COLUMN created_by TO created_by_label;
ALTER TABLE tasks ADD COLUMN parent_task_id uuid REFERENCES tasks(id) ON DELETE SET NULL;
ALTER TABLE tasks ADD COLUMN execution_scope text NOT NULL DEFAULT 'cluster';
ALTER TABLE tasks ADD COLUMN target_node_id uuid REFERENCES nodes(id) ON DELETE SET NULL;
ALTER TABLE tasks ADD COLUMN batch_cursor text;
ALTER TABLE tasks ADD COLUMN batch_total bigint;
ALTER TABLE tasks ADD COLUMN batch_completed bigint NOT NULL DEFAULT 0;
ALTER TABLE nodes RENAME COLUMN kind TO role;
ALTER TABLE nodes ADD COLUMN status text NOT NULL DEFAULT 'registered';
ALTER TABLE nodes ADD COLUMN credential_version bigint NOT NULL DEFAULT 1;
ALTER TABLE nodes ADD COLUMN recovery_required boolean NOT NULL DEFAULT false;
ALTER TABLE nodes ADD COLUMN lease_expires_at timestamptz;
ALTER TABLE nodes ADD COLUMN load_score double precision;
ALTER TABLE nodes ADD COLUMN load_sampled_at timestamptz;
ALTER TABLE quota_scopes ADD COLUMN cached_bytes bigint NOT NULL DEFAULT 0 CHECK (cached_bytes >= 0);
ALTER TABLE quota_scopes ADD COLUMN cached_compressed boolean NOT NULL DEFAULT false;
ALTER TABLE pending_uploads RENAME COLUMN source_pack TO source_pack_id;
ALTER TABLE pending_uploads RENAME COLUMN cache_size TO staged_size;
ALTER TABLE pending_uploads DROP COLUMN cache_compressed;
ALTER TABLE quota_reservations RENAME COLUMN base_stream TO replaced_stream_id;
ALTER TABLE web_uploads RENAME COLUMN expected_stream TO expected_stream_id;
ALTER TABLE storage_permits RENAME COLUMN holder TO holder_id;
ALTER TABLE credentials RENAME TO s3_credentials;
ALTER TABLE s3_credentials RENAME COLUMN access_key TO access_key_id;
ALTER TABLE grants RENAME TO s3_credential_grants;
ALTER TABLE member_grants RENAME TO member_bucket_grants;
ALTER TABLE token_grants RENAME TO token_bucket_grants;
ALTER TABLE domains RENAME TO bucket_domains;
ALTER TABLE storage_insights RENAME TO usage_summary;
ALTER TABLE storage_history RENAME TO usage_history;
ALTER TABLE runtime_history RENAME TO runtime_samples;
ALTER TABLE usage_history RENAME COLUMN at TO snapshot_at;
ALTER TABLE runtime_samples RENAME COLUMN at TO sampled_at;
ALTER TABLE uploads ADD COLUMN staging_storage_id uuid;
ALTER TABLE uploads ADD COLUMN staging_object_key text;
ALTER TABLE fragments ADD COLUMN staging_storage_id uuid;
ALTER TABLE fragments ADD COLUMN staging_object_key text;
ALTER TABLE uploads ADD CONSTRAINT uploads_staging_storage_fkey FOREIGN KEY (staging_storage_id) REFERENCES storages(id) ON DELETE SET NULL;
ALTER TABLE fragments ADD CONSTRAINT fragments_staging_storage_fkey FOREIGN KEY (staging_storage_id) REFERENCES storages(id) ON DELETE SET NULL;
ALTER TABLE audit_events RENAME COLUMN status TO http_status;
ALTER TABLE runtime_settings RENAME COLUMN updated_by TO updated_by_user_id;

ALTER INDEX IF EXISTS nodes_kind_enabled RENAME TO nodes_role_enabled;
ALTER INDEX IF EXISTS pending_uploads_source RENAME TO pending_uploads_source_pack_id;
ALTER INDEX IF EXISTS member_grants_bucket RENAME TO member_bucket_grants_bucket;
ALTER INDEX IF EXISTS token_grants_bucket RENAME TO token_bucket_grants_bucket;
ALTER INDEX IF EXISTS storage_history_expiry RENAME TO usage_history_snapshot;
CREATE INDEX IF NOT EXISTS tasks_parent_task ON tasks(parent_task_id) WHERE parent_task_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS tasks_target_node ON tasks(target_node_id) WHERE target_node_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS nodes_role_enabled ON nodes(role,enabled);
ALTER TABLE nodes RENAME CONSTRAINT nodes_kind_check TO nodes_role_check;


-- Recreate trigger functions whose PL/pgSQL bodies mention renamed tables/columns.
DROP TRIGGER IF EXISTS stores_current_block_key_validate ON storages;
CREATE OR REPLACE FUNCTION validate_store_current_key() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.block_key_id IS NOT NULL THEN
        PERFORM id FROM managed_keys WHERE id = NEW.block_key_id FOR SHARE;
    END IF;
    IF NEW.block_key_id IS NOT NULL AND NOT EXISTS (
        SELECT 1 FROM managed_keys
        WHERE id = NEW.block_key_id AND purpose = 'block' AND revoked_at IS NULL
    ) THEN
        RAISE EXCEPTION 'storage block key must be an active block key';
    END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER storages_block_key_validate
    BEFORE INSERT OR UPDATE OF block_key_id ON storages
    FOR EACH ROW EXECUTE FUNCTION validate_store_current_key();

CREATE OR REPLACE FUNCTION prevent_bucket_store_change() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.storage_id IS DISTINCT FROM OLD.storage_id THEN
        RAISE EXCEPTION 'bucket storage is immutable';
    END IF;
    RETURN NEW;
END
$$;
CREATE OR REPLACE FUNCTION validate_storage_binding() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    expected uuid;
BEGIN
    SELECT storage_id INTO expected FROM chunks WHERE id = NEW.chunk_id;
    IF expected IS NULL OR NEW.storage_id IS DISTINCT FROM expected THEN
        RAISE EXCEPTION 'storage binding does not match its parent';
    END IF;
    RETURN NEW;
END
$$;
CREATE OR REPLACE FUNCTION validate_pack_member_binding() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    pack_storage uuid;
    chunk_storage uuid;
BEGIN
    SELECT storage_id INTO pack_storage FROM packs WHERE id = NEW.pack_id;
    SELECT storage_id INTO chunk_storage FROM chunks WHERE id = NEW.chunk_id;
    IF pack_storage IS NULL OR chunk_storage IS NULL OR pack_storage IS DISTINCT FROM chunk_storage THEN
        RAISE EXCEPTION 'pack member storage binding does not match its pack';
    END IF;
    RETURN NEW;
END
$$;

ALTER TABLE chunks ADD COLUMN block_key_id uuid REFERENCES managed_keys(id);
UPDATE chunks c SET block_key_id = k.id
FROM managed_keys k WHERE c.key_id <> '' AND k.name = c.key_id AND k.purpose = 'block';
ALTER TABLE chunks DROP COLUMN key_id;
ALTER TABLE chunks ADD CONSTRAINT chunks_block_key_check
    CHECK ((encryption_algorithm = 'none' AND block_key_id IS NULL)
        OR (encryption_algorithm <> 'none' AND block_key_id IS NOT NULL));

ALTER TABLE chunk_encodings ADD COLUMN block_key_id uuid REFERENCES managed_keys(id);
UPDATE chunk_encodings c SET block_key_id = k.id
FROM managed_keys k WHERE c.key_id <> '' AND k.name = c.key_id AND k.purpose = 'block';
ALTER TABLE chunk_encodings DROP COLUMN key_id;
ALTER TABLE chunk_encodings ADD CONSTRAINT chunk_encodings_block_key_check
    CHECK ((encryption_algorithm = 'none' AND block_key_id IS NULL)
        OR (encryption_algorithm <> 'none' AND block_key_id IS NOT NULL));

ALTER TABLE packs ADD COLUMN block_key_id uuid REFERENCES managed_keys(id);
UPDATE packs p SET block_key_id = k.id
FROM managed_keys k WHERE p.key_id <> '' AND k.name = p.key_id AND k.purpose = 'block';
ALTER TABLE packs DROP COLUMN key_id;
ALTER TABLE packs ADD CONSTRAINT packs_block_key_check
    CHECK ((encryption_algorithm = 'none' AND block_key_id IS NULL)
        OR (encryption_algorithm <> 'none' AND block_key_id IS NOT NULL));

ALTER TABLE chunks DROP COLUMN legacy_location_id;
CREATE UNIQUE INDEX chunks_dedup ON chunks(storage_id,hash,raw_size) WHERE state IN ('preparing','uploading','ready');
ALTER TABLE chunk_encodings ADD COLUMN encoding_fingerprint bytea;
ALTER TABLE packs ADD COLUMN pack_fingerprint bytea;

UPDATE chunk_encodings e
SET encoding_fingerprint = digest(
    concat_ws(':', e.block_key_id::text, e.encryption_algorithm, e.standalone_compressed::text, 'standalone'),
    'sha256'
);
UPDATE packs p
SET pack_fingerprint = digest(
    concat_ws(':', p.block_key_id::text, p.encryption_algorithm, p.pack_compressed::text, p.member_count::text),
    'sha256'
);
ALTER TABLE chunk_encodings ALTER COLUMN encoding_fingerprint SET NOT NULL;
ALTER TABLE chunk_encodings ADD CONSTRAINT chunk_encodings_fingerprint_size
    CHECK (octet_length(encoding_fingerprint) = 32);
ALTER TABLE packs ALTER COLUMN pack_fingerprint SET NOT NULL;
ALTER TABLE packs ADD CONSTRAINT packs_fingerprint_size
    CHECK (octet_length(pack_fingerprint) = 32);

ALTER TABLE integrity_issues RENAME COLUMN storage_id TO location_id;
ALTER TABLE quota_reservations RENAME COLUMN output_stream TO result_stream_id;
ALTER TABLE pending_uploads ADD COLUMN staging_storage_id uuid;
ALTER TABLE pending_uploads ADD COLUMN staging_object_key text;
ALTER TABLE pending_uploads ADD CONSTRAINT pending_uploads_staging_storage_fkey FOREIGN KEY (staging_storage_id) REFERENCES storages(id) ON DELETE SET NULL;

-- Rebuild quota functions after quota scope and limit column renames.
CREATE OR REPLACE FUNCTION quota_lock(b uuid) RETURNS uuid LANGUAGE plpgsql AS $$
DECLARE p uuid;
BEGIN
    SELECT project_id INTO STRICT p FROM buckets WHERE id=b FOR SHARE;
    PERFORM 1 FROM quota_scopes WHERE scope='project' AND scope_id=p FOR UPDATE;
    PERFORM 1 FROM quota_scopes WHERE scope='bucket' AND scope_id=b FOR UPDATE;
    RETURN p;
END $$;
CREATE OR REPLACE FUNCTION quota_adjust(b uuid, used_delta bigint, reserved_delta bigint, inflight_delta bigint, objects_delta bigint) RETURNS void LANGUAGE plpgsql AS $$
DECLARE p uuid; q quota_scopes;
BEGIN
    p := quota_lock(b);
    FOR q IN SELECT * FROM quota_scopes WHERE (scope='project' AND scope_id=p) OR (scope='bucket' AND scope_id=b) LOOP
        IF (used_delta+reserved_delta>0 AND q.logical_bytes_limit IS NOT NULL AND q.used_bytes+q.reserved_bytes+used_delta+reserved_delta>q.logical_bytes_limit)
          OR (inflight_delta>0 AND q.inflight_bytes_limit IS NOT NULL AND q.inflight_bytes+inflight_delta>q.inflight_bytes_limit) THEN
            RAISE EXCEPTION 'storage quota exceeded' USING ERRCODE='MKQ01';
        END IF;
    END LOOP;
    UPDATE quota_scopes SET used_bytes=used_bytes+used_delta,reserved_bytes=reserved_bytes+reserved_delta,
        inflight_bytes=inflight_bytes+inflight_delta,object_count=object_count+objects_delta
    WHERE (scope='project' AND scope_id=p) OR (scope='bucket' AND scope_id=b);
END $$;
CREATE OR REPLACE FUNCTION quota_change(owner uuid, logical bigint, inflight bigint, credit bigint) RETURNS void LANGUAGE plpgsql AS $$
DECLARE r quota_reservations;
BEGIN
    SELECT * INTO STRICT r FROM quota_reservations WHERE id=owner;
    PERFORM quota_adjust(r.bucket_id,0,greatest(logical-credit,0)-greatest(r.logical_bytes-r.credit_bytes,0),inflight-r.inflight_bytes,0);
    UPDATE quota_reservations SET logical_bytes=logical,inflight_bytes=inflight,credit_bytes=credit WHERE id=owner;
END $$;
CREATE OR REPLACE FUNCTION quota_drop(owner uuid) RETURNS void LANGUAGE plpgsql AS $$
DECLARE r quota_reservations;
BEGIN
    SELECT * INTO r FROM quota_reservations WHERE id=owner;
    IF NOT FOUND THEN RETURN; END IF;
    PERFORM quota_change(owner,0,0,0);
    DELETE FROM quota_reservations WHERE id=owner;
END $$;
CREATE OR REPLACE FUNCTION quota_forget_write(s uuid) RETURNS void LANGUAGE plpgsql AS $$
DECLARE w quota_writes; r quota_reservations;
BEGIN
    SELECT * INTO w FROM quota_writes WHERE stream_id=s;
    IF NOT FOUND THEN RETURN; END IF;
    SELECT * INTO STRICT r FROM quota_reservations WHERE id=w.reservation_id;
    IF r.stream_id IS NOT NULL THEN
        PERFORM quota_drop(r.id);
    ELSE
        PERFORM quota_change(r.id,r.logical_bytes-w.contribution_bytes,r.inflight_bytes-w.allocated_bytes,r.credit_bytes);
        DELETE FROM quota_writes WHERE stream_id=s;
        IF r.closed AND NOT EXISTS(SELECT 1 FROM quota_writes WHERE reservation_id=r.id) THEN PERFORM quota_drop(r.id); END IF;
    END IF;
END $$;
CREATE OR REPLACE FUNCTION quota_reserve(s uuid, required bigint, padding boolean, upload uuid, part integer) RETURNS bigint LANGUAGE plpgsql AS $$
DECLARE info streams; owner uuid; r quota_reservations; w quota_writes; old_id uuid; old_size bigint;
    accepted bigint:=0; target bigint; delta bigint; available bigint; q quota_scopes; project uuid;
BEGIN
    IF required<0 THEN RAISE EXCEPTION 'negative reservation'; END IF;
    IF upload IS NOT NULL THEN PERFORM 1 FROM uploads WHERE id=upload FOR SHARE; END IF;
    -- Keep cancellation cleanup from abandoning this stream before its reservation exists.
    SELECT * INTO STRICT info FROM streams WHERE id=s FOR SHARE;
    project:=quota_lock(info.bucket_id);
    IF info.state<>'writing' OR (upload IS NULL AND NOT EXISTS(SELECT 1 FROM objects WHERE bucket_id=info.bucket_id AND object_key=info.object_key AND write_token=s))
      OR (upload IS NOT NULL AND NOT EXISTS(SELECT 1 FROM parts p JOIN uploads u ON u.id=p.upload_id WHERE p.upload_id=upload AND p.part_number=part AND p.write_token=s AND u.state='active')) THEN
        RAISE EXCEPTION 'write was superseded' USING ERRCODE='MKQ02';
    END IF;
    owner:=coalesce(upload,s);
    IF NOT EXISTS(SELECT 1 FROM quota_reservations WHERE id=owner) THEN
        SELECT o.stream_id,st.size INTO old_id,old_size FROM objects o JOIN streams st ON st.id=o.stream_id WHERE o.bucket_id=info.bucket_id AND o.object_key=info.object_key;
        IF old_id IS NOT NULL AND EXISTS(SELECT 1 FROM quota_reservations WHERE bucket_id=info.bucket_id AND object_key=info.object_key AND replaced_stream_id=old_id) THEN old_id:=NULL; END IF;
        INSERT INTO quota_reservations(id,bucket_id,object_key,stream_id,upload_id,replaced_stream_id,credit_bytes)
        VALUES(owner,info.bucket_id,info.object_key,CASE WHEN upload IS NULL THEN s END,upload,old_id,CASE WHEN old_id IS NULL THEN 0 ELSE old_size END);
    END IF;
    SELECT * INTO STRICT r FROM quota_reservations WHERE id=owner;
    INSERT INTO quota_writes(stream_id,reservation_id) VALUES(s,owner) ON CONFLICT DO NOTHING;
    SELECT * INTO STRICT w FROM quota_writes WHERE stream_id=s;
    IF upload IS NOT NULL THEN SELECT quota_size INTO STRICT accepted FROM parts WHERE upload_id=upload AND part_number=part; END IF;
    target:=required;
    IF padding AND required>w.allocated_bytes THEN
        target:=((required-1)/4194304+1)*4194304;
        available:=target-w.allocated_bytes;
        FOR q IN SELECT * FROM quota_scopes WHERE (scope='project' AND scope_id=project) OR (scope='bucket' AND scope_id=info.bucket_id) LOOP
            IF q.logical_bytes_limit IS NOT NULL THEN available:=least(available,greatest(q.logical_bytes_limit-q.used_bytes-q.reserved_bytes,0)+greatest(r.credit_bytes-r.logical_bytes,0)+greatest(accepted-w.allocated_bytes,0)); END IF;
            IF q.inflight_bytes_limit IS NOT NULL THEN available:=least(available,greatest(q.inflight_bytes_limit-q.inflight_bytes,0)); END IF;
        END LOOP;
        target:=greatest(required,least(target,w.allocated_bytes+available));
    END IF;
    delta:=greatest(target-accepted,0)-w.contribution_bytes;
    PERFORM quota_change(owner,r.logical_bytes+delta,r.inflight_bytes+target-w.allocated_bytes,r.credit_bytes);
    UPDATE quota_writes SET allocated_bytes=target,contribution_bytes=greatest(target-accepted,0) WHERE stream_id=s;
    RETURN target;
END $$;
CREATE OR REPLACE FUNCTION quota_object() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE b uuid; k text; before_id uuid; after_id uuid; before_size bigint:=0; after_size bigint:=0;
    r quota_reservations; freed bigint:=0; inflight bigint:=0; credits bigint:=0; pending bigint:=0;
BEGIN
    b:=coalesce(NEW.bucket_id,OLD.bucket_id); k:=coalesce(NEW.object_key,OLD.object_key);
    PERFORM quota_lock(b);
    before_id:=OLD.stream_id; after_id:=NEW.stream_id;
    IF before_id IS DISTINCT FROM after_id THEN
        IF before_id IS NOT NULL THEN SELECT size INTO STRICT before_size FROM streams WHERE id=before_id; END IF;
        IF after_id IS NOT NULL THEN SELECT size INTO STRICT after_size FROM streams WHERE id=after_id; END IF;
        SELECT * INTO r FROM quota_reservations WHERE stream_id=after_id OR result_stream_id=after_id;
        IF FOUND THEN
            freed:=greatest(r.logical_bytes-r.credit_bytes,0); inflight:=r.inflight_bytes;
            IF r.upload_id IS NOT NULL THEN
                SELECT coalesce(sum(allocated_bytes),0) INTO pending FROM quota_writes WHERE reservation_id=r.id;
            END IF;
            IF pending=0 THEN DELETE FROM quota_reservations WHERE id=r.id;
            ELSE
                UPDATE quota_writes SET contribution_bytes=0 WHERE reservation_id=r.id;
                UPDATE quota_reservations SET logical_bytes=0,inflight_bytes=pending,credit_bytes=0,replaced_stream_id=NULL,closed=true,result_stream_id=NULL WHERE id=r.id;
                inflight:=inflight-pending;
            END IF;
        END IF;
        SELECT coalesce(sum(greatest(logical_bytes,0)-greatest(logical_bytes-credit_bytes,0)),0) INTO credits
        FROM quota_reservations WHERE bucket_id=b AND object_key=k AND replaced_stream_id=before_id;
        UPDATE quota_reservations SET credit_bytes=0,replaced_stream_id=NULL WHERE bucket_id=b AND object_key=k AND replaced_stream_id=before_id;
        PERFORM quota_adjust(b,after_size-before_size,credits-freed,-inflight,(after_id IS NOT NULL)::int-(before_id IS NOT NULL)::int);
    END IF;
    IF TG_OP='UPDATE' AND OLD.write_token<>NEW.write_token THEN
        SELECT * INTO r FROM quota_reservations WHERE stream_id=OLD.write_token;
        IF FOUND THEN
            PERFORM quota_change(r.id,0,r.inflight_bytes,0);
            UPDATE quota_reservations SET replaced_stream_id=NULL WHERE id=r.id;
            UPDATE quota_writes SET contribution_bytes=0 WHERE reservation_id=r.id;
        END IF;
    END IF;
    RETURN NULL;
END $$;
CREATE OR REPLACE FUNCTION quota_part() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE u uuid; b uuid; r quota_reservations; w quota_writes; size bigint:=0;
BEGIN
    u:=coalesce(NEW.upload_id,OLD.upload_id);
    SELECT bucket_id INTO b FROM uploads WHERE id=u;
    IF b IS NULL THEN RETURN coalesce(NEW,OLD); END IF;
    PERFORM quota_lock(b);
    SELECT * INTO r FROM quota_reservations WHERE id=u;
    IF NOT FOUND OR r.closed THEN RETURN coalesce(NEW,OLD); END IF;
    IF TG_OP='UPDATE' AND OLD.write_token<>NEW.write_token THEN
        SELECT * INTO w FROM quota_writes WHERE stream_id=OLD.write_token;
        IF FOUND THEN
            PERFORM quota_change(u,r.logical_bytes-w.contribution_bytes,r.inflight_bytes,r.credit_bytes);
            UPDATE quota_writes SET contribution_bytes=0 WHERE stream_id=OLD.write_token;
            r.logical_bytes:=r.logical_bytes-w.contribution_bytes;
        END IF;
    END IF;
    IF OLD.stream_id IS DISTINCT FROM NEW.stream_id THEN
        IF NEW.stream_id IS NOT NULL THEN
            SELECT s.size INTO STRICT size FROM streams s WHERE s.id=NEW.stream_id;
            SELECT * INTO STRICT w FROM quota_writes WHERE stream_id=NEW.stream_id;
            IF w.allocated_bytes<size THEN RAISE EXCEPTION 'part is not reserved'; END IF;
            PERFORM quota_change(u,r.logical_bytes+size-coalesce(OLD.quota_size,0)-w.contribution_bytes,r.inflight_bytes+size-coalesce(OLD.quota_size,0)-w.allocated_bytes,r.credit_bytes);
            DELETE FROM quota_writes WHERE stream_id=NEW.stream_id;
            NEW.quota_size:=size;
        ELSE
            PERFORM quota_change(u,r.logical_bytes-OLD.quota_size,r.inflight_bytes-OLD.quota_size,r.credit_bytes);
        END IF;
    END IF;
    RETURN coalesce(NEW,OLD);
END $$;
CREATE OR REPLACE FUNCTION quota_stream_end() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP='DELETE' THEN
        IF EXISTS(SELECT 1 FROM quota_writes WHERE stream_id=OLD.id) THEN PERFORM quota_lock(OLD.bucket_id); PERFORM quota_forget_write(OLD.id); END IF;
        RETURN OLD;
    END IF;
    IF NEW.state IN ('retired','abandoned') AND OLD.state<>NEW.state AND EXISTS(SELECT 1 FROM quota_writes WHERE stream_id=NEW.id) THEN
        PERFORM quota_lock(NEW.bucket_id); PERFORM quota_forget_write(NEW.id);
    END IF;
    RETURN NULL;
END $$;
CREATE OR REPLACE FUNCTION quota_upload_end() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE r quota_reservations; pending bigint;
BEGIN
    IF TG_OP='DELETE' THEN
        PERFORM quota_lock(OLD.bucket_id); PERFORM quota_drop(OLD.id); RETURN OLD;
    END IF;
    IF NEW.state IN ('completed','aborted') AND OLD.state<>NEW.state THEN
        PERFORM quota_lock(NEW.bucket_id);
        SELECT * INTO r FROM quota_reservations WHERE id=NEW.id;
        IF FOUND AND NOT r.closed THEN
            SELECT coalesce(sum(allocated_bytes),0) INTO pending FROM quota_writes WHERE reservation_id=r.id;
            PERFORM quota_change(r.id,0,pending,0);
            UPDATE quota_writes SET contribution_bytes=0 WHERE reservation_id=r.id;
            UPDATE quota_reservations SET closed=true,replaced_stream_id=NULL,result_stream_id=NULL WHERE id=r.id;
            IF pending=0 THEN DELETE FROM quota_reservations WHERE id=r.id; END IF;
        END IF;
    END IF;
    RETURN NULL;
END $$;
CREATE OR REPLACE FUNCTION quota_project() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP='INSERT' THEN INSERT INTO quota_scopes(scope,scope_id) VALUES('project',NEW.id);
    ELSE DELETE FROM quota_scopes WHERE scope='project' AND scope_id=OLD.id; END IF;
    RETURN NULL;
END $$;

CREATE OR REPLACE FUNCTION quota_bucket() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE q quota_scopes; p uuid; source quota_scopes;
BEGIN
    IF TG_OP='UPDATE' THEN
        IF OLD.project_id<>NEW.project_id THEN
            PERFORM 1 FROM quota_scopes WHERE scope='project' AND scope_id IN (OLD.project_id,NEW.project_id) ORDER BY scope_id FOR UPDATE;
            SELECT * INTO STRICT source FROM quota_scopes WHERE scope='bucket' AND scope_id=NEW.id FOR UPDATE;
            IF source.reserved_bytes<>0 OR source.inflight_bytes<>0 THEN RAISE EXCEPTION 'wait for bucket uploads to drain' USING ERRCODE='MKQ02'; END IF;
            SELECT * INTO STRICT q FROM quota_scopes WHERE scope='project' AND scope_id=NEW.project_id;
            IF (q.bucket_count_limit IS NOT NULL AND q.bucket_count>=q.bucket_count_limit) OR
               (source.used_bytes>0 AND q.logical_bytes_limit IS NOT NULL AND source.used_bytes>greatest(q.logical_bytes_limit-q.used_bytes-q.reserved_bytes,0)) THEN
                RAISE EXCEPTION 'storage quota exceeded' USING ERRCODE='MKQ01';
            END IF;
            UPDATE quota_scopes SET bucket_count=bucket_count-1,used_bytes=used_bytes-source.used_bytes,object_count=object_count-source.object_count
            WHERE scope='project' AND scope_id=OLD.project_id;
            UPDATE quota_scopes SET bucket_count=bucket_count+1,used_bytes=used_bytes+source.used_bytes,object_count=object_count+source.object_count
            WHERE scope='project' AND scope_id=NEW.project_id;
        END IF;
        RETURN NULL;
    END IF;
    p:=coalesce(NEW.project_id,OLD.project_id);
    SELECT * INTO STRICT q FROM quota_scopes WHERE scope='project' AND scope_id=p FOR UPDATE;
    IF TG_OP='INSERT' THEN
        IF q.bucket_count_limit IS NOT NULL AND q.bucket_count>=q.bucket_count_limit THEN RAISE EXCEPTION 'storage quota exceeded' USING ERRCODE='MKQ01'; END IF;
        UPDATE quota_scopes SET bucket_count=bucket_count+1 WHERE scope='project' AND scope_id=p;
        INSERT INTO quota_scopes(scope,scope_id) VALUES('bucket',NEW.id);
    ELSE
        UPDATE quota_scopes SET bucket_count=bucket_count-1 WHERE scope='project' AND scope_id=p;
        DELETE FROM quota_scopes WHERE scope='bucket' AND scope_id=OLD.id;
    END IF;
    RETURN NULL;
END $$;

-- PL/pgSQL bodies keep SQL text until execution, so rebuild the functions that
-- mention the renamed object, stream, credential, grant and domain tables.
CREATE OR REPLACE FUNCTION catalog_object() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE item streams;
BEGIN
    IF NEW.stream_id IS NULL THEN
        NEW.catalog_size:=NULL; NEW.catalog_modified:=NULL; NEW.catalog_type:=NULL;
        NEW.catalog_kind:=NULL; NEW.catalog_public:=NULL;
    ELSE
        SELECT * INTO STRICT item FROM streams WHERE id=NEW.stream_id;
        NEW.catalog_size:=item.size;
        NEW.catalog_modified:=item.created_at;
        NEW.catalog_type:=coalesce(item.metadata->>'content_type','application/octet-stream');
        NEW.catalog_kind:=catalog_kind(NEW.object_key,NEW.catalog_type);
        NEW.catalog_public:=item.public_read;
    END IF;
    RETURN NEW;
END $$;
CREATE OR REPLACE FUNCTION catalog_stream() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    UPDATE objects SET catalog_size=NEW.size,catalog_modified=NEW.created_at,
        catalog_type=coalesce(NEW.metadata->>'content_type','application/octet-stream'),
        catalog_kind=catalog_kind(object_key,coalesce(NEW.metadata->>'content_type','application/octet-stream')),
        catalog_public=NEW.public_read WHERE stream_id=NEW.id;
    RETURN NULL;
END $$;

CREATE OR REPLACE FUNCTION invalidate_service_authorization() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    UPDATE s3_credentials SET authorization_revision=authorization_revision+1 WHERE access_key_id=COALESCE(NEW.access_key,OLD.access_key);
    RETURN NULL;
END $$;
CREATE OR REPLACE FUNCTION invalidate_token_authorization() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    UPDATE api_tokens SET authorization_revision=authorization_revision+1 WHERE id=COALESCE(NEW.token_id,OLD.token_id);
    RETURN NULL;
END $$;



-- 0038_physical_fingerprints.sql
-- Keep physical fingerprints complete for every insert and profile change.

-- The stream vocabulary migration renamed streams.kind to stream_type. Recreate
-- the ready-stream trigger so a completed object still enters packing work.
CREATE OR REPLACE FUNCTION enqueue_pack_stream() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.state='ready' AND OLD.state IS DISTINCT FROM NEW.state THEN
        INSERT INTO pack_maintenance(stream_id)
        SELECT NEW.id
        WHERE NEW.stream_type='object'
          AND EXISTS(SELECT 1 FROM extents WHERE stream_id=NEW.id AND chunk_id IS NOT NULL OFFSET 1)
        ON CONFLICT(stream_id) DO UPDATE SET
            generation=pack_maintenance.generation+1,
            cursor=0,
            reason='pack',
            updated_at=now(),
            next_check_at=now();
        INSERT INTO pack_changes(pack_id)
        SELECT DISTINCT c.pack_id
        FROM extents e JOIN chunks c ON c.id=e.chunk_id
        WHERE e.stream_id=NEW.id AND c.pack_id IS NOT NULL
        ON CONFLICT(pack_id) DO UPDATE SET
            generation=pack_changes.generation+1,
            next_check_at=now(),
            reason='reuse';
    END IF;
    RETURN NULL;
END
$$;

CREATE OR REPLACE FUNCTION refresh_encoding_fingerprint() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    p storage_profiles;
BEGIN
    SELECT * INTO p FROM storage_profiles WHERE storage_id = NEW.storage_id;
    NEW.encoding_fingerprint := digest(
        concat_ws(':',
            NEW.block_key_id::text,
            NEW.encryption_algorithm,
            NEW.standalone_compressed::text,
            coalesce(p.compression_strategy, ''),
            coalesce(p.compression_level::text, ''),
            coalesce(p.compression_sample_level::text, ''),
            coalesce(p.compression_min_savings_percent::text, ''),
            coalesce(p.compression_min_savings_bytes::text, ''),
            'standalone'),
        'sha256');
    RETURN NEW;
END
$$;

CREATE OR REPLACE FUNCTION refresh_pack_fingerprint() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    p storage_profiles;
BEGIN
    SELECT * INTO p FROM storage_profiles WHERE storage_id = NEW.storage_id;
    NEW.pack_fingerprint := digest(
        concat_ws(':',
            NEW.block_key_id::text,
            NEW.encryption_algorithm,
            NEW.pack_compressed::text,
            NEW.member_count::text,
            coalesce(p.pack_max_size, ''),
            coalesce(p.pack_range_enabled::text, ''),
            coalesce(p.pack_range_min_downloads::text, ''),
            coalesce(p.pack_range_min_savings_percent::text, ''),
            coalesce(p.pack_range_min_savings_bytes, ''),
            'pack'),
        'sha256');
    RETURN NEW;
END
$$;

DROP TRIGGER IF EXISTS chunk_encodings_fingerprint ON chunk_encodings;
CREATE TRIGGER chunk_encodings_fingerprint
    BEFORE INSERT OR UPDATE OF storage_id,block_key_id,encryption_algorithm,standalone_compressed
    ON chunk_encodings
    FOR EACH ROW EXECUTE FUNCTION refresh_encoding_fingerprint();

DROP TRIGGER IF EXISTS packs_fingerprint ON packs;
CREATE TRIGGER packs_fingerprint
    BEFORE INSERT OR UPDATE OF storage_id,block_key_id,encryption_algorithm,pack_compressed,member_count
    ON packs
    FOR EACH ROW EXECUTE FUNCTION refresh_pack_fingerprint();

UPDATE chunk_encodings SET encryption_algorithm = encryption_algorithm;
UPDATE packs SET encryption_algorithm = encryption_algorithm;


-- 0039_shared_staging.sql
-- Cluster upload staging reservations are shared through PostgreSQL.
CREATE TABLE staging_objects (
    storage_id uuid NOT NULL REFERENCES storages(id) ON DELETE CASCADE,
    object_key text NOT NULL,
    bytes bigint NOT NULL CHECK (bytes > 0),
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (storage_id, object_key)
);
CREATE INDEX staging_objects_created ON staging_objects(created_at, storage_id);


-- 0040_rewrite_compression_contract.sql
-- Final maintenance task and compression strategy vocabulary.
UPDATE storage_profiles
SET compression_strategy = 'sample'
WHERE compression_strategy IN ('file_type', 'chunk_hint');

ALTER TABLE storage_profiles
    DROP CONSTRAINT IF EXISTS storage_profiles_compression_strategy_check;
ALTER TABLE storage_profiles
    ADD CONSTRAINT storage_profiles_compression_strategy_check
        CHECK (compression_strategy IN ('never', 'always', 'sample'));

ALTER TABLE tasks DROP CONSTRAINT IF EXISTS tasks_kind_check;
ALTER TABLE tasks DROP CONSTRAINT IF EXISTS tasks_task_type_check;
UPDATE tasks SET task_type = 'rewrite' WHERE task_type = 'rekey';
ALTER TABLE tasks ADD CONSTRAINT tasks_task_type_check
    CHECK (task_type IN ('purge','sweep','integrity','pack','unpack','upload','cache_flush','gc','cleanup','storage_migration','rewrite'));

CREATE OR REPLACE FUNCTION refresh_pack_fingerprint() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    p storage_profiles;
BEGIN
    SELECT * INTO p FROM storage_profiles WHERE storage_id = NEW.storage_id;
    NEW.pack_fingerprint := digest(
        concat_ws(':',
            NEW.block_key_id::text,
            NEW.encryption_algorithm,
            NEW.pack_compressed::text,
            NEW.member_count::text,
            coalesce(p.compression_strategy, ''),
            coalesce(p.compression_level::text, ''),
            coalesce(p.compression_sample_level::text, ''),
            coalesce(p.compression_min_savings_percent::text, ''),
            coalesce(p.compression_min_savings_bytes::text, ''),
            coalesce(p.pack_max_size, ''),
            coalesce(p.pack_range_enabled::text, ''),
            coalesce(p.pack_range_min_downloads::text, ''),
            coalesce(p.pack_range_min_savings_percent::text, ''),
            coalesce(p.pack_range_min_savings_bytes, ''),
            'pack'),
        'sha256');
    RETURN NEW;
END
$$;

UPDATE packs SET encryption_algorithm = encryption_algorithm;


-- 0041_remove_global_storage_identity.sql
-- Storage identity and initialization are per-storage metadata, not deployment state.
ALTER TABLE mokyu_meta
    DROP COLUMN IF EXISTS storage_identity,
    DROP COLUMN IF EXISTS storage_initialized;


-- 0042_usage_tasks.sql
-- Usage snapshots are worker tasks so controls never run large aggregate queries.
ALTER TABLE tasks DROP CONSTRAINT IF EXISTS tasks_task_type_check;
ALTER TABLE tasks ADD CONSTRAINT tasks_task_type_check
    CHECK (task_type IN ('purge','sweep','integrity','pack','unpack','upload','cache_flush','gc','cleanup','storage_migration','rewrite','usage'));


-- 0043_storage_identity_marker.sql
-- Storage identity markers use an explicit column name across records and verification.
ALTER TABLE storages RENAME COLUMN metadata TO identity_marker;


UPDATE mokyu_meta SET schema_version=31;
