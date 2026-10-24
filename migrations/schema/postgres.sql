-- Generated from the complete postgres migration chain through 0075.
-- Do not edit directly. The initializer owns deployment metadata and migration history.

SET LOCAL check_function_bodies = false;
SET LOCAL standard_conforming_strings = on;
SET LOCAL default_tablespace = '';
SET LOCAL default_table_access_method = heap;

CREATE EXTENSION IF NOT EXISTS pg_trgm WITH SCHEMA public;

CREATE EXTENSION IF NOT EXISTS pgcrypto WITH SCHEMA public;

CREATE FUNCTION public.advance_authorization_revision() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    NEW.authorization_revision=OLD.authorization_revision+1;
    RETURN NEW;
END $$;

CREATE FUNCTION public.bucket_settings_revision() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    NEW.settings_revision:=OLD.settings_revision+1;
    RETURN NEW;
END $$;

CREATE FUNCTION public.catalog_category(key text, mime text) RETURNS text
    LANGUAGE sql IMMUTABLE
    AS $_$
    SELECT CASE
        WHEN mime LIKE 'image/%' OR key ~* '\.(png|jpe?g|webp|gif|avif|svg|bmp|heic)$' THEN 'image'
        WHEN mime LIKE 'video/%' OR key ~* '\.(mp4|webm|mkv|mov|avi|m4v)$' THEN 'video'
        WHEN mime LIKE 'audio/%' OR key ~* '\.(mp3|ogg|opus|wav|flac|aac|m4a)$' THEN 'audio'
        WHEN mime IN ('application/zip','application/gzip','application/x-7z-compressed','application/x-tar') OR key ~* '\.(zip|gz|7z|rar|tar|zst)$' THEN 'archive'
        WHEN mime LIKE 'text/%' OR mime='application/pdf' OR key ~* '\.(pdf|txt|md|json|csv|html?|xml|docx?|xlsx?|pptx?)$' THEN 'document'
        ELSE 'other' END
$_$;

CREATE FUNCTION public.catalog_object() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE item streams;
BEGIN
    IF NEW.stream_id IS NULL THEN
        NEW.catalog_size:=NULL; NEW.catalog_modified:=NULL; NEW.catalog_type:=NULL;
        NEW.catalog_category:=NULL; NEW.catalog_public:=NULL;
    ELSE
        SELECT * INTO STRICT item FROM streams WHERE id=NEW.stream_id;
        NEW.catalog_size:=item.size;
        NEW.catalog_modified:=item.created_at;
        NEW.catalog_type:=coalesce(item.metadata->>'content_type','application/octet-stream');
        NEW.catalog_category:=catalog_category(NEW.object_key,NEW.catalog_type);
        NEW.catalog_public:=item.public_read;
    END IF;
    RETURN NEW;
END $$;

CREATE FUNCTION public.catalog_stream() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    UPDATE objects SET catalog_size=NEW.size,catalog_modified=NEW.created_at,
        catalog_type=coalesce(NEW.metadata->>'content_type','application/octet-stream'),
        catalog_category=catalog_category(object_key,coalesce(NEW.metadata->>'content_type','application/octet-stream')),
        catalog_public=NEW.public_read WHERE stream_id=NEW.id;
    RETURN NULL;
END $$;

CREATE FUNCTION public.clear_stream_write_lease() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    IF NEW.state <> 'writing' THEN
        DELETE FROM stream_leases
        WHERE stream_id = NEW.id AND lease_type = 'write';
    END IF;
    RETURN NEW;
END
$$;

CREATE FUNCTION public.domain_settings_revision() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    UPDATE buckets SET settings_revision=settings_revision+1 WHERE id IN (NEW.bucket_id,OLD.bucket_id);
    RETURN NULL;
END $$;

CREATE FUNCTION public.enqueue_pack_extents() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    PERFORM id FROM chunks WHERE id IN (SELECT chunk_id FROM changed) ORDER BY id FOR NO KEY UPDATE;
    UPDATE chunks SET reference_changed_at=now() WHERE id IN (SELECT chunk_id FROM changed);
    INSERT INTO pack_changes(pack_id,reason)
        SELECT DISTINCT c.pack_id,CASE WHEN TG_OP='DELETE' THEN 'reclaim' ELSE 'reuse' END FROM changed e JOIN chunks c ON c.id=e.chunk_id WHERE c.pack_id IS NOT NULL
        ON CONFLICT(pack_id) DO UPDATE SET generation=pack_changes.generation+1,next_check_at=now(),reason=CASE WHEN EXCLUDED.reason='reuse' THEN 'reuse' ELSE pack_changes.reason END;
    INSERT INTO pack_maintenance(stream_id,reason)
        SELECT DISTINCT e.stream_id,'repack' FROM changed x JOIN extents e ON e.chunk_id=x.chunk_id
        JOIN streams s ON s.id=e.stream_id WHERE s.state='ready'
        ON CONFLICT(stream_id) DO UPDATE SET generation=pack_maintenance.generation+1,cursor=0,updated_at=now(),next_check_at=now();
    RETURN NULL;
END $$;

CREATE FUNCTION public.enqueue_pack_stream() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
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

CREATE FUNCTION public.invalidate_member_authorization() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    UPDATE web_users SET authorization_revision=authorization_revision+1 WHERE id=COALESCE(NEW.user_id,OLD.user_id);
    RETURN NULL;
END $$;

CREATE FUNCTION public.invalidate_service_authorization() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    UPDATE s3_credentials SET authorization_revision=authorization_revision+1 WHERE access_key_id=COALESCE(NEW.access_key,OLD.access_key);
    RETURN NULL;
END $$;

CREATE FUNCTION public.invalidate_token_authorization() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    UPDATE api_tokens SET authorization_revision=authorization_revision+1 WHERE id=COALESCE(NEW.token_id,OLD.token_id);
    RETURN NULL;
END $$;

CREATE FUNCTION public.prevent_bucket_store_change() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    IF NEW.storage_id IS DISTINCT FROM OLD.storage_id THEN
        RAISE EXCEPTION 'bucket storage is immutable';
    END IF;
    RETURN NEW;
END
$$;

CREATE FUNCTION public.quota_adjust(b uuid, used_delta bigint, reserved_delta bigint, inflight_delta bigint, objects_delta bigint) RETURNS void
    LANGUAGE plpgsql
    AS $$
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

CREATE FUNCTION public.quota_bucket() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
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

CREATE FUNCTION public.quota_change(owner uuid, logical bigint, inflight bigint, credit bigint) RETURNS void
    LANGUAGE plpgsql
    AS $$
DECLARE r quota_reservations;
BEGIN
    SELECT * INTO STRICT r FROM quota_reservations WHERE id=owner;
    PERFORM quota_adjust(r.bucket_id,0,greatest(logical-credit,0)-greatest(r.logical_bytes-r.credit_bytes,0),inflight-r.inflight_bytes,0);
    UPDATE quota_reservations SET logical_bytes=logical,inflight_bytes=inflight,credit_bytes=credit WHERE id=owner;
END $$;

CREATE FUNCTION public.quota_drop(owner uuid) RETURNS void
    LANGUAGE plpgsql
    AS $$
DECLARE r quota_reservations;
BEGIN
    SELECT * INTO r FROM quota_reservations WHERE id=owner;
    IF NOT FOUND THEN RETURN; END IF;
    PERFORM quota_change(owner,0,0,0);
    DELETE FROM quota_reservations WHERE id=owner;
END $$;

CREATE FUNCTION public.quota_forget_write(s uuid) RETURNS void
    LANGUAGE plpgsql
    AS $$
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

CREATE FUNCTION public.quota_lock(b uuid) RETURNS uuid
    LANGUAGE plpgsql
    AS $$
DECLARE p uuid;
BEGIN
    SELECT project_id INTO STRICT p FROM buckets WHERE id=b FOR SHARE;
    PERFORM 1 FROM quota_scopes WHERE scope='project' AND scope_id=p FOR UPDATE;
    PERFORM 1 FROM quota_scopes WHERE scope='bucket' AND scope_id=b FOR UPDATE;
    RETURN p;
END $$;

CREATE FUNCTION public.quota_object() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
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

CREATE FUNCTION public.quota_part() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
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

CREATE FUNCTION public.quota_project() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    IF TG_OP='INSERT' THEN INSERT INTO quota_scopes(scope,scope_id) VALUES('project',NEW.id);
    ELSE DELETE FROM quota_scopes WHERE scope='project' AND scope_id=OLD.id; END IF;
    RETURN NULL;
END $$;

CREATE FUNCTION public.quota_reserve(s uuid, required bigint, padding boolean, upload uuid, part integer) RETURNS bigint
    LANGUAGE plpgsql
    AS $$
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

CREATE FUNCTION public.quota_stream_end() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
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

CREATE FUNCTION public.quota_upload_end() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
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

CREATE FUNCTION public.refresh_encoding_fingerprint() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE
    p storage_profiles;
BEGIN
    SELECT * INTO p FROM storage_profiles WHERE storage_id = NEW.storage_id;
    NEW.encoding_fingerprint := digest(
        concat_ws(':',
            NEW.block_key_id::text,
            NEW.encryption_algorithm,
            NEW.standalone_compressed::text,
            coalesce(p.compression_enabled::text, ''),
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

CREATE FUNCTION public.refresh_pack_fingerprint() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
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
            coalesce(p.compression_enabled::text, ''),
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

CREATE FUNCTION public.validate_pack_member_binding() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
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

CREATE FUNCTION public.validate_storage_binding() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
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

CREATE FUNCTION public.validate_store_current_key() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
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

CREATE TABLE public.api_tokens (
    id uuid DEFAULT uuidv7() NOT NULL,
    user_id uuid NOT NULL,
    label text NOT NULL,
    token_hash bytea NOT NULL,
    prefix text NOT NULL,
    system boolean DEFAULT false NOT NULL,
    auth_revision bigint NOT NULL,
    authorization_revision bigint DEFAULT 0 NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    expires_at timestamp with time zone,
    last_used_at timestamp with time zone,
    revoked_at timestamp with time zone,
    CONSTRAINT api_tokens_label_check CHECK (((octet_length(label) >= 1) AND (octet_length(label) <= 128))),
    CONSTRAINT api_tokens_token_hash_check CHECK ((octet_length(token_hash) = 32))
);

CREATE TABLE public.audit_events (
    id uuid DEFAULT uuidv7() NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    finished_at timestamp with time zone,
    actor_id uuid,
    actor_label text NOT NULL,
    token_id uuid,
    source text NOT NULL,
    action text NOT NULL,
    target text DEFAULT ''::text NOT NULL,
    project_id uuid,
    bucket_id uuid,
    outcome text DEFAULT 'unknown'::text NOT NULL,
    request_id text,
    http_status integer,
    detail jsonb DEFAULT '{}'::jsonb NOT NULL,
    CONSTRAINT audit_events_outcome_check CHECK ((outcome = ANY (ARRAY['unknown'::text, 'succeeded'::text, 'failed'::text, 'partial'::text]))),
    CONSTRAINT audit_events_source_check CHECK ((source = ANY (ARRAY['web'::text, 'token'::text, 'cli'::text, 's3'::text])))
);

CREATE TABLE public.bucket_domains (
    host text CONSTRAINT domains_host_not_null NOT NULL,
    bucket_id uuid CONSTRAINT domains_bucket_id_not_null NOT NULL
);

CREATE TABLE public.buckets (
    id uuid DEFAULT uuidv7() NOT NULL,
    name text NOT NULL COLLATE pg_catalog."C",
    state text DEFAULT 'active'::text NOT NULL,
    cors jsonb DEFAULT '[]'::jsonb NOT NULL,
    website_enabled boolean DEFAULT false NOT NULL,
    index_document text DEFAULT 'index.html'::text NOT NULL,
    error_document text DEFAULT '404.html'::text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    project_id uuid DEFAULT '00000000-0000-0000-0000-000000000001'::uuid NOT NULL,
    settings_revision bigint DEFAULT 0 NOT NULL,
    uploads_paused boolean DEFAULT false NOT NULL,
    public_base_url text DEFAULT ''::text NOT NULL,
    storage_id uuid CONSTRAINT buckets_storage_target_id_not_null NOT NULL,
    CONSTRAINT buckets_state_check CHECK ((state = ANY (ARRAY['active'::text, 'purging'::text])))
);

CREATE TABLE public.cache_pins (
    chunk_id uuid NOT NULL,
    pin_type text NOT NULL,
    owner_id uuid NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT cache_pins_pin_type_check CHECK ((pin_type = ANY (ARRAY['upload'::text, 'pack'::text])))
);

CREATE TABLE public.chunk_access_stats (
    chunk_id uuid NOT NULL,
    reads bigint DEFAULT 0 NOT NULL,
    range_reads bigint DEFAULT 0 NOT NULL,
    bytes bigint DEFAULT 0 NOT NULL,
    last_read_at timestamp with time zone NOT NULL
);

CREATE TABLE public.chunk_access_windows (
    chunk_id uuid NOT NULL,
    window_start timestamp with time zone NOT NULL,
    reads bigint DEFAULT 0 NOT NULL,
    range_reads bigint DEFAULT 0 NOT NULL,
    bytes bigint DEFAULT 0 NOT NULL,
    range_origin_reads bigint DEFAULT 0 NOT NULL
);

CREATE TABLE public.chunk_encodings (
    encoding_id uuid DEFAULT uuidv7() CONSTRAINT chunk_locations_id_not_null NOT NULL,
    chunk_id uuid CONSTRAINT chunk_locations_chunk_id_not_null NOT NULL,
    standalone_size integer,
    standalone_compressed boolean CONSTRAINT chunk_locations_compressed_not_null NOT NULL,
    nonce bytea,
    state text CONSTRAINT chunk_locations_state_not_null NOT NULL,
    created_at timestamp with time zone CONSTRAINT chunk_locations_created_at_not_null NOT NULL,
    unreferenced_at timestamp with time zone,
    deleted_at timestamp with time zone,
    owner_task uuid,
    encryption_algorithm text CONSTRAINT chunk_locations_algorithm_not_null NOT NULL,
    stored_at timestamp with time zone,
    storage_id uuid CONSTRAINT chunk_locations_storage_target_id_not_null NOT NULL,
    block_key_id uuid,
    encoding_fingerprint bytea NOT NULL,
    quarantined_at timestamp with time zone,
    quarantine_code text,
    quarantine_revision bigint DEFAULT 0 NOT NULL,
    owner_stream uuid,
    recovery_pending boolean DEFAULT false NOT NULL,
    nonce_id bigint,
    sync_pending boolean DEFAULT false NOT NULL,
    boot_id uuid,
    CONSTRAINT chunk_encodings_block_key_check CHECK ((((encryption_algorithm = 'none'::text) AND (block_key_id IS NULL)) OR ((encryption_algorithm <> 'none'::text) AND (block_key_id IS NOT NULL)))),
    CONSTRAINT chunk_encodings_check CHECK (((boot_id IS NULL) OR sync_pending)),
    CONSTRAINT chunk_encodings_fingerprint_size CHECK ((octet_length(encoding_fingerprint) = 32)),
    CONSTRAINT chunk_encodings_nonce_id CHECK ((((encryption_algorithm = 'none'::text) AND (nonce_id IS NULL) AND (nonce IS NULL) AND (block_key_id IS NULL)) OR ((encryption_algorithm <> 'none'::text) AND (block_key_id IS NOT NULL) AND (((nonce IS NULL) AND (nonce_id IS NULL) AND (state <> 'ready'::text)) OR ((nonce IS NOT NULL) AND (nonce_id IS NOT NULL) AND (nonce_id > 0)))))),
    CONSTRAINT chunk_locations_algorithm_check CHECK ((encryption_algorithm = ANY (ARRAY['none'::text, 'aes-256-gcm'::text, 'chacha20-poly1305'::text]))),
    CONSTRAINT chunk_locations_state_check CHECK ((state = ANY (ARRAY['uploading'::text, 'ready'::text, 'retired'::text, 'deleting'::text, 'deleted'::text]))),
    CONSTRAINT encodings_owner_exclusive CHECK (((owner_task IS NULL) OR (owner_stream IS NULL))),
    CONSTRAINT encodings_quarantine CHECK (((quarantined_at IS NULL) = (quarantine_code IS NULL))),
    CONSTRAINT encodings_quarantine_revision CHECK ((quarantine_revision >= 0))
);

CREATE TABLE public.chunks (
    id uuid DEFAULT uuidv7() NOT NULL,
    owner_stream uuid,
    hash bytea NOT NULL,
    raw_size integer NOT NULL,
    standalone_size integer,
    encryption_algorithm text CONSTRAINT chunks_algorithm_not_null NOT NULL,
    standalone_compressed boolean DEFAULT false CONSTRAINT chunks_compressed_not_null NOT NULL,
    nonce bytea,
    format integer DEFAULT 2 NOT NULL,
    state text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    unreferenced_at timestamp with time zone,
    deleted_at timestamp with time zone,
    current_encoding_id uuid DEFAULT uuidv7() CONSTRAINT chunks_encoding_id_not_null NOT NULL,
    pack_id uuid,
    reference_changed_at timestamp with time zone DEFAULT now() NOT NULL,
    split_at timestamp with time zone,
    range_split_at timestamp with time zone,
    repack_after timestamp with time zone,
    storage_id uuid CONSTRAINT chunks_storage_target_id_not_null NOT NULL,
    block_key_id uuid,
    CONSTRAINT chunks_algorithm_check CHECK ((encryption_algorithm = ANY (ARRAY['none'::text, 'aes-256-gcm'::text, 'chacha20-poly1305'::text]))),
    CONSTRAINT chunks_block_key_check CHECK ((((encryption_algorithm = 'none'::text) AND (block_key_id IS NULL)) OR ((encryption_algorithm <> 'none'::text) AND (block_key_id IS NOT NULL)))),
    CONSTRAINT chunks_format_check CHECK ((format = 2)),
    CONSTRAINT chunks_hash_check CHECK ((octet_length(hash) = 32)),
    CONSTRAINT chunks_raw_size_check CHECK (((raw_size >= 1) AND (raw_size <= 4194304))),
    CONSTRAINT chunks_state_check CHECK ((state = ANY (ARRAY['preparing'::text, 'uploading'::text, 'ready'::text, 'failed'::text, 'deleting'::text, 'deleted'::text]))),
    CONSTRAINT chunks_stored_size_check CHECK (((standalone_size >= 1) AND (standalone_size <= 4194320)))
)
WITH (autovacuum_vacuum_scale_factor='0.05', autovacuum_analyze_scale_factor='0.02');

CREATE TABLE public.extents (
    stream_id uuid NOT NULL,
    offset_bytes bigint NOT NULL,
    length integer NOT NULL,
    chunk_id uuid,
    fragment_id uuid,
    source_offset integer DEFAULT 0 NOT NULL,
    CONSTRAINT extents_check CHECK (((chunk_id IS NULL) <> (fragment_id IS NULL))),
    CONSTRAINT extents_check1 CHECK (((source_offset + length) <= 4194304)),
    CONSTRAINT extents_length_check CHECK (((length >= 1) AND (length <= 4194304))),
    CONSTRAINT extents_offset_bytes_check CHECK ((offset_bytes >= 0)),
    CONSTRAINT extents_source_offset_check CHECK (((source_offset >= 0) AND (source_offset < 4194304)))
)
WITH (autovacuum_vacuum_scale_factor='0.05', autovacuum_analyze_scale_factor='0.02');

CREATE TABLE public.fragments (
    id uuid DEFAULT uuidv7() NOT NULL,
    owner_stream uuid,
    sealed boolean DEFAULT false NOT NULL,
    size integer NOT NULL,
    hash bytea NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    staging_storage_id uuid,
    staging_object_key text,
    file_id uuid,
    file_offset bigint,
    CONSTRAINT fragments_check CHECK ((((file_id IS NULL) = (file_offset IS NULL)) AND ((file_offset IS NULL) OR (file_offset >= 0)))),
    CONSTRAINT fragments_hash_check CHECK ((octet_length(hash) = 32)),
    CONSTRAINT fragments_size_check CHECK (((size >= 1) AND (size <= 4194304)))
)
WITH (autovacuum_vacuum_scale_factor='0.05', autovacuum_analyze_scale_factor='0.02');

CREATE TABLE public.integrity_issues (
    id uuid DEFAULT uuidv7() NOT NULL,
    task_id uuid NOT NULL,
    subject text NOT NULL,
    code text NOT NULL,
    chunk_id uuid,
    location_id uuid,
    stream_id uuid,
    bucket_id uuid,
    object_key text,
    detail jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);

CREATE TABLE public.integrity_packs (
    task_id uuid NOT NULL,
    pack_id uuid NOT NULL,
    error_code text
);

CREATE TABLE public.key_fingerprints (
    key_id text NOT NULL,
    algorithm text NOT NULL,
    fingerprint bytea NOT NULL,
    CONSTRAINT key_fingerprints_fingerprint_check CHECK ((octet_length(fingerprint) = 32))
);

CREATE TABLE public.key_replacements (
    id uuid NOT NULL,
    actor text NOT NULL,
    plan jsonb NOT NULL,
    result jsonb,
    expires_at timestamp with time zone NOT NULL
);

CREATE TABLE public.maintenance_controls (
    policy text CONSTRAINT maintenance_controls_kind_not_null NOT NULL,
    paused boolean DEFAULT false NOT NULL,
    last_scheduled_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT maintenance_controls_policy_check CHECK ((policy = ANY (ARRAY['pack'::text, 'reuse'::text, 'reclaim'::text, 'repack'::text, 'range'::text, 'gc'::text, 'cleanup'::text])))
);

CREATE TABLE public.maintenance_previews (
    id uuid DEFAULT uuidv7() NOT NULL,
    actor_id uuid NOT NULL,
    token_id uuid,
    action text NOT NULL,
    parameters jsonb NOT NULL,
    fingerprint text NOT NULL,
    expires_at timestamp with time zone DEFAULT (now() + '00:10:00'::interval) NOT NULL,
    task_id uuid,
    CONSTRAINT maintenance_previews_action_check CHECK ((action = ANY (ARRAY['unpack'::text, 'sweep'::text, 'quarantine'::text, 'recheck'::text])))
);

CREATE TABLE public.managed_keys (
    id uuid DEFAULT uuidv7() NOT NULL,
    name text NOT NULL,
    purpose text NOT NULL,
    key_algorithm text DEFAULT 'raw'::text CONSTRAINT managed_keys_algorithm_not_null NOT NULL,
    secret_encrypted bytea,
    fingerprint bytea NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    revoked_at timestamp with time zone,
    CONSTRAINT managed_keys_algorithm_check CHECK ((key_algorithm = ANY (ARRAY['raw'::text, 'aes-256-gcm'::text, 'chacha20-poly1305'::text]))),
    CONSTRAINT managed_keys_fingerprint_check CHECK ((octet_length(fingerprint) = 32)),
    CONSTRAINT managed_keys_name_check CHECK (((octet_length(name) >= 1) AND (octet_length(name) <= 128))),
    CONSTRAINT managed_keys_purpose_check CHECK ((purpose = ANY (ARRAY['access'::text, 'signing'::text, 'block'::text])))
);

CREATE TABLE public.media_operations (
    id uuid DEFAULT uuidv7() NOT NULL,
    user_id uuid NOT NULL,
    token_id uuid,
    client_id uuid NOT NULL,
    request_hash bytea NOT NULL,
    result jsonb,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT media_operations_request_hash_check CHECK ((octet_length(request_hash) = 32))
);

CREATE TABLE public.member_bucket_grants (
    user_id uuid CONSTRAINT member_grants_user_id_not_null NOT NULL,
    project_id uuid CONSTRAINT member_grants_project_id_not_null NOT NULL,
    bucket_id uuid CONSTRAINT member_grants_bucket_id_not_null NOT NULL,
    actions text[] CONSTRAINT member_grants_actions_not_null NOT NULL,
    CONSTRAINT member_grants_actions_check CHECK ((actions <@ ARRAY['bucket.list'::text, 'object.read'::text, 'object.write'::text, 'object.delete'::text, 'object.acl'::text, 'bucket.settings'::text, 'storage.inspect'::text]))
);

CREATE TABLE public.mokyu_meta (
    singleton boolean DEFAULT true NOT NULL,
    schema_version integer NOT NULL,
    deployment_id uuid NOT NULL,
    gc_paused boolean DEFAULT false NOT NULL,
    maintenance boolean DEFAULT false NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    access_coverage_since timestamp with time zone DEFAULT now() NOT NULL,
    access_flushed_at timestamp with time zone DEFAULT now() NOT NULL,
    project_management boolean DEFAULT false NOT NULL,
    pack_creation_paused boolean DEFAULT false NOT NULL,
    deployment_mode text DEFAULT 'single'::text NOT NULL,
    deployment_key_fingerprint bytea,
    deployment_mode_revision bigint DEFAULT 1 NOT NULL,
    CONSTRAINT mokyu_meta_deployment_key_fingerprint_check CHECK (((deployment_key_fingerprint IS NULL) OR (octet_length(deployment_key_fingerprint) = 32))),
    CONSTRAINT mokyu_meta_deployment_mode_check CHECK ((deployment_mode = ANY (ARRAY['single'::text, 'cluster'::text]))),
    CONSTRAINT mokyu_meta_deployment_mode_revision_check CHECK ((deployment_mode_revision > 0)),
    CONSTRAINT mokyu_meta_singleton_check CHECK (singleton)
);

CREATE TABLE public.multipart_files (
    id uuid DEFAULT uuidv7() NOT NULL,
    upload_id uuid,
    storage_id uuid NOT NULL,
    source_id uuid,
    reclaim_pending boolean DEFAULT false NOT NULL,
    state text DEFAULT 'open'::text NOT NULL,
    node_id uuid,
    CONSTRAINT multipart_files_state_check CHECK ((state = ANY (ARRAY['open'::text, 'sealed'::text, 'deleting'::text])))
);

CREATE TABLE public.node_boots (
    node_id uuid NOT NULL,
    credential_generation bigint NOT NULL,
    boot_id uuid NOT NULL,
    created_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    retired_at timestamp with time zone,
    CONSTRAINT node_boots_credential_generation_check CHECK ((credential_generation > 0))
);

CREATE TABLE public.node_read_permits (
    token uuid NOT NULL,
    read_id uuid NOT NULL
);

CREATE TABLE public.node_read_sources (
    window_id uuid NOT NULL,
    kind text NOT NULL,
    location_id uuid NOT NULL,
    CONSTRAINT node_read_sources_kind_check CHECK ((kind = ANY (ARRAY['chunk'::text, 'pack'::text, 'raw'::text, 'fragment'::text])))
);

CREATE TABLE public.node_read_windows (
    id uuid DEFAULT uuidv7() NOT NULL,
    read_id uuid NOT NULL,
    offset_bytes bigint NOT NULL,
    end_bytes bigint NOT NULL,
    mapping jsonb NOT NULL,
    CONSTRAINT node_read_windows_check CHECK ((end_bytes > offset_bytes)),
    CONSTRAINT node_read_windows_mapping_check CHECK ((octet_length((mapping)::text) <= 4194304)),
    CONSTRAINT node_read_windows_offset_bytes_check CHECK ((offset_bytes >= 0))
);

CREATE TABLE public.node_reads (
    read_id uuid NOT NULL,
    node_id uuid NOT NULL,
    credential_generation bigint NOT NULL,
    boot_id uuid NOT NULL,
    fence uuid NOT NULL,
    operation_id uuid NOT NULL,
    request_hash bytea NOT NULL,
    payload_revision bigint NOT NULL,
    offset_bytes bigint NOT NULL,
    end_bytes bigint NOT NULL,
    replica_allowed boolean DEFAULT false NOT NULL,
    CONSTRAINT node_reads_check CHECK ((end_bytes > offset_bytes)),
    CONSTRAINT node_reads_credential_generation_check CHECK ((credential_generation > 0)),
    CONSTRAINT node_reads_offset_bytes_check CHECK ((offset_bytes >= 0)),
    CONSTRAINT node_reads_payload_revision_check CHECK ((payload_revision > 0)),
    CONSTRAINT node_reads_request_hash_check CHECK ((octet_length(request_hash) = 32))
);

CREATE TABLE public.node_reports (
    node_id uuid NOT NULL,
    boot_id uuid NOT NULL,
    sequence bigint DEFAULT 0 NOT NULL,
    started_at timestamp with time zone NOT NULL,
    sampled_at timestamp with time zone,
    received_at timestamp with time zone DEFAULT now() NOT NULL,
    metadata jsonb DEFAULT '{}'::jsonb NOT NULL,
    status jsonb DEFAULT '{}'::jsonb NOT NULL,
    CONSTRAINT node_reports_metadata_check CHECK ((octet_length((metadata)::text) <= 65536)),
    CONSTRAINT node_reports_sequence_check CHECK ((sequence >= 0)),
    CONSTRAINT node_reports_status_check CHECK ((octet_length((status)::text) <= 32768))
);

CREATE TABLE public.node_runtime_samples (
    node_id uuid NOT NULL,
    boot_id uuid NOT NULL,
    sequence bigint NOT NULL,
    received_at timestamp with time zone DEFAULT now() NOT NULL,
    uptime_seconds bigint NOT NULL,
    data jsonb NOT NULL,
    CONSTRAINT node_runtime_samples_data_check CHECK ((octet_length((data)::text) <= 16384)),
    CONSTRAINT node_runtime_samples_sequence_check CHECK ((sequence > 0)),
    CONSTRAINT node_runtime_samples_uptime_seconds_check CHECK ((uptime_seconds >= 0))
);

CREATE TABLE public.node_sessions (
    node_id uuid NOT NULL,
    credential_generation bigint NOT NULL,
    boot_id uuid NOT NULL,
    fence uuid NOT NULL,
    epoch bigint NOT NULL,
    connection_id uuid NOT NULL,
    expires_at timestamp with time zone NOT NULL,
    sequence bigint DEFAULT 0 NOT NULL,
    draining boolean DEFAULT false NOT NULL,
    CONSTRAINT node_sessions_credential_generation_check CHECK ((credential_generation > 0)),
    CONSTRAINT node_sessions_epoch_check CHECK ((epoch > 0)),
    CONSTRAINT node_sessions_sequence_check CHECK ((sequence >= 0))
);

CREATE TABLE public.node_writes (
    node_id uuid NOT NULL,
    operation_id uuid NOT NULL,
    credential_generation bigint NOT NULL,
    boot_id uuid NOT NULL,
    fence uuid NOT NULL,
    request_hash bytea NOT NULL,
    stream_id uuid,
    lease_id uuid,
    storage_id uuid,
    block_key_id uuid,
    profile jsonb NOT NULL,
    state text DEFAULT 'active'::text NOT NULL,
    created_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    completed_at timestamp with time zone,
    CONSTRAINT node_writes_check CHECK (((state = 'active'::text) = (completed_at IS NULL))),
    CONSTRAINT node_writes_credential_generation_check CHECK ((credential_generation > 0)),
    CONSTRAINT node_writes_profile_check CHECK (((jsonb_typeof(profile) = 'object'::text) AND (octet_length((profile)::text) <= 32768))),
    CONSTRAINT node_writes_request_hash_check CHECK ((octet_length(request_hash) = 32)),
    CONSTRAINT node_writes_state_check CHECK ((state = ANY (ARRAY['active'::text, 'aborted'::text])))
);

CREATE TABLE public.nodes (
    id uuid DEFAULT uuidv7() NOT NULL,
    name text NOT NULL,
    role text CONSTRAINT nodes_kind_not_null NOT NULL,
    credential_encrypted bytea,
    enabled boolean DEFAULT true NOT NULL,
    last_seen_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    status text DEFAULT 'registered'::text NOT NULL,
    recovery_required boolean DEFAULT false NOT NULL,
    lease_expires_at timestamp with time zone,
    credential_generation bigint DEFAULT 1 NOT NULL,
    CONSTRAINT nodes_credential_generation_check CHECK ((credential_generation > 0)),
    CONSTRAINT nodes_name_check CHECK (((octet_length(name) >= 1) AND (octet_length(name) <= 128))),
    CONSTRAINT nodes_role_check CHECK ((role = ANY (ARRAY['control'::text, 'worker'::text, 'data'::text])))
);

CREATE TABLE public.nonce_counters (
    block_key_id uuid NOT NULL,
    type_tag bigint NOT NULL,
    reserved_id bigint DEFAULT 0 NOT NULL,
    CONSTRAINT nonce_counters_reserved_id_check CHECK ((reserved_id >= 0)),
    CONSTRAINT nonce_counters_type_tag_check CHECK (((type_tag >= 0) AND (type_tag <= 31)))
);

CREATE TABLE public.nonce_observations (
    block_key_id uuid NOT NULL,
    data jsonb NOT NULL,
    revision bigint NOT NULL,
    CONSTRAINT nonce_observations_revision_check CHECK ((revision > 0))
);

CREATE TABLE public.objects (
    bucket_id uuid NOT NULL,
    object_key text CONSTRAINT objects_key_not_null NOT NULL COLLATE pg_catalog."C",
    stream_id uuid,
    write_token uuid CONSTRAINT objects_write_epoch_not_null NOT NULL,
    catalog_size bigint,
    catalog_modified timestamp with time zone,
    catalog_type text,
    catalog_category text,
    catalog_public boolean,
    CONSTRAINT objects_key_check CHECK (((octet_length(object_key) >= 1) AND (octet_length(object_key) <= 1024)))
)
WITH (autovacuum_vacuum_scale_factor='0.05', autovacuum_analyze_scale_factor='0.02');

CREATE TABLE public.pack_access_windows (
    pack_id uuid NOT NULL,
    window_start timestamp with time zone NOT NULL,
    downloads bigint DEFAULT 0 NOT NULL,
    downloaded_bytes bigint DEFAULT 0 NOT NULL,
    partial_downloads bigint DEFAULT 0 NOT NULL,
    useful_bytes bigint DEFAULT 0 NOT NULL,
    partial_bytes bigint DEFAULT 0 NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);

CREATE TABLE public.pack_changes (
    pack_id uuid NOT NULL,
    generation bigint DEFAULT 1 NOT NULL,
    next_check_at timestamp with time zone DEFAULT now() NOT NULL,
    reason text DEFAULT 'reuse'::text NOT NULL,
    CONSTRAINT pack_changes_reason_check CHECK ((reason = ANY (ARRAY['reuse'::text, 'reclaim'::text])))
);

CREATE TABLE public.pack_inputs (
    task_id uuid NOT NULL,
    chunk_id uuid NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);

CREATE TABLE public.pack_maintenance (
    stream_id uuid NOT NULL,
    cursor bigint DEFAULT 0 NOT NULL,
    generation bigint DEFAULT 1 NOT NULL,
    next_check_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    reason text DEFAULT 'pack'::text NOT NULL
);

CREATE TABLE public.pack_member_access_windows (
    window_start timestamp with time zone NOT NULL,
    pack_id uuid NOT NULL,
    chunk_id uuid NOT NULL,
    downloads bigint DEFAULT 0 NOT NULL
);

CREATE TABLE public.pack_members (
    pack_id uuid NOT NULL,
    ordinal integer NOT NULL,
    chunk_id uuid NOT NULL,
    offset_bytes bigint NOT NULL,
    CONSTRAINT pack_members_offset_bytes_check CHECK ((offset_bytes >= 0)),
    CONSTRAINT pack_members_ordinal_check CHECK ((ordinal >= 0))
);

CREATE TABLE public.packs (
    id uuid DEFAULT uuidv7() NOT NULL,
    encryption_algorithm text CONSTRAINT packs_algorithm_not_null NOT NULL,
    raw_size bigint NOT NULL,
    stored_size bigint,
    pack_compressed boolean DEFAULT false CONSTRAINT packs_compressed_not_null NOT NULL,
    nonce bytea,
    digest bytea,
    member_count integer NOT NULL,
    state text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    unreferenced_at timestamp with time zone,
    deleted_at timestamp with time zone,
    owner_task uuid,
    range_checked_at timestamp with time zone,
    stored_at timestamp with time zone,
    storage_id uuid CONSTRAINT packs_storage_target_id_not_null NOT NULL,
    block_key_id uuid,
    pack_fingerprint bytea NOT NULL,
    quarantined_at timestamp with time zone,
    quarantine_code text,
    quarantine_revision bigint DEFAULT 0 NOT NULL,
    nonce_id bigint,
    CONSTRAINT packs_algorithm_check CHECK ((encryption_algorithm = ANY (ARRAY['none'::text, 'aes-256-gcm'::text, 'chacha20-poly1305'::text]))),
    CONSTRAINT packs_block_key_check CHECK ((((encryption_algorithm = 'none'::text) AND (block_key_id IS NULL)) OR ((encryption_algorithm <> 'none'::text) AND (block_key_id IS NOT NULL)))),
    CONSTRAINT packs_digest_check CHECK (((digest IS NULL) OR (octet_length(digest) = 32))),
    CONSTRAINT packs_fingerprint_size CHECK ((octet_length(pack_fingerprint) = 32)),
    CONSTRAINT packs_member_count_check CHECK ((member_count >= 2)),
    CONSTRAINT packs_nonce_check CHECK (((nonce IS NULL) OR (octet_length(nonce) = 12))),
    CONSTRAINT packs_nonce_id CHECK ((((encryption_algorithm = 'none'::text) AND (nonce_id IS NULL) AND (nonce IS NULL) AND (block_key_id IS NULL)) OR ((encryption_algorithm <> 'none'::text) AND (block_key_id IS NOT NULL) AND (((nonce IS NULL) AND (nonce_id IS NULL) AND (state <> 'ready'::text)) OR ((nonce IS NOT NULL) AND (nonce_id IS NOT NULL) AND (nonce_id > 0)))))),
    CONSTRAINT packs_quarantine CHECK (((quarantined_at IS NULL) = (quarantine_code IS NULL))),
    CONSTRAINT packs_quarantine_revision CHECK ((quarantine_revision >= 0)),
    CONSTRAINT packs_raw_size_check CHECK ((raw_size > 0)),
    CONSTRAINT packs_state_check CHECK ((state = ANY (ARRAY['preparing'::text, 'ready'::text, 'retired'::text, 'deleting'::text, 'deleted'::text])))
);

CREATE TABLE public.parts (
    upload_id uuid NOT NULL,
    part_number integer NOT NULL,
    stream_id uuid,
    write_token uuid CONSTRAINT parts_write_epoch_not_null NOT NULL,
    quota_size bigint DEFAULT 0 NOT NULL,
    remote_node_id uuid,
    remote_etag text,
    remote_token uuid,
    remote_expires_at timestamp with time zone,
    remote_retry_at timestamp with time zone DEFAULT now() NOT NULL,
    remote_error text,
    remote_cached boolean DEFAULT false NOT NULL,
    CONSTRAINT parts_part_number_check CHECK (((part_number >= 1) AND (part_number <= 10000))),
    CONSTRAINT parts_quota_size_check CHECK ((quota_size >= 0))
)
WITH (autovacuum_vacuum_scale_factor='0.05', autovacuum_analyze_scale_factor='0.02');

CREATE TABLE public.pending_uploads (
    chunk_id uuid NOT NULL,
    stream_id uuid,
    offset_bytes bigint NOT NULL,
    staged_size bigint CONSTRAINT pending_uploads_cache_size_not_null NOT NULL,
    source_pack_id uuid,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    next_retry_at timestamp with time zone NOT NULL,
    attempts integer DEFAULT 0 NOT NULL,
    last_error text,
    owner_task uuid,
    staging_storage_id uuid,
    staging_object_key text,
    node_id uuid,
    encoding_policy jsonb,
    CONSTRAINT pending_uploads_cache_size_check CHECK ((staged_size > 0)),
    CONSTRAINT pending_uploads_offset_bytes_check CHECK ((offset_bytes >= 0))
);

CREATE TABLE public.project_members (
    user_id uuid NOT NULL,
    project_id uuid NOT NULL,
    role text NOT NULL,
    scope text NOT NULL,
    CONSTRAINT project_members_role_check CHECK ((role = ANY (ARRAY['reader'::text, 'writer'::text, 'maintainer'::text]))),
    CONSTRAINT project_members_scope_check CHECK ((scope = ANY (ARRAY['all'::text, 'selected'::text])))
);

CREATE TABLE public.projects (
    id uuid DEFAULT uuidv7() NOT NULL,
    name text NOT NULL,
    description text DEFAULT ''::text NOT NULL,
    builtin boolean DEFAULT false NOT NULL,
    allow_bucket_create boolean DEFAULT false NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT projects_description_check CHECK ((octet_length(description) <= 2000)),
    CONSTRAINT projects_name_check CHECK (((octet_length(name) >= 1) AND (octet_length(name) <= 128)))
);

CREATE TABLE public.quota_reservations (
    id uuid DEFAULT uuidv7() NOT NULL,
    bucket_id uuid NOT NULL,
    object_key text NOT NULL COLLATE pg_catalog."C",
    stream_id uuid,
    upload_id uuid,
    result_stream_id uuid,
    closed boolean DEFAULT false NOT NULL,
    replaced_stream_id uuid,
    credit_bytes bigint DEFAULT 0 NOT NULL,
    logical_bytes bigint DEFAULT 0 NOT NULL,
    inflight_bytes bigint DEFAULT 0 NOT NULL,
    CONSTRAINT quota_reservations_check CHECK (((stream_id IS NULL) <> (upload_id IS NULL))),
    CONSTRAINT quota_reservations_credit_bytes_check CHECK ((credit_bytes >= 0)),
    CONSTRAINT quota_reservations_inflight_bytes_check CHECK ((inflight_bytes >= 0)),
    CONSTRAINT quota_reservations_logical_bytes_check CHECK ((logical_bytes >= 0))
);

CREATE TABLE public.quota_scopes (
    scope text CONSTRAINT quota_accounts_kind_not_null NOT NULL,
    scope_id uuid CONSTRAINT quota_accounts_id_not_null NOT NULL,
    used_bytes bigint DEFAULT 0 CONSTRAINT quota_accounts_used_bytes_not_null NOT NULL,
    reserved_bytes bigint DEFAULT 0 CONSTRAINT quota_accounts_reserved_bytes_not_null NOT NULL,
    inflight_bytes bigint DEFAULT 0 CONSTRAINT quota_accounts_inflight_bytes_not_null NOT NULL,
    object_count bigint DEFAULT 0 CONSTRAINT quota_accounts_object_count_not_null NOT NULL,
    bucket_count bigint DEFAULT 0 CONSTRAINT quota_accounts_bucket_count_not_null NOT NULL,
    logical_bytes_limit bigint,
    inflight_bytes_limit bigint,
    bucket_count_limit bigint,
    cached_bytes bigint DEFAULT 0 NOT NULL,
    cached_compressed boolean DEFAULT false NOT NULL,
    CONSTRAINT quota_accounts_bucket_count_check CHECK ((bucket_count >= 0)),
    CONSTRAINT quota_accounts_bucket_limit_check CHECK ((bucket_count_limit >= 0)),
    CONSTRAINT quota_accounts_byte_limit_check CHECK ((logical_bytes_limit >= 0)),
    CONSTRAINT quota_accounts_inflight_bytes_check CHECK ((inflight_bytes >= 0)),
    CONSTRAINT quota_accounts_inflight_limit_check CHECK ((inflight_bytes_limit >= 0)),
    CONSTRAINT quota_accounts_kind_check CHECK ((scope = ANY (ARRAY['project'::text, 'bucket'::text]))),
    CONSTRAINT quota_accounts_object_count_check CHECK ((object_count >= 0)),
    CONSTRAINT quota_accounts_reserved_bytes_check CHECK ((reserved_bytes >= 0)),
    CONSTRAINT quota_accounts_used_bytes_check CHECK ((used_bytes >= 0)),
    CONSTRAINT quota_scopes_cached_bytes_check CHECK ((cached_bytes >= 0))
);

CREATE TABLE public.quota_writes (
    stream_id uuid NOT NULL,
    reservation_id uuid NOT NULL,
    allocated_bytes bigint DEFAULT 0 NOT NULL,
    contribution_bytes bigint DEFAULT 0 NOT NULL,
    CONSTRAINT quota_writes_allocated_bytes_check CHECK ((allocated_bytes >= 0)),
    CONSTRAINT quota_writes_contribution_bytes_check CHECK ((contribution_bytes >= 0))
);

CREATE TABLE public.raw_processing (
    stream_id uuid NOT NULL,
    source_id uuid NOT NULL,
    payload_revision bigint NOT NULL,
    content_hash bytea,
    source_revision bigint,
    token uuid,
    lease_expires_at timestamp with time zone,
    next_attempt_at timestamp with time zone DEFAULT now() NOT NULL,
    attempts integer DEFAULT 0 NOT NULL,
    last_error text,
    CONSTRAINT raw_processing_content_hash_check CHECK (((content_hash IS NULL) OR (octet_length(content_hash) = 32)))
);

CREATE TABLE public.raw_ranges (
    stream_id uuid NOT NULL,
    offset_bytes bigint NOT NULL,
    length bigint NOT NULL,
    source_id uuid NOT NULL,
    source_offset bigint DEFAULT 0 NOT NULL,
    CONSTRAINT raw_ranges_length_check CHECK ((length >= 0)),
    CONSTRAINT raw_ranges_offset_bytes_check CHECK ((offset_bytes >= 0)),
    CONSTRAINT raw_ranges_source_offset_check CHECK ((source_offset >= 0))
);

CREATE TABLE public.raw_sources (
    id uuid DEFAULT uuidv7() NOT NULL,
    storage_id uuid NOT NULL,
    owner_stream uuid,
    medium text DEFAULT 'local'::text NOT NULL,
    durability text DEFAULT 'durable'::text NOT NULL,
    boot_id uuid,
    state text DEFAULT 'writing'::text NOT NULL,
    size bigint DEFAULT 0 NOT NULL,
    digest bytea,
    content_hash bytea,
    upload_id text,
    revision bigint DEFAULT 1 NOT NULL,
    profile_revision bigint NOT NULL,
    quarantined_at timestamp with time zone,
    quarantine_code text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    unreferenced_at timestamp with time zone DEFAULT now() NOT NULL,
    pending_upload boolean DEFAULT false NOT NULL,
    processing_task uuid,
    CONSTRAINT raw_sources_check CHECK (((state <> 'ready'::text) OR (digest IS NOT NULL))),
    CONSTRAINT raw_sources_check1 CHECK (((medium = 'memory'::text) = (boot_id IS NOT NULL))),
    CONSTRAINT raw_sources_content_hash_check CHECK (((content_hash IS NULL) OR (octet_length(content_hash) = 32))),
    CONSTRAINT raw_sources_digest_check CHECK (((digest IS NULL) OR (octet_length(digest) = 16))),
    CONSTRAINT raw_sources_durability_check CHECK ((durability = ANY (ARRAY['durable'::text, 'buffered'::text, 'volatile'::text]))),
    CONSTRAINT raw_sources_medium_check CHECK ((medium = ANY (ARRAY['local'::text, 'memory'::text, 'shared_s3'::text]))),
    CONSTRAINT raw_sources_profile_revision_check CHECK ((profile_revision > 0)),
    CONSTRAINT raw_sources_revision_check CHECK ((revision > 0)),
    CONSTRAINT raw_sources_size_check CHECK ((size >= 0)),
    CONSTRAINT raw_sources_state_check CHECK ((state = ANY (ARRAY['writing'::text, 'ready'::text, 'unavailable'::text, 'deleting'::text])))
);

CREATE TABLE public.raw_uploads (
    source_id uuid NOT NULL,
    token uuid,
    deduplicate boolean NOT NULL,
    background boolean NOT NULL,
    content_hash bytea,
    next_attempt_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    attempts integer DEFAULT 0 NOT NULL,
    last_error text,
    node_id uuid,
    lease_expires_at timestamp with time zone
);

CREATE TABLE public.runtime_samples (
    sampled_at timestamp with time zone CONSTRAINT runtime_history_at_not_null NOT NULL,
    data jsonb CONSTRAINT runtime_history_data_not_null NOT NULL
);

CREATE TABLE public.runtime_settings (
    section text NOT NULL,
    value jsonb NOT NULL,
    revision bigint DEFAULT 1 NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_by_user_id uuid,
    CONSTRAINT runtime_settings_revision_check CHECK ((revision > 0)),
    CONSTRAINT runtime_settings_section_check CHECK ((section = ANY (ARRAY['web'::text, 'requests'::text, 'uploads'::text, 'metrics'::text, 'maintenance'::text, 'io'::text]))),
    CONSTRAINT runtime_settings_value_check CHECK ((jsonb_typeof(value) = 'object'::text))
);

CREATE TABLE public.s3_credential_grants (
    access_key text CONSTRAINT grants_access_key_not_null NOT NULL,
    bucket_id uuid CONSTRAINT grants_bucket_id_not_null NOT NULL,
    actions text[] CONSTRAINT grants_actions_not_null NOT NULL,
    CONSTRAINT grants_actions_check CHECK ((actions <@ ARRAY['bucket.list'::text, 'object.read'::text, 'object.write'::text, 'object.delete'::text, 'object.acl'::text, 'bucket.settings'::text, 'storage.inspect'::text]))
);

CREATE TABLE public.s3_credentials (
    access_key_id text CONSTRAINT credentials_access_key_not_null NOT NULL,
    secret_encrypted bytea CONSTRAINT credentials_secret_encrypted_not_null NOT NULL,
    enabled boolean DEFAULT true CONSTRAINT credentials_enabled_not_null NOT NULL,
    created_at timestamp with time zone DEFAULT now() CONSTRAINT credentials_created_at_not_null NOT NULL,
    project_id uuid DEFAULT '00000000-0000-0000-0000-000000000001'::uuid CONSTRAINT credentials_project_id_not_null NOT NULL,
    authorization_revision bigint DEFAULT 0 CONSTRAINT credentials_authorization_revision_not_null NOT NULL,
    label text DEFAULT ''::text CONSTRAINT credentials_label_not_null NOT NULL,
    expires_at timestamp with time zone,
    last_used_at timestamp with time zone,
    created_by uuid,
    CONSTRAINT credentials_label_check CHECK ((octet_length(label) <= 128))
);

CREATE TABLE public.session_refresh_tokens (
    id uuid DEFAULT uuidv7() NOT NULL,
    session_id uuid NOT NULL,
    family_id uuid NOT NULL,
    token_hash bytea NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    last_used_at timestamp with time zone DEFAULT now() NOT NULL,
    expires_at timestamp with time zone NOT NULL,
    revoked_at timestamp with time zone,
    replaced_by uuid,
    CONSTRAINT session_refresh_tokens_token_hash_check CHECK ((octet_length(token_hash) = 32))
);

CREATE TABLE public.sessions (
    token_hash bytea NOT NULL,
    user_id uuid NOT NULL,
    csrf_hash bytea NOT NULL,
    expires_at timestamp with time zone NOT NULL,
    id uuid DEFAULT uuidv7() NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    last_seen_at timestamp with time zone DEFAULT now() NOT NULL,
    reauthenticated_at timestamp with time zone DEFAULT now() NOT NULL,
    auth_revision bigint DEFAULT 0 NOT NULL,
    user_agent text DEFAULT ''::text NOT NULL,
    CONSTRAINT sessions_csrf_hash_check CHECK ((octet_length(csrf_hash) = 32)),
    CONSTRAINT sessions_token_hash_check CHECK ((octet_length(token_hash) = 32)),
    CONSTRAINT sessions_user_agent_check CHECK ((octet_length(user_agent) <= 512))
)
WITH (autovacuum_vacuum_scale_factor='0.05', autovacuum_analyze_scale_factor='0.02');

CREATE TABLE public.setup_tokens (
    singleton boolean DEFAULT true CONSTRAINT manage_setup_singleton_not_null NOT NULL,
    token_hash bytea CONSTRAINT manage_setup_token_hash_not_null NOT NULL,
    created_at timestamp with time zone DEFAULT now() CONSTRAINT manage_setup_created_at_not_null NOT NULL,
    expires_at timestamp with time zone DEFAULT (now() + '01:00:00'::interval) CONSTRAINT admin_setup_expires_at_not_null NOT NULL,
    CONSTRAINT manage_setup_singleton_check CHECK (singleton),
    CONSTRAINT manage_setup_token_hash_check CHECK ((octet_length(token_hash) = 32))
);

CREATE TABLE public.staging_objects (
    storage_id uuid NOT NULL,
    object_key text NOT NULL,
    bytes bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT staging_objects_bytes_check CHECK ((bytes > 0))
);

CREATE TABLE public.storage_maintenance (
    storage_id uuid NOT NULL,
    policy text NOT NULL,
    last_scheduled_at timestamp with time zone NOT NULL
);

CREATE TABLE public.storage_permits (
    token uuid CONSTRAINT store_permits_token_not_null NOT NULL,
    storage_id uuid CONSTRAINT store_permits_store_id_not_null NOT NULL,
    direction text CONSTRAINT store_permits_direction_not_null NOT NULL,
    holder_id uuid CONSTRAINT store_permits_holder_not_null NOT NULL,
    acquired_at timestamp with time zone DEFAULT now() CONSTRAINT store_permits_acquired_at_not_null NOT NULL,
    expires_at timestamp with time zone CONSTRAINT store_permits_expires_at_not_null NOT NULL,
    CONSTRAINT storage_permits_direction_check CHECK ((direction = ANY (ARRAY['read'::text, 'upload'::text, 'control'::text, 'pack'::text, 'integrity'::text, 'maintenance'::text, 'migration'::text, 'staging'::text])))
);

CREATE TABLE public.storage_profiles (
    storage_id uuid NOT NULL,
    revision bigint DEFAULT 1 NOT NULL,
    connect_timeout text DEFAULT '5s'::text NOT NULL,
    request_timeout text DEFAULT '30s'::text NOT NULL,
    max_retries integer DEFAULT 3 NOT NULL,
    retry_timeout text DEFAULT '120s'::text NOT NULL,
    priority_aging text DEFAULT '30s'::text NOT NULL,
    min_storage_duration text DEFAULT '0s'::text NOT NULL,
    unreferenced_grace text DEFAULT '48h'::text NOT NULL,
    deleted_chunk_retention text DEFAULT '7d'::text NOT NULL,
    upload_retention text DEFAULT '24h'::text NOT NULL,
    cache_enabled boolean DEFAULT false NOT NULL,
    cache_soft_limit text,
    cache_entries bigint,
    upload_cache boolean DEFAULT false NOT NULL,
    upload_cache_size text,
    compression_enabled boolean DEFAULT false NOT NULL,
    compression_strategy text DEFAULT 'always'::text NOT NULL,
    compression_level integer DEFAULT 6 NOT NULL,
    compression_sample_level integer DEFAULT 3 NOT NULL,
    compression_min_savings_percent integer DEFAULT 2 NOT NULL,
    compression_min_savings_bytes bigint DEFAULT 256 NOT NULL,
    pack_enabled boolean DEFAULT false NOT NULL,
    pack_max_size text DEFAULT '32MiB'::text NOT NULL,
    pack_upload_flush_timeout text DEFAULT '60s'::text NOT NULL,
    pack_interval text DEFAULT '1h'::text NOT NULL,
    pack_reuse_interval text DEFAULT '5m'::text NOT NULL,
    pack_reclaim_interval text DEFAULT '30m'::text NOT NULL,
    pack_repack_interval text DEFAULT '24h'::text NOT NULL,
    pack_repack_cooldown text DEFAULT '1h'::text NOT NULL,
    pack_reclaim_min_savings_bytes text DEFAULT '4MiB'::text NOT NULL,
    pack_range_enabled boolean DEFAULT true NOT NULL,
    pack_range_interval text DEFAULT '15m'::text NOT NULL,
    pack_range_window text DEFAULT '24h'::text NOT NULL,
    pack_range_min_downloads integer DEFAULT 8 NOT NULL,
    pack_range_min_savings_percent integer DEFAULT 50 NOT NULL,
    pack_range_min_savings_bytes text DEFAULT '64MiB'::text NOT NULL,
    pack_range_repack_after text DEFAULT '30d'::text NOT NULL,
    pack_range_repack_window text DEFAULT '7d'::text NOT NULL,
    pack_range_repack_retry_interval text DEFAULT '7d'::text NOT NULL,
    pack_concurrency integer DEFAULT 1 NOT NULL,
    integrity_concurrency integer DEFAULT 1 NOT NULL,
    integrity_rps bigint,
    integrity_bandwidth text,
    integrity_batch_timeout text DEFAULT '30s'::text NOT NULL,
    read_concurrency integer,
    upload_concurrency integer,
    control_concurrency integer,
    pack_byte_budget text,
    integrity_byte_budget text,
    maintenance_concurrency integer,
    maintenance_byte_budget text,
    migration_concurrency integer,
    migration_byte_budget text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    cdc_enabled boolean DEFAULT true NOT NULL,
    deduplication_enabled boolean DEFAULT false NOT NULL,
    multipart_idle_timeout text DEFAULT '24h'::text NOT NULL,
    min_free_space text,
    write_processing text DEFAULT 'inline'::text NOT NULL,
    upload_cache_overflow text DEFAULT 'fallback'::text NOT NULL,
    write_durability text DEFAULT 'durable'::text NOT NULL,
    write_flush_interval bigint DEFAULT 0 NOT NULL,
    write_backlog_max_age bigint DEFAULT 300 NOT NULL,
    background_scheduling text DEFAULT 'balanced'::text NOT NULL,
    public_cache_validation text DEFAULT 'strict'::text NOT NULL,
    public_cache_refresh_interval text DEFAULT '1s'::text NOT NULL,
    public_cache_max_age text DEFAULT '5s'::text NOT NULL,
    read_source_validation text DEFAULT 'strict'::text NOT NULL,
    source_validation_max_age text DEFAULT '5s'::text NOT NULL,
    read_integrity text DEFAULT 'inline'::text NOT NULL,
    file_delivery text DEFAULT 'application'::text NOT NULL,
    file_delivery_prefix text,
    cors_cache_enabled boolean DEFAULT false NOT NULL,
    CONSTRAINT storage_background_scheduling CHECK ((background_scheduling = ANY (ARRAY['balanced'::text, 'foreground'::text]))),
    CONSTRAINT storage_delivery_prefix CHECK ((((file_delivery = 'application'::text) AND (file_delivery_prefix IS NULL)) OR ((file_delivery <> 'application'::text) AND (file_delivery_prefix IS NOT NULL)))),
    CONSTRAINT storage_file_delivery CHECK ((file_delivery = ANY (ARRAY['application'::text, 'x_accel_redirect'::text, 'x_sendfile'::text]))),
    CONSTRAINT storage_profiles_cache_entries_check CHECK (((cache_entries IS NULL) OR (cache_entries > 0))),
    CONSTRAINT storage_profiles_compression_min_savings_bytes_check CHECK ((compression_min_savings_bytes >= 0)),
    CONSTRAINT storage_profiles_compression_min_savings_percent_check CHECK (((compression_min_savings_percent >= 0) AND (compression_min_savings_percent <= 100))),
    CONSTRAINT storage_profiles_compression_strategy_check CHECK ((compression_strategy = ANY (ARRAY['never'::text, 'always'::text, 'sample'::text]))),
    CONSTRAINT storage_profiles_control_concurrency_check CHECK (((control_concurrency IS NULL) OR (control_concurrency > 0))),
    CONSTRAINT storage_profiles_integrity_concurrency_check CHECK ((integrity_concurrency > 0)),
    CONSTRAINT storage_profiles_integrity_rps_check CHECK (((integrity_rps IS NULL) OR (integrity_rps > 0))),
    CONSTRAINT storage_profiles_maintenance_concurrency_check CHECK (((maintenance_concurrency IS NULL) OR (maintenance_concurrency > 0))),
    CONSTRAINT storage_profiles_max_retries_check CHECK (((max_retries >= 0) AND (max_retries <= 100))),
    CONSTRAINT storage_profiles_migration_concurrency_check CHECK (((migration_concurrency IS NULL) OR (migration_concurrency > 0))),
    CONSTRAINT storage_profiles_pack_concurrency_check CHECK ((pack_concurrency > 0)),
    CONSTRAINT storage_profiles_pack_range_min_downloads_check CHECK ((pack_range_min_downloads > 0)),
    CONSTRAINT storage_profiles_pack_range_min_savings_percent_check CHECK (((pack_range_min_savings_percent >= 1) AND (pack_range_min_savings_percent <= 100))),
    CONSTRAINT storage_profiles_read_concurrency_check CHECK (((read_concurrency IS NULL) OR (read_concurrency > 0))),
    CONSTRAINT storage_profiles_revision_check CHECK ((revision > 0)),
    CONSTRAINT storage_profiles_upload_concurrency_check CHECK (((upload_concurrency IS NULL) OR (upload_concurrency > 0))),
    CONSTRAINT storage_profiles_write_durability_check CHECK ((write_durability = ANY (ARRAY['durable'::text, 'buffered'::text, 'volatile'::text]))),
    CONSTRAINT storage_public_cache_validation CHECK ((public_cache_validation = ANY (ARRAY['strict'::text, 'background'::text]))),
    CONSTRAINT storage_read_integrity CHECK ((read_integrity = ANY (ARRAY['inline'::text, 'background'::text, 'off'::text]))),
    CONSTRAINT storage_source_validation CHECK ((read_source_validation = ANY (ARRAY['strict'::text, 'background'::text]))),
    CONSTRAINT storage_upload_overflow_check CHECK ((upload_cache_overflow = ANY (ARRAY['fallback'::text, 'reject'::text])))
);

CREATE TABLE public.storage_rate_state (
    storage_id uuid NOT NULL,
    metric text NOT NULL,
    tokens numeric(30,6) DEFAULT 0 NOT NULL,
    replenished_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT storage_rate_state_metric_check CHECK ((metric = ANY (ARRAY['integrity_rps'::text, 'integrity_bandwidth'::text]))),
    CONSTRAINT storage_rate_state_tokens_check CHECK ((tokens >= (0)::numeric))
);

CREATE TABLE public.storages (
    id uuid DEFAULT uuidv7() CONSTRAINT storage_targets_id_not_null NOT NULL,
    name text CONSTRAINT storage_targets_name_not_null NOT NULL,
    type text CONSTRAINT storage_targets_type_not_null NOT NULL,
    path text,
    endpoint_url text,
    remote_bucket_name text,
    region text,
    prefix text DEFAULT ''::text CONSTRAINT storage_targets_prefix_not_null NOT NULL,
    access_key_encrypted bytea,
    secret_key_encrypted bytea,
    settings jsonb DEFAULT '{}'::jsonb CONSTRAINT storage_targets_settings_not_null NOT NULL,
    identity_marker jsonb DEFAULT '{}'::jsonb CONSTRAINT storage_targets_metadata_not_null NOT NULL,
    enabled boolean DEFAULT true CONSTRAINT storage_targets_enabled_not_null NOT NULL,
    created_at timestamp with time zone DEFAULT now() CONSTRAINT storage_targets_created_at_not_null NOT NULL,
    updated_at timestamp with time zone DEFAULT now() CONSTRAINT storage_targets_updated_at_not_null NOT NULL,
    read_concurrency integer,
    upload_concurrency integer,
    control_concurrency integer,
    block_key_id uuid,
    CONSTRAINT storage_targets_check CHECK ((((type = 'local'::text) AND (path IS NOT NULL) AND (endpoint_url IS NULL) AND (remote_bucket_name IS NULL) AND (region IS NULL) AND (access_key_encrypted IS NULL) AND (secret_key_encrypted IS NULL)) OR ((type = 's3'::text) AND (path IS NULL) AND (endpoint_url IS NOT NULL) AND (remote_bucket_name IS NOT NULL) AND (region IS NOT NULL) AND (access_key_encrypted IS NOT NULL) AND (secret_key_encrypted IS NOT NULL)))),
    CONSTRAINT storage_targets_name_check CHECK (((octet_length(name) >= 1) AND (octet_length(name) <= 128))),
    CONSTRAINT storage_targets_type_check CHECK ((type = ANY (ARRAY['local'::text, 's3'::text]))),
    CONSTRAINT stores_control_concurrency_check CHECK (((control_concurrency IS NULL) OR (control_concurrency > 0))),
    CONSTRAINT stores_current_block_key CHECK (((block_key_id IS NULL) OR (block_key_id <> '00000000-0000-0000-0000-000000000000'::uuid))),
    CONSTRAINT stores_read_concurrency_check CHECK (((read_concurrency IS NULL) OR (read_concurrency > 0))),
    CONSTRAINT stores_upload_concurrency_check CHECK (((upload_concurrency IS NULL) OR (upload_concurrency > 0)))
);

CREATE TABLE public.stream_leases (
    id uuid DEFAULT uuidv7() NOT NULL,
    stream_id uuid NOT NULL,
    lease_type text NOT NULL,
    expires_at timestamp with time zone NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT stream_leases_lease_type_check CHECK ((lease_type = ANY (ARRAY['read'::text, 'write'::text])))
);

CREATE TABLE public.streams (
    id uuid DEFAULT uuidv7() NOT NULL,
    bucket_id uuid NOT NULL,
    object_key text NOT NULL COLLATE pg_catalog."C",
    stream_type text CONSTRAINT streams_kind_not_null NOT NULL,
    state text NOT NULL,
    size bigint DEFAULT 0 NOT NULL,
    etag text DEFAULT ''::text NOT NULL,
    metadata jsonb DEFAULT '{}'::jsonb NOT NULL,
    public_read boolean DEFAULT false NOT NULL,
    checksums jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    touched_at timestamp with time zone DEFAULT now() NOT NULL,
    upload_cache_bypass boolean DEFAULT false NOT NULL,
    write_authorization jsonb DEFAULT '{"principal": {"kind": "local"}, "resources": {}}'::jsonb NOT NULL,
    owner_task_id uuid,
    owner_lease_token uuid,
    http_compression jsonb DEFAULT '{}'::jsonb NOT NULL,
    payload_type text DEFAULT 'chunks'::text NOT NULL,
    payload_revision bigint DEFAULT 1 NOT NULL,
    CONSTRAINT streams_kind_check CHECK ((stream_type = ANY (ARRAY['object'::text, 'part'::text]))),
    CONSTRAINT streams_payload_revision_check CHECK ((payload_revision > 0)),
    CONSTRAINT streams_payload_type_check CHECK ((payload_type = ANY (ARRAY['chunks'::text, 'raw'::text]))),
    CONSTRAINT streams_size_check CHECK ((size >= 0)),
    CONSTRAINT streams_state_check CHECK ((state = ANY (ARRAY['writing'::text, 'ready'::text, 'retired'::text, 'abandoned'::text])))
)
WITH (autovacuum_vacuum_scale_factor='0.05', autovacuum_analyze_scale_factor='0.02');

CREATE TABLE public.tasks (
    id uuid DEFAULT uuidv7() NOT NULL,
    task_type text CONSTRAINT tasks_kind_not_null NOT NULL,
    bucket_id uuid,
    state text NOT NULL,
    cursor text,
    processed bigint DEFAULT 0 NOT NULL,
    detail jsonb DEFAULT '{}'::jsonb NOT NULL,
    error text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    created_by_label text DEFAULT 'Unknown'::text CONSTRAINT tasks_created_by_not_null NOT NULL,
    source text DEFAULT 'unknown'::text NOT NULL,
    started_at timestamp with time zone,
    lease_node_id uuid,
    lease_token uuid,
    lease_expires_at timestamp with time zone,
    parent_task_id uuid,
    execution_scope text DEFAULT 'cluster'::text NOT NULL,
    target_node_id uuid,
    batch_total bigint,
    batch_completed bigint DEFAULT 0 NOT NULL,
    CONSTRAINT tasks_state_check CHECK ((state = ANY (ARRAY['queued'::text, 'running'::text, 'paused'::text, 'completed'::text, 'failed'::text]))),
    CONSTRAINT tasks_task_type_check CHECK ((task_type = ANY (ARRAY['purge'::text, 'sweep'::text, 'integrity'::text, 'pack'::text, 'unpack'::text, 'upload'::text, 'cache_flush'::text, 'gc'::text, 'cleanup'::text, 'storage_migration'::text, 'rewrite'::text, 'usage'::text])))
)
WITH (autovacuum_vacuum_scale_factor='0.05', autovacuum_analyze_scale_factor='0.02');

CREATE TABLE public.token_bucket_grants (
    token_id uuid CONSTRAINT token_grants_token_id_not_null NOT NULL,
    bucket_id uuid CONSTRAINT token_grants_bucket_id_not_null NOT NULL,
    actions text[] CONSTRAINT token_grants_actions_not_null NOT NULL,
    CONSTRAINT token_grants_actions_check CHECK ((actions <@ ARRAY['bucket.list'::text, 'object.read'::text, 'object.write'::text, 'object.delete'::text, 'object.acl'::text, 'bucket.settings'::text, 'storage.inspect'::text]))
);

CREATE VIEW public.user_bucket_access AS
 SELECT m.user_id,
    b.id AS bucket_id,
    ARRAY( SELECT action.action
           FROM unnest(
                CASE m.role
                    WHEN 'reader'::text THEN ARRAY['bucket.list'::text, 'object.read'::text, 'storage.inspect'::text]
                    WHEN 'writer'::text THEN ARRAY['bucket.list'::text, 'object.read'::text, 'object.write'::text, 'object.delete'::text, 'object.acl'::text, 'storage.inspect'::text]
                    ELSE ARRAY['bucket.list'::text, 'object.read'::text, 'object.write'::text, 'object.delete'::text, 'object.acl'::text, 'bucket.settings'::text, 'storage.inspect'::text]
                END) action(action)
          WHERE ((m.scope = 'all'::text) OR (action.action = ANY (COALESCE(g.actions, ARRAY[]::text[]))))) AS actions
   FROM ((public.project_members m
     JOIN public.buckets b ON ((b.project_id = m.project_id)))
     LEFT JOIN public.member_bucket_grants g ON (((g.user_id = m.user_id) AND (g.bucket_id = b.id))));

CREATE TABLE public.web_users (
    id uuid DEFAULT uuidv7() NOT NULL,
    username text NOT NULL,
    password_hash text NOT NULL,
    enabled boolean DEFAULT true NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    display_name text DEFAULT ''::text NOT NULL,
    locale text,
    theme text,
    avatar_email text DEFAULT ''::text NOT NULL,
    avatar_enabled boolean DEFAULT false NOT NULL,
    auth_revision bigint DEFAULT 0 NOT NULL,
    role text DEFAULT 'member'::text NOT NULL,
    authorization_revision bigint DEFAULT 0 NOT NULL,
    must_change_password boolean DEFAULT false NOT NULL,
    CONSTRAINT web_users_avatar_email_check CHECK ((octet_length(avatar_email) <= 320)),
    CONSTRAINT web_users_display_name_check CHECK ((octet_length(display_name) <= 240)),
    CONSTRAINT web_users_locale_check CHECK ((locale = ANY (ARRAY['en'::text, 'zh-CN'::text, 'ja'::text]))),
    CONSTRAINT web_users_role_check CHECK ((role = ANY (ARRAY['admin'::text, 'member'::text]))),
    CONSTRAINT web_users_theme_check CHECK ((theme = ANY (ARRAY['auto'::text, 'light'::text, 'dark'::text])))
);

CREATE VIEW public.token_bucket_access AS
 SELECT t.id AS token_id,
    g.bucket_id,
    ARRAY( SELECT action.action
           FROM unnest(g.actions) action(action)
          WHERE ((u.role = 'admin'::text) OR (action.action = ANY (COALESCE(a.actions, ARRAY[]::text[]))))) AS actions
   FROM (((public.api_tokens t
     JOIN public.web_users u ON ((u.id = t.user_id)))
     JOIN public.token_bucket_grants g ON ((g.token_id = t.id)))
     LEFT JOIN public.user_bucket_access a ON (((a.user_id = t.user_id) AND (a.bucket_id = g.bucket_id))));

CREATE TABLE public.uploads (
    id uuid DEFAULT uuidv7() NOT NULL,
    bucket_id uuid NOT NULL,
    object_key text NOT NULL COLLATE pg_catalog."C",
    access_key text NOT NULL,
    state text DEFAULT 'active'::text NOT NULL,
    metadata jsonb DEFAULT '{}'::jsonb NOT NULL,
    public_read boolean DEFAULT false NOT NULL,
    checksum_algorithm text,
    checksum_type text,
    manifest_hash text,
    result jsonb,
    output_stream uuid,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    touched_at timestamp with time zone DEFAULT now() NOT NULL,
    durable_at timestamp with time zone,
    staging_storage_id uuid,
    staging_object_key text,
    completion_token uuid,
    completion_expires_at timestamp with time zone,
    raw_source_id uuid,
    remote_token uuid,
    remote_expires_at timestamp with time zone,
    remote_manifest_hash text,
    CONSTRAINT uploads_state_check CHECK ((state = ANY (ARRAY['active'::text, 'completing'::text, 'completed'::text, 'aborted'::text])))
)
WITH (autovacuum_vacuum_scale_factor='0.05', autovacuum_analyze_scale_factor='0.02');

CREATE TABLE public.usage_history (
    scope_id text CONSTRAINT storage_history_scope_id_not_null NOT NULL,
    snapshot_at timestamp with time zone CONSTRAINT storage_history_at_not_null NOT NULL,
    data jsonb CONSTRAINT storage_history_data_not_null NOT NULL
);

CREATE TABLE public.usage_summary (
    id text CONSTRAINT storage_insights_id_not_null NOT NULL,
    bucket_ids uuid[],
    requested_at timestamp with time zone DEFAULT now() CONSTRAINT storage_insights_requested_at_not_null NOT NULL,
    as_of timestamp with time zone,
    data jsonb,
    CONSTRAINT storage_insights_check CHECK (((id = 'global'::text) = (bucket_ids IS NULL)))
);

CREATE TABLE public.web_uploads (
    upload_id uuid NOT NULL,
    user_id uuid,
    client_id uuid NOT NULL,
    request_hash bytea NOT NULL,
    file_name text NOT NULL,
    expected_size bigint NOT NULL,
    part_size bigint NOT NULL,
    modified_at bigint,
    expected_stream_id uuid,
    CONSTRAINT web_uploads_expected_size_check CHECK (((expected_size >= 0) AND (expected_size <= '53687091200000'::bigint))),
    CONSTRAINT web_uploads_file_name_check CHECK (((octet_length(file_name) >= 1) AND (octet_length(file_name) <= 1024))),
    CONSTRAINT web_uploads_modified_at_check CHECK ((modified_at >= 0)),
    CONSTRAINT web_uploads_part_size_check CHECK (((part_size >= 5242880) AND (part_size <= '5368709120'::bigint))),
    CONSTRAINT web_uploads_request_hash_check CHECK ((octet_length(request_hash) = 32))
);

INSERT INTO "maintenance_controls" ("policy", "paused", "last_scheduled_at", "updated_at") VALUES ('cleanup', 'false', now(), now());
INSERT INTO "maintenance_controls" ("policy", "paused", "last_scheduled_at", "updated_at") VALUES ('gc', 'false', now(), now());
INSERT INTO "maintenance_controls" ("policy", "paused", "last_scheduled_at", "updated_at") VALUES ('pack', 'false', now(), now());
INSERT INTO "maintenance_controls" ("policy", "paused", "last_scheduled_at", "updated_at") VALUES ('range', 'false', now(), now());
INSERT INTO "maintenance_controls" ("policy", "paused", "last_scheduled_at", "updated_at") VALUES ('reclaim', 'false', now(), now());
INSERT INTO "maintenance_controls" ("policy", "paused", "last_scheduled_at", "updated_at") VALUES ('repack', 'false', now(), now());
INSERT INTO "maintenance_controls" ("policy", "paused", "last_scheduled_at", "updated_at") VALUES ('reuse', 'false', now(), now());
INSERT INTO "projects" ("id", "name", "description", "builtin", "allow_bucket_create", "created_at") VALUES ('00000000-0000-0000-0000-000000000001', 'Default', '', 'true', 'false', now());
INSERT INTO "quota_scopes" ("scope", "scope_id", "used_bytes", "reserved_bytes", "inflight_bytes", "object_count", "bucket_count", "logical_bytes_limit", "inflight_bytes_limit", "bucket_count_limit", "cached_bytes", "cached_compressed") VALUES ('project', '00000000-0000-0000-0000-000000000001', '0', '0', '0', '0', '0', NULL, NULL, NULL, '0', 'false');
INSERT INTO "runtime_settings" ("section", "value", "revision", "updated_at", "updated_by_user_id") VALUES ('maintenance', '{"interval": "30m", "batch_size": 1000, "max_duration": "5s", "task_retention": "30d", "audit_retention": "90d"}', '1', now(), NULL);
INSERT INTO "runtime_settings" ("section", "value", "revision", "updated_at", "updated_by_user_id") VALUES ('metrics', '{"scope_limit": 64, "query_timeout": "2m", "access_retention": "14d", "refresh_interval": "15m", "runtime_interval": "1m", "storage_interval": "15m", "runtime_retention": "24h", "storage_retention": "7d", "access_flush_interval": "1m"}', '1', now(), NULL);
INSERT INTO "runtime_settings" ("section", "value", "revision", "updated_at", "updated_by_user_id") VALUES ('requests', '{"s3_domain": null, "s3_region": "us-east-1", "s3_chunk_limit": "8MiB", "read_idle_timeout": "120s", "upload_idle_timeout": "120s"}', '1', now(), NULL);
INSERT INTO "runtime_settings" ("section", "value", "revision", "updated_at", "updated_by_user_id") VALUES ('uploads', '{"sweep_interval": "5m", "max_active_uploads": null, "staging_storage_id": null, "client_idle_timeout": "120s", "staging_bytes_limit": null}', '2', now(), NULL);
INSERT INTO "runtime_settings" ("section", "value", "revision", "updated_at", "updated_by_user_id") VALUES ('web', '{"origin": "http://localhost:9002", "secure_cookie": true, "session_lifetime": "12h", "gravatar_base_url": "https://www.gravatar.com/avatar/"}', '1', now(), NULL);
INSERT INTO "usage_summary" ("id", "bucket_ids", "requested_at", "as_of", "data") VALUES ('global', NULL, now(), NULL, NULL);

ALTER TABLE ONLY public.api_tokens
    ADD CONSTRAINT api_tokens_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.api_tokens
    ADD CONSTRAINT api_tokens_token_hash_key UNIQUE (token_hash);

ALTER TABLE ONLY public.audit_events
    ADD CONSTRAINT audit_events_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.buckets
    ADD CONSTRAINT buckets_id_project_id_key UNIQUE (id, project_id);

ALTER TABLE ONLY public.buckets
    ADD CONSTRAINT buckets_name_key UNIQUE (name);

ALTER TABLE ONLY public.buckets
    ADD CONSTRAINT buckets_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.cache_pins
    ADD CONSTRAINT cache_pins_pkey PRIMARY KEY (chunk_id, pin_type, owner_id);

ALTER TABLE ONLY public.chunk_access_stats
    ADD CONSTRAINT chunk_access_stats_pkey PRIMARY KEY (chunk_id);

ALTER TABLE ONLY public.chunk_access_windows
    ADD CONSTRAINT chunk_access_windows_pkey PRIMARY KEY (window_start, chunk_id);

ALTER TABLE ONLY public.chunk_encodings
    ADD CONSTRAINT chunk_locations_pkey PRIMARY KEY (encoding_id);

ALTER TABLE ONLY public.chunks
    ADD CONSTRAINT chunks_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.s3_credentials
    ADD CONSTRAINT credentials_pkey PRIMARY KEY (access_key_id);

ALTER TABLE ONLY public.bucket_domains
    ADD CONSTRAINT domains_pkey PRIMARY KEY (host);

ALTER TABLE ONLY public.extents
    ADD CONSTRAINT extents_pkey PRIMARY KEY (stream_id, offset_bytes);

ALTER TABLE ONLY public.fragments
    ADD CONSTRAINT fragments_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.s3_credential_grants
    ADD CONSTRAINT grants_pkey PRIMARY KEY (access_key, bucket_id);

ALTER TABLE ONLY public.integrity_issues
    ADD CONSTRAINT integrity_issues_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.integrity_issues
    ADD CONSTRAINT integrity_issues_task_id_subject_code_key UNIQUE (task_id, subject, code);

ALTER TABLE ONLY public.integrity_packs
    ADD CONSTRAINT integrity_packs_pkey PRIMARY KEY (task_id, pack_id);

ALTER TABLE ONLY public.key_fingerprints
    ADD CONSTRAINT key_fingerprints_pkey PRIMARY KEY (key_id);

ALTER TABLE ONLY public.key_replacements
    ADD CONSTRAINT key_replacements_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.maintenance_controls
    ADD CONSTRAINT maintenance_controls_pkey PRIMARY KEY (policy);

ALTER TABLE ONLY public.maintenance_previews
    ADD CONSTRAINT maintenance_previews_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.setup_tokens
    ADD CONSTRAINT manage_setup_pkey PRIMARY KEY (singleton);

ALTER TABLE ONLY public.managed_keys
    ADD CONSTRAINT managed_keys_name_key UNIQUE (name);

ALTER TABLE ONLY public.managed_keys
    ADD CONSTRAINT managed_keys_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.media_operations
    ADD CONSTRAINT media_operations_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.media_operations
    ADD CONSTRAINT media_operations_user_id_token_id_client_id_key UNIQUE NULLS NOT DISTINCT (user_id, token_id, client_id);

ALTER TABLE ONLY public.member_bucket_grants
    ADD CONSTRAINT member_grants_pkey PRIMARY KEY (user_id, bucket_id);

ALTER TABLE ONLY public.mokyu_meta
    ADD CONSTRAINT mokyu_meta_pkey PRIMARY KEY (singleton);

ALTER TABLE ONLY public.multipart_files
    ADD CONSTRAINT multipart_files_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.multipart_files
    ADD CONSTRAINT multipart_files_source_id_key UNIQUE (source_id);

ALTER TABLE ONLY public.node_boots
    ADD CONSTRAINT node_boots_pkey PRIMARY KEY (node_id, boot_id);

ALTER TABLE ONLY public.node_read_permits
    ADD CONSTRAINT node_read_permits_pkey PRIMARY KEY (token);

ALTER TABLE ONLY public.node_read_sources
    ADD CONSTRAINT node_read_sources_pkey PRIMARY KEY (window_id, kind, location_id);

ALTER TABLE ONLY public.node_read_windows
    ADD CONSTRAINT node_read_windows_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.node_read_windows
    ADD CONSTRAINT node_read_windows_read_id_offset_bytes_key UNIQUE (read_id, offset_bytes);

ALTER TABLE ONLY public.node_reads
    ADD CONSTRAINT node_reads_node_id_fence_operation_id_key UNIQUE (node_id, fence, operation_id);

ALTER TABLE ONLY public.node_reads
    ADD CONSTRAINT node_reads_pkey PRIMARY KEY (read_id);

ALTER TABLE ONLY public.node_reports
    ADD CONSTRAINT node_reports_pkey PRIMARY KEY (node_id);

ALTER TABLE ONLY public.node_runtime_samples
    ADD CONSTRAINT node_runtime_samples_pkey PRIMARY KEY (node_id, boot_id, sequence);

ALTER TABLE ONLY public.node_sessions
    ADD CONSTRAINT node_sessions_pkey PRIMARY KEY (node_id);

ALTER TABLE ONLY public.node_writes
    ADD CONSTRAINT node_writes_lease_id_key UNIQUE (lease_id);

ALTER TABLE ONLY public.node_writes
    ADD CONSTRAINT node_writes_pkey PRIMARY KEY (node_id, operation_id);

ALTER TABLE ONLY public.node_writes
    ADD CONSTRAINT node_writes_stream_id_key UNIQUE (stream_id);

ALTER TABLE ONLY public.nodes
    ADD CONSTRAINT nodes_name_key UNIQUE (name);

ALTER TABLE ONLY public.nodes
    ADD CONSTRAINT nodes_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.nonce_counters
    ADD CONSTRAINT nonce_counters_pkey PRIMARY KEY (block_key_id, type_tag);

ALTER TABLE ONLY public.nonce_observations
    ADD CONSTRAINT nonce_observations_pkey PRIMARY KEY (block_key_id);

ALTER TABLE ONLY public.objects
    ADD CONSTRAINT objects_pkey PRIMARY KEY (bucket_id, object_key);

ALTER TABLE ONLY public.objects
    ADD CONSTRAINT objects_stream_id_key UNIQUE (stream_id);

ALTER TABLE ONLY public.pack_access_windows
    ADD CONSTRAINT pack_access_windows_pkey PRIMARY KEY (window_start, pack_id);

ALTER TABLE ONLY public.pack_changes
    ADD CONSTRAINT pack_changes_pkey PRIMARY KEY (pack_id);

ALTER TABLE ONLY public.pack_inputs
    ADD CONSTRAINT pack_inputs_pkey PRIMARY KEY (task_id, chunk_id);

ALTER TABLE ONLY public.pack_maintenance
    ADD CONSTRAINT pack_maintenance_pkey PRIMARY KEY (stream_id);

ALTER TABLE ONLY public.pack_member_access_windows
    ADD CONSTRAINT pack_member_access_windows_pkey PRIMARY KEY (window_start, pack_id, chunk_id);

ALTER TABLE ONLY public.pack_members
    ADD CONSTRAINT pack_members_pack_id_chunk_id_key UNIQUE (pack_id, chunk_id);

ALTER TABLE ONLY public.pack_members
    ADD CONSTRAINT pack_members_pkey PRIMARY KEY (pack_id, ordinal);

ALTER TABLE ONLY public.packs
    ADD CONSTRAINT packs_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.parts
    ADD CONSTRAINT parts_pkey PRIMARY KEY (upload_id, part_number);

ALTER TABLE ONLY public.pending_uploads
    ADD CONSTRAINT pending_uploads_pkey PRIMARY KEY (chunk_id);

ALTER TABLE ONLY public.project_members
    ADD CONSTRAINT project_members_pkey PRIMARY KEY (user_id, project_id);

ALTER TABLE ONLY public.projects
    ADD CONSTRAINT projects_name_key UNIQUE (name);

ALTER TABLE ONLY public.projects
    ADD CONSTRAINT projects_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.quota_scopes
    ADD CONSTRAINT quota_accounts_pkey PRIMARY KEY (scope, scope_id);

ALTER TABLE ONLY public.quota_reservations
    ADD CONSTRAINT quota_reservations_output_stream_key UNIQUE (result_stream_id);

ALTER TABLE ONLY public.quota_reservations
    ADD CONSTRAINT quota_reservations_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.quota_reservations
    ADD CONSTRAINT quota_reservations_stream_id_key UNIQUE (stream_id);

ALTER TABLE ONLY public.quota_reservations
    ADD CONSTRAINT quota_reservations_upload_id_key UNIQUE (upload_id);

ALTER TABLE ONLY public.quota_writes
    ADD CONSTRAINT quota_writes_pkey PRIMARY KEY (stream_id);

ALTER TABLE ONLY public.raw_processing
    ADD CONSTRAINT raw_processing_pkey PRIMARY KEY (stream_id);

ALTER TABLE ONLY public.raw_ranges
    ADD CONSTRAINT raw_ranges_pkey PRIMARY KEY (stream_id, offset_bytes);

ALTER TABLE ONLY public.raw_sources
    ADD CONSTRAINT raw_sources_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.raw_uploads
    ADD CONSTRAINT raw_uploads_pkey PRIMARY KEY (source_id);

ALTER TABLE ONLY public.runtime_samples
    ADD CONSTRAINT runtime_history_pkey PRIMARY KEY (sampled_at);

ALTER TABLE ONLY public.runtime_settings
    ADD CONSTRAINT runtime_settings_pkey PRIMARY KEY (section);

ALTER TABLE ONLY public.session_refresh_tokens
    ADD CONSTRAINT session_refresh_tokens_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.session_refresh_tokens
    ADD CONSTRAINT session_refresh_tokens_token_hash_key UNIQUE (token_hash);

ALTER TABLE ONLY public.sessions
    ADD CONSTRAINT sessions_id_key UNIQUE (id);

ALTER TABLE ONLY public.sessions
    ADD CONSTRAINT sessions_pkey PRIMARY KEY (token_hash);

ALTER TABLE ONLY public.staging_objects
    ADD CONSTRAINT staging_objects_pkey PRIMARY KEY (storage_id, object_key);

ALTER TABLE ONLY public.usage_history
    ADD CONSTRAINT storage_history_pkey PRIMARY KEY (scope_id, snapshot_at);

ALTER TABLE ONLY public.usage_summary
    ADD CONSTRAINT storage_insights_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.storage_maintenance
    ADD CONSTRAINT storage_maintenance_pkey PRIMARY KEY (storage_id, policy);

ALTER TABLE ONLY public.storage_profiles
    ADD CONSTRAINT storage_profiles_pkey PRIMARY KEY (storage_id);

ALTER TABLE ONLY public.storage_rate_state
    ADD CONSTRAINT storage_rate_state_pkey PRIMARY KEY (storage_id, metric);

ALTER TABLE ONLY public.storages
    ADD CONSTRAINT storage_targets_name_key UNIQUE (name);

ALTER TABLE ONLY public.storage_permits
    ADD CONSTRAINT store_permits_pkey PRIMARY KEY (token);

ALTER TABLE ONLY public.storages
    ADD CONSTRAINT stores_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.stream_leases
    ADD CONSTRAINT stream_leases_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.streams
    ADD CONSTRAINT streams_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.tasks
    ADD CONSTRAINT tasks_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.token_bucket_grants
    ADD CONSTRAINT token_grants_pkey PRIMARY KEY (token_id, bucket_id);

ALTER TABLE ONLY public.uploads
    ADD CONSTRAINT uploads_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.web_uploads
    ADD CONSTRAINT web_uploads_pkey PRIMARY KEY (upload_id);

ALTER TABLE ONLY public.web_uploads
    ADD CONSTRAINT web_uploads_user_id_client_id_key UNIQUE (user_id, client_id);

ALTER TABLE ONLY public.web_users
    ADD CONSTRAINT web_users_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.web_users
    ADD CONSTRAINT web_users_username_key UNIQUE (username);

CREATE INDEX api_tokens_user ON public.api_tokens USING btree (user_id, id);

CREATE INDEX audit_events_actor ON public.audit_events USING btree (actor_id, id);

CREATE INDEX audit_events_bucket ON public.audit_events USING btree (bucket_id, id);

CREATE INDEX audit_events_created ON public.audit_events USING btree (created_at, id);

CREATE INDEX audit_events_project ON public.audit_events USING btree (project_id, id);

CREATE INDEX audit_events_request ON public.audit_events USING btree (request_id) WHERE (request_id IS NOT NULL);

CREATE INDEX buckets_project ON public.buckets USING btree (project_id, name);

CREATE INDEX buckets_store ON public.buckets USING btree (storage_id);

CREATE INDEX chunk_access_history ON public.chunk_access_windows USING btree (chunk_id, window_start);

CREATE UNIQUE INDEX chunk_encodings_current ON public.chunk_encodings USING btree (chunk_id) WHERE (state = 'ready'::text);

CREATE INDEX chunk_encodings_gc ON public.chunk_encodings USING btree (unreferenced_at, encoding_id) WHERE (state = ANY (ARRAY['retired'::text, 'deleting'::text]));

CREATE INDEX chunk_encodings_uploading_owner ON public.chunk_encodings USING btree (owner_task) WHERE ((owner_task IS NOT NULL) AND (state = 'uploading'::text));

CREATE INDEX chunk_locations_store ON public.chunk_encodings USING btree (storage_id) WHERE (storage_id IS NOT NULL);

CREATE UNIQUE INDEX chunks_dedup ON public.chunks USING btree (storage_id, hash, raw_size) WHERE (state = ANY (ARRAY['preparing'::text, 'uploading'::text, 'ready'::text]));

CREATE INDEX chunks_deleted ON public.chunks USING btree (deleted_at, id) WHERE (state = 'deleted'::text);

CREATE INDEX chunks_gc ON public.chunks USING btree (unreferenced_at, id) WHERE (state = ANY (ARRAY['ready'::text, 'failed'::text, 'deleting'::text]));

CREATE INDEX chunks_owner ON public.chunks USING btree (owner_stream) WHERE (owner_stream IS NOT NULL);

CREATE INDEX chunks_pack ON public.chunks USING btree (pack_id) WHERE (pack_id IS NOT NULL);

CREATE INDEX chunks_pending_key ON public.chunks USING btree (block_key_id) WHERE (state = ANY (ARRAY['preparing'::text, 'uploading'::text]));

CREATE INDEX chunks_range_repack ON public.chunks USING btree (repack_after, id) WHERE (range_split_at IS NOT NULL);

CREATE INDEX chunks_store ON public.chunks USING btree (storage_id) WHERE (storage_id IS NOT NULL);

CREATE INDEX credentials_project ON public.s3_credentials USING btree (project_id);

CREATE INDEX encodings_owner_stream ON public.chunk_encodings USING btree (owner_stream) WHERE (owner_stream IS NOT NULL);

CREATE INDEX encodings_quarantined ON public.chunk_encodings USING btree (storage_id, encoding_id) WHERE (quarantined_at IS NOT NULL);

CREATE INDEX encodings_recovery ON public.chunk_encodings USING btree (storage_id, unreferenced_at, encoding_id) WHERE recovery_pending;

CREATE INDEX encodings_sync ON public.chunk_encodings USING btree (chunk_id) WHERE (sync_pending AND (state = ANY (ARRAY['ready'::text, 'uploading'::text])) AND (quarantined_at IS NULL));

CREATE INDEX extents_chunk ON public.extents USING btree (chunk_id) WHERE (chunk_id IS NOT NULL);

CREATE INDEX extents_fragment ON public.extents USING btree (fragment_id) WHERE (fragment_id IS NOT NULL);

CREATE INDEX fragments_cleanup ON public.fragments USING btree (created_at, id);

CREATE INDEX fragments_file ON public.fragments USING btree (file_id, file_offset) WHERE (file_id IS NOT NULL);

CREATE INDEX fragments_owner ON public.fragments USING btree (owner_stream) WHERE (owner_stream IS NOT NULL);

CREATE INDEX integrity_issues_page ON public.integrity_issues USING btree (task_id, id);

CREATE INDEX key_replacements_expiry ON public.key_replacements USING btree (expires_at);

CREATE INDEX maintenance_previews_expiry ON public.maintenance_previews USING btree (expires_at);

CREATE UNIQUE INDEX managed_keys_fingerprint ON public.managed_keys USING btree (fingerprint);

CREATE INDEX media_operations_cleanup ON public.media_operations USING btree (created_at, id);

CREATE INDEX member_bucket_grants_bucket ON public.member_bucket_grants USING btree (bucket_id, user_id);

CREATE INDEX multipart_files_node ON public.multipart_files USING btree (node_id, id);

CREATE INDEX multipart_files_reclaim ON public.multipart_files USING btree (storage_id, id) WHERE reclaim_pending;

CREATE INDEX multipart_files_upload ON public.multipart_files USING btree (upload_id, state);

CREATE INDEX node_read_permits_read ON public.node_read_permits USING btree (read_id);

CREATE INDEX node_read_sources_location ON public.node_read_sources USING btree (kind, location_id);

CREATE INDEX node_runtime_expire ON public.node_runtime_samples USING btree (received_at);

CREATE INDEX node_runtime_time ON public.node_runtime_samples USING btree (node_id, received_at DESC);

CREATE INDEX node_writes_cleanup ON public.node_writes USING btree (node_id, created_at);

CREATE INDEX node_writes_key ON public.node_writes USING btree (block_key_id) WHERE ((state = 'active'::text) AND (block_key_id IS NOT NULL));

CREATE INDEX nodes_role_enabled ON public.nodes USING btree (role, enabled);

CREATE INDEX objects_catalog_category ON public.objects USING btree (bucket_id, catalog_category, object_key) WHERE (stream_id IS NOT NULL);

CREATE INDEX objects_catalog_modified ON public.objects USING btree (bucket_id, catalog_modified, object_key) WHERE (stream_id IS NOT NULL);

CREATE INDEX objects_catalog_search ON public.objects USING gin (object_key COLLATE "default" public.gin_trgm_ops) WHERE (stream_id IS NOT NULL);

CREATE INDEX objects_catalog_size ON public.objects USING btree (bucket_id, catalog_size, object_key) WHERE (stream_id IS NOT NULL);

CREATE INDEX objects_empty ON public.objects USING btree (bucket_id, object_key) WHERE (stream_id IS NULL);

CREATE INDEX pack_access_history ON public.pack_access_windows USING btree (pack_id, window_start);

CREATE INDEX pack_changes_due ON public.pack_changes USING btree (reason, next_check_at, pack_id);

CREATE UNIQUE INDEX pack_inputs_chunk ON public.pack_inputs USING btree (chunk_id);

CREATE INDEX pack_maintenance_due ON public.pack_maintenance USING btree (next_check_at, stream_id);

CREATE INDEX pack_member_access_lookup ON public.pack_member_access_windows USING btree (pack_id, window_start);

CREATE INDEX pack_members_chunk ON public.pack_members USING btree (chunk_id);

CREATE INDEX packs_gc ON public.packs USING btree (unreferenced_at, id) WHERE (state = ANY (ARRAY['retired'::text, 'deleting'::text]));

CREATE INDEX packs_preparing_owner ON public.packs USING btree (owner_task) WHERE ((owner_task IS NOT NULL) AND (state = 'preparing'::text));

CREATE INDEX packs_quarantined ON public.packs USING btree (storage_id, id) WHERE (quarantined_at IS NOT NULL);

CREATE INDEX packs_store ON public.packs USING btree (storage_id) WHERE (storage_id IS NOT NULL);

CREATE INDEX parts_remote_pending ON public.parts USING btree (remote_node_id, remote_retry_at) WHERE ((remote_node_id IS NOT NULL) AND (remote_etag IS NULL));

CREATE INDEX parts_stream ON public.parts USING btree (stream_id) WHERE (stream_id IS NOT NULL);

CREATE INDEX pending_uploads_due ON public.pending_uploads USING btree (next_retry_at, chunk_id) WHERE (owner_task IS NULL);

CREATE INDEX pending_uploads_node ON public.pending_uploads USING btree (node_id) WHERE (node_id IS NOT NULL);

CREATE INDEX pending_uploads_retry ON public.pending_uploads USING btree (next_retry_at, owner_task) WHERE (owner_task IS NOT NULL);

CREATE INDEX pending_uploads_source_pack_id ON public.pending_uploads USING btree (source_pack_id) WHERE (source_pack_id IS NOT NULL);

CREATE INDEX pending_uploads_stream ON public.pending_uploads USING btree (stream_id, offset_bytes);

CREATE INDEX pending_uploads_task ON public.pending_uploads USING btree (owner_task) WHERE (owner_task IS NOT NULL);

CREATE INDEX project_members_project ON public.project_members USING btree (project_id, user_id);

CREATE UNIQUE INDEX projects_builtin ON public.projects USING btree (builtin) WHERE builtin;

CREATE UNIQUE INDEX quota_credit ON public.quota_reservations USING btree (bucket_id, object_key, replaced_stream_id) WHERE (replaced_stream_id IS NOT NULL);

CREATE INDEX quota_reservations_bucket ON public.quota_reservations USING btree (bucket_id);

CREATE INDEX quota_writes_reservation ON public.quota_writes USING btree (reservation_id);

CREATE INDEX raw_processing_due ON public.raw_processing USING btree (next_attempt_at, stream_id);

CREATE INDEX raw_ranges_source ON public.raw_ranges USING btree (source_id);

CREATE INDEX raw_sources_cleanup ON public.raw_sources USING btree (storage_id, state, unreferenced_at, id);

CREATE UNIQUE INDEX raw_sources_content ON public.raw_sources USING btree (storage_id, content_hash, size);

CREATE INDEX raw_sources_owner ON public.raw_sources USING btree (owner_stream) WHERE (owner_stream IS NOT NULL);

CREATE INDEX raw_sources_processing ON public.raw_sources USING btree (processing_task);

CREATE INDEX raw_sources_sync ON public.raw_sources USING btree (id) WHERE ((durability = ANY (ARRAY['buffered'::text, 'volatile'::text])) AND (state = 'ready'::text) AND (quarantine_code IS NULL));

CREATE INDEX raw_uploads_due ON public.raw_uploads USING btree (next_attempt_at, source_id);

CREATE INDEX raw_uploads_node ON public.raw_uploads USING btree (node_id) WHERE (node_id IS NOT NULL);

CREATE INDEX runtime_cluster_time ON public.runtime_samples USING btree (sampled_at DESC) WHERE ((data ->> 'scope'::text) = 'cluster'::text);

CREATE UNIQUE INDEX session_refresh_active ON public.session_refresh_tokens USING btree (session_id) WHERE (revoked_at IS NULL);

CREATE INDEX session_refresh_expiry ON public.session_refresh_tokens USING btree (expires_at) WHERE (revoked_at IS NULL);

CREATE INDEX sessions_expiry ON public.sessions USING btree (expires_at);

CREATE INDEX sessions_user ON public.sessions USING btree (user_id, created_at DESC, id);

CREATE INDEX staging_objects_created ON public.staging_objects USING btree (created_at, storage_id);

CREATE INDEX storage_permits_active ON public.storage_permits USING btree (storage_id, direction, expires_at);

CREATE INDEX stream_lease_expiry ON public.stream_leases USING btree (expires_at);

CREATE INDEX stream_lease_stream ON public.stream_leases USING btree (stream_id, expires_at);

CREATE UNIQUE INDEX stream_write_lease ON public.stream_leases USING btree (stream_id) WHERE (lease_type = 'write'::text);

CREATE INDEX streams_cleanup ON public.streams USING btree (state, touched_at, id);

CREATE INDEX streams_task_owner ON public.streams USING btree (owner_task_id) WHERE (owner_task_id IS NOT NULL);

CREATE INDEX streams_writing ON public.streams USING btree (bucket_id, object_key) WHERE (state = 'writing'::text);

CREATE INDEX tasks_completed ON public.tasks USING btree (updated_at, id) WHERE (state = 'completed'::text);

CREATE INDEX tasks_lease_expiry ON public.tasks USING btree (lease_expires_at) WHERE (state = 'running'::text);

CREATE INDEX tasks_list ON public.tasks USING btree (created_at DESC, id DESC);

CREATE INDEX tasks_node_running ON public.tasks USING btree (lease_node_id, updated_at DESC, id) WHERE (state = 'running'::text);

CREATE INDEX tasks_parent_task ON public.tasks USING btree (parent_task_id) WHERE (parent_task_id IS NOT NULL);

CREATE INDEX tasks_policy_recent ON public.tasks USING btree (COALESCE((detail ->> 'policy'::text), task_type), created_at DESC, id DESC);

CREATE INDEX tasks_state_list ON public.tasks USING btree (state, created_at DESC, id DESC);

CREATE INDEX tasks_target_node ON public.tasks USING btree (target_node_id) WHERE (target_node_id IS NOT NULL);

CREATE INDEX tasks_work ON public.tasks USING btree (updated_at, id) WHERE (state = ANY (ARRAY['queued'::text, 'running'::text]));

CREATE INDEX token_bucket_grants_bucket ON public.token_bucket_grants USING btree (bucket_id, token_id);

CREATE INDEX uploads_bucket_transfers ON public.uploads USING btree (bucket_id, created_at DESC, id);

CREATE INDEX uploads_expiry ON public.uploads USING btree (state, touched_at, id);

CREATE INDEX uploads_finished ON public.uploads USING btree (touched_at, id) WHERE (state = ANY (ARRAY['completed'::text, 'aborted'::text]));

CREATE INDEX uploads_list ON public.uploads USING btree (bucket_id, object_key, id);

CREATE INDEX uploads_output ON public.uploads USING btree (output_stream) WHERE (output_stream IS NOT NULL);

CREATE INDEX uploads_raw_source ON public.uploads USING btree (raw_source_id) WHERE (raw_source_id IS NOT NULL);

CREATE INDEX uploads_transfers ON public.uploads USING btree (created_at DESC, id);

CREATE INDEX usage_history_snapshot ON public.usage_history USING btree (snapshot_at);

CREATE INDEX web_users_directory ON public.web_users USING btree (username COLLATE "C");

CREATE TRIGGER bucket_settings_revision BEFORE UPDATE OF cors, website_enabled, index_document, error_document, project_id, state, uploads_paused, public_base_url ON public.buckets FOR EACH ROW EXECUTE FUNCTION public.bucket_settings_revision();

CREATE TRIGGER buckets_store_immutable BEFORE UPDATE OF storage_id ON public.buckets FOR EACH ROW EXECUTE FUNCTION public.prevent_bucket_store_change();

CREATE TRIGGER catalog_object BEFORE INSERT OR UPDATE OF stream_id ON public.objects FOR EACH ROW EXECUTE FUNCTION public.catalog_object();

CREATE TRIGGER catalog_stream AFTER UPDATE OF size, metadata, public_read, created_at ON public.streams FOR EACH ROW WHEN (((new.size IS DISTINCT FROM old.size) OR (new.metadata IS DISTINCT FROM old.metadata) OR (new.public_read IS DISTINCT FROM old.public_read) OR (new.created_at IS DISTINCT FROM old.created_at))) EXECUTE FUNCTION public.catalog_stream();

CREATE TRIGGER chunk_encodings_fingerprint BEFORE INSERT OR UPDATE OF storage_id, block_key_id, encryption_algorithm, standalone_compressed ON public.chunk_encodings FOR EACH ROW EXECUTE FUNCTION public.refresh_encoding_fingerprint();

CREATE TRIGGER chunk_locations_storage_binding BEFORE INSERT OR UPDATE OF chunk_id, storage_id ON public.chunk_encodings FOR EACH ROW EXECUTE FUNCTION public.validate_storage_binding();

CREATE TRIGGER credential_authorization BEFORE UPDATE OF enabled, secret_encrypted, project_id ON public.s3_credentials FOR EACH ROW EXECUTE FUNCTION public.advance_authorization_revision();

CREATE TRIGGER credential_expiration BEFORE UPDATE OF expires_at ON public.s3_credentials FOR EACH ROW EXECUTE FUNCTION public.advance_authorization_revision();

CREATE TRIGGER domain_settings_revision AFTER INSERT OR DELETE OR UPDATE ON public.bucket_domains FOR EACH ROW EXECUTE FUNCTION public.domain_settings_revision();

CREATE TRIGGER grant_authorization AFTER INSERT OR DELETE OR UPDATE ON public.member_bucket_grants FOR EACH ROW EXECUTE FUNCTION public.invalidate_member_authorization();

CREATE TRIGGER member_authorization AFTER INSERT OR DELETE OR UPDATE ON public.project_members FOR EACH ROW EXECUTE FUNCTION public.invalidate_member_authorization();

CREATE TRIGGER pack_extents_delete AFTER DELETE ON public.extents REFERENCING OLD TABLE AS changed FOR EACH STATEMENT EXECUTE FUNCTION public.enqueue_pack_extents();

CREATE TRIGGER pack_extents_insert AFTER INSERT ON public.extents REFERENCING NEW TABLE AS changed FOR EACH STATEMENT EXECUTE FUNCTION public.enqueue_pack_extents();

CREATE TRIGGER pack_extents_update AFTER UPDATE ON public.extents REFERENCING NEW TABLE AS changed FOR EACH STATEMENT EXECUTE FUNCTION public.enqueue_pack_extents();

CREATE TRIGGER pack_members_storage_binding BEFORE INSERT OR UPDATE OF pack_id, chunk_id ON public.pack_members FOR EACH ROW EXECUTE FUNCTION public.validate_pack_member_binding();

CREATE TRIGGER pack_stream_ready AFTER UPDATE OF state ON public.streams FOR EACH ROW EXECUTE FUNCTION public.enqueue_pack_stream();

CREATE TRIGGER packs_fingerprint BEFORE INSERT OR UPDATE OF storage_id, block_key_id, encryption_algorithm, pack_compressed, member_count ON public.packs FOR EACH ROW EXECUTE FUNCTION public.refresh_pack_fingerprint();

CREATE TRIGGER quota_bucket AFTER INSERT OR DELETE OR UPDATE OF project_id ON public.buckets FOR EACH ROW EXECUTE FUNCTION public.quota_bucket();

CREATE TRIGGER quota_object AFTER INSERT OR DELETE OR UPDATE OF stream_id, write_token ON public.objects FOR EACH ROW EXECUTE FUNCTION public.quota_object();

CREATE TRIGGER quota_part BEFORE INSERT OR DELETE OR UPDATE ON public.parts FOR EACH ROW EXECUTE FUNCTION public.quota_part();

CREATE TRIGGER quota_project AFTER INSERT OR DELETE ON public.projects FOR EACH ROW EXECUTE FUNCTION public.quota_project();

CREATE TRIGGER quota_stream_delete BEFORE DELETE ON public.streams FOR EACH ROW EXECUTE FUNCTION public.quota_stream_end();

CREATE TRIGGER quota_stream_end AFTER UPDATE OF state ON public.streams FOR EACH ROW EXECUTE FUNCTION public.quota_stream_end();

CREATE TRIGGER quota_upload_delete BEFORE DELETE ON public.uploads FOR EACH ROW EXECUTE FUNCTION public.quota_upload_end();

CREATE TRIGGER quota_upload_end AFTER UPDATE OF state ON public.uploads FOR EACH ROW EXECUTE FUNCTION public.quota_upload_end();

CREATE TRIGGER service_grant_authorization AFTER INSERT OR DELETE OR UPDATE ON public.s3_credential_grants FOR EACH ROW EXECUTE FUNCTION public.invalidate_service_authorization();

CREATE TRIGGER storages_block_key_validate BEFORE INSERT OR UPDATE OF block_key_id ON public.storages FOR EACH ROW EXECUTE FUNCTION public.validate_store_current_key();

CREATE TRIGGER streams_clear_write_lease AFTER UPDATE OF state ON public.streams FOR EACH ROW EXECUTE FUNCTION public.clear_stream_write_lease();

CREATE TRIGGER token_authorization BEFORE UPDATE OF system, expires_at, revoked_at ON public.api_tokens FOR EACH ROW EXECUTE FUNCTION public.advance_authorization_revision();

CREATE TRIGGER token_grant_authorization AFTER INSERT OR DELETE OR UPDATE ON public.token_bucket_grants FOR EACH ROW EXECUTE FUNCTION public.invalidate_token_authorization();

CREATE TRIGGER user_authorization BEFORE UPDATE OF role, enabled, password_hash ON public.web_users FOR EACH ROW EXECUTE FUNCTION public.advance_authorization_revision();

ALTER TABLE ONLY public.api_tokens
    ADD CONSTRAINT api_tokens_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.web_users(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.buckets
    ADD CONSTRAINT buckets_project_id_fkey FOREIGN KEY (project_id) REFERENCES public.projects(id);

ALTER TABLE ONLY public.buckets
    ADD CONSTRAINT buckets_store_id_fkey FOREIGN KEY (storage_id) REFERENCES public.storages(id);

ALTER TABLE ONLY public.cache_pins
    ADD CONSTRAINT cache_pins_chunk_id_fkey FOREIGN KEY (chunk_id) REFERENCES public.pending_uploads(chunk_id) ON DELETE CASCADE;

ALTER TABLE ONLY public.chunk_access_stats
    ADD CONSTRAINT chunk_access_stats_chunk_id_fkey FOREIGN KEY (chunk_id) REFERENCES public.chunks(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.chunk_access_windows
    ADD CONSTRAINT chunk_access_windows_chunk_id_fkey FOREIGN KEY (chunk_id) REFERENCES public.chunks(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.chunk_encodings
    ADD CONSTRAINT chunk_encodings_block_key_id_fkey FOREIGN KEY (block_key_id) REFERENCES public.managed_keys(id);

ALTER TABLE ONLY public.chunk_encodings
    ADD CONSTRAINT chunk_encodings_owner_stream_fkey FOREIGN KEY (owner_stream) REFERENCES public.streams(id) ON DELETE RESTRICT;

ALTER TABLE ONLY public.chunk_encodings
    ADD CONSTRAINT chunk_locations_chunk_id_fkey FOREIGN KEY (chunk_id) REFERENCES public.chunks(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.chunk_encodings
    ADD CONSTRAINT chunk_locations_owner_task_fkey FOREIGN KEY (owner_task) REFERENCES public.tasks(id) ON DELETE SET NULL;

ALTER TABLE ONLY public.chunk_encodings
    ADD CONSTRAINT chunk_locations_store_id_fkey FOREIGN KEY (storage_id) REFERENCES public.storages(id);

ALTER TABLE ONLY public.chunks
    ADD CONSTRAINT chunks_block_key_id_fkey FOREIGN KEY (block_key_id) REFERENCES public.managed_keys(id);

ALTER TABLE ONLY public.chunks
    ADD CONSTRAINT chunks_owner_stream_fkey FOREIGN KEY (owner_stream) REFERENCES public.streams(id) ON DELETE SET NULL;

ALTER TABLE ONLY public.chunks
    ADD CONSTRAINT chunks_pack_id_fkey FOREIGN KEY (pack_id) REFERENCES public.packs(id);

ALTER TABLE ONLY public.chunks
    ADD CONSTRAINT chunks_store_id_fkey FOREIGN KEY (storage_id) REFERENCES public.storages(id);

ALTER TABLE ONLY public.s3_credentials
    ADD CONSTRAINT credentials_created_by_fkey FOREIGN KEY (created_by) REFERENCES public.web_users(id) ON DELETE SET NULL;

ALTER TABLE ONLY public.s3_credentials
    ADD CONSTRAINT credentials_project_id_fkey FOREIGN KEY (project_id) REFERENCES public.projects(id);

ALTER TABLE ONLY public.bucket_domains
    ADD CONSTRAINT domains_bucket_id_fkey FOREIGN KEY (bucket_id) REFERENCES public.buckets(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.extents
    ADD CONSTRAINT extents_chunk_id_fkey FOREIGN KEY (chunk_id) REFERENCES public.chunks(id);

ALTER TABLE ONLY public.extents
    ADD CONSTRAINT extents_fragment_id_fkey FOREIGN KEY (fragment_id) REFERENCES public.fragments(id);

ALTER TABLE ONLY public.extents
    ADD CONSTRAINT extents_stream_id_fkey FOREIGN KEY (stream_id) REFERENCES public.streams(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.fragments
    ADD CONSTRAINT fragments_file_id_fkey FOREIGN KEY (file_id) REFERENCES public.multipart_files(id);

ALTER TABLE ONLY public.fragments
    ADD CONSTRAINT fragments_owner_stream_fkey FOREIGN KEY (owner_stream) REFERENCES public.streams(id) ON DELETE SET NULL;

ALTER TABLE ONLY public.fragments
    ADD CONSTRAINT fragments_staging_storage_fkey FOREIGN KEY (staging_storage_id) REFERENCES public.storages(id) ON DELETE SET NULL;

ALTER TABLE ONLY public.s3_credential_grants
    ADD CONSTRAINT grants_access_key_fkey FOREIGN KEY (access_key) REFERENCES public.s3_credentials(access_key_id) ON DELETE CASCADE;

ALTER TABLE ONLY public.s3_credential_grants
    ADD CONSTRAINT grants_bucket_id_fkey FOREIGN KEY (bucket_id) REFERENCES public.buckets(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.integrity_issues
    ADD CONSTRAINT integrity_issues_task_id_fkey FOREIGN KEY (task_id) REFERENCES public.tasks(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.integrity_packs
    ADD CONSTRAINT integrity_packs_pack_id_fkey FOREIGN KEY (pack_id) REFERENCES public.packs(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.integrity_packs
    ADD CONSTRAINT integrity_packs_task_id_fkey FOREIGN KEY (task_id) REFERENCES public.tasks(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.maintenance_previews
    ADD CONSTRAINT maintenance_previews_actor_id_fkey FOREIGN KEY (actor_id) REFERENCES public.web_users(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.maintenance_previews
    ADD CONSTRAINT maintenance_previews_task_id_fkey FOREIGN KEY (task_id) REFERENCES public.tasks(id) ON DELETE SET NULL;

ALTER TABLE ONLY public.maintenance_previews
    ADD CONSTRAINT maintenance_previews_token_id_fkey FOREIGN KEY (token_id) REFERENCES public.api_tokens(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.media_operations
    ADD CONSTRAINT media_operations_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.web_users(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.member_bucket_grants
    ADD CONSTRAINT member_grants_bucket_id_project_id_fkey FOREIGN KEY (bucket_id, project_id) REFERENCES public.buckets(id, project_id) ON DELETE CASCADE;

ALTER TABLE ONLY public.member_bucket_grants
    ADD CONSTRAINT member_grants_user_id_project_id_fkey FOREIGN KEY (user_id, project_id) REFERENCES public.project_members(user_id, project_id) ON DELETE CASCADE;

ALTER TABLE ONLY public.multipart_files
    ADD CONSTRAINT multipart_files_node_id_fkey FOREIGN KEY (node_id) REFERENCES public.nodes(id);

ALTER TABLE ONLY public.multipart_files
    ADD CONSTRAINT multipart_files_source_id_fkey FOREIGN KEY (source_id) REFERENCES public.raw_sources(id);

ALTER TABLE ONLY public.multipart_files
    ADD CONSTRAINT multipart_files_storage_id_fkey FOREIGN KEY (storage_id) REFERENCES public.storages(id);

ALTER TABLE ONLY public.multipart_files
    ADD CONSTRAINT multipart_files_upload_id_fkey FOREIGN KEY (upload_id) REFERENCES public.uploads(id) ON DELETE SET NULL;

ALTER TABLE ONLY public.node_boots
    ADD CONSTRAINT node_boots_node_id_fkey FOREIGN KEY (node_id) REFERENCES public.nodes(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.node_read_permits
    ADD CONSTRAINT node_read_permits_read_id_fkey FOREIGN KEY (read_id) REFERENCES public.node_reads(read_id) ON DELETE CASCADE;

ALTER TABLE ONLY public.node_read_permits
    ADD CONSTRAINT node_read_permits_token_fkey FOREIGN KEY (token) REFERENCES public.storage_permits(token) ON DELETE CASCADE;

ALTER TABLE ONLY public.node_read_sources
    ADD CONSTRAINT node_read_sources_window_id_fkey FOREIGN KEY (window_id) REFERENCES public.node_read_windows(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.node_read_windows
    ADD CONSTRAINT node_read_windows_read_id_fkey FOREIGN KEY (read_id) REFERENCES public.node_reads(read_id) ON DELETE CASCADE;

ALTER TABLE ONLY public.node_reads
    ADD CONSTRAINT node_reads_node_id_fkey FOREIGN KEY (node_id) REFERENCES public.nodes(id);

ALTER TABLE ONLY public.node_reads
    ADD CONSTRAINT node_reads_read_id_fkey FOREIGN KEY (read_id) REFERENCES public.stream_leases(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.node_reports
    ADD CONSTRAINT node_reports_node_id_fkey FOREIGN KEY (node_id) REFERENCES public.nodes(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.node_runtime_samples
    ADD CONSTRAINT node_runtime_samples_node_id_fkey FOREIGN KEY (node_id) REFERENCES public.nodes(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.node_sessions
    ADD CONSTRAINT node_sessions_node_id_boot_id_fkey FOREIGN KEY (node_id, boot_id) REFERENCES public.node_boots(node_id, boot_id);

ALTER TABLE ONLY public.node_sessions
    ADD CONSTRAINT node_sessions_node_id_fkey FOREIGN KEY (node_id) REFERENCES public.nodes(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.node_writes
    ADD CONSTRAINT node_writes_block_key_id_fkey FOREIGN KEY (block_key_id) REFERENCES public.managed_keys(id);

ALTER TABLE ONLY public.node_writes
    ADD CONSTRAINT node_writes_lease_id_fkey FOREIGN KEY (lease_id) REFERENCES public.stream_leases(id) ON DELETE SET NULL;

ALTER TABLE ONLY public.node_writes
    ADD CONSTRAINT node_writes_node_id_fkey FOREIGN KEY (node_id) REFERENCES public.nodes(id);

ALTER TABLE ONLY public.node_writes
    ADD CONSTRAINT node_writes_storage_id_fkey FOREIGN KEY (storage_id) REFERENCES public.storages(id) ON DELETE SET NULL;

ALTER TABLE ONLY public.node_writes
    ADD CONSTRAINT node_writes_stream_id_fkey FOREIGN KEY (stream_id) REFERENCES public.streams(id) ON DELETE SET NULL;

ALTER TABLE ONLY public.nonce_counters
    ADD CONSTRAINT nonce_counters_block_key_id_fkey FOREIGN KEY (block_key_id) REFERENCES public.managed_keys(id);

ALTER TABLE ONLY public.nonce_observations
    ADD CONSTRAINT nonce_observations_block_key_id_fkey FOREIGN KEY (block_key_id) REFERENCES public.managed_keys(id);

ALTER TABLE ONLY public.objects
    ADD CONSTRAINT objects_bucket_id_fkey FOREIGN KEY (bucket_id) REFERENCES public.buckets(id);

ALTER TABLE ONLY public.objects
    ADD CONSTRAINT objects_stream_id_fkey FOREIGN KEY (stream_id) REFERENCES public.streams(id);

ALTER TABLE ONLY public.pack_access_windows
    ADD CONSTRAINT pack_access_windows_pack_id_fkey FOREIGN KEY (pack_id) REFERENCES public.packs(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.pack_changes
    ADD CONSTRAINT pack_changes_pack_id_fkey FOREIGN KEY (pack_id) REFERENCES public.packs(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.pack_inputs
    ADD CONSTRAINT pack_inputs_chunk_id_fkey FOREIGN KEY (chunk_id) REFERENCES public.chunks(id);

ALTER TABLE ONLY public.pack_inputs
    ADD CONSTRAINT pack_inputs_task_id_fkey FOREIGN KEY (task_id) REFERENCES public.tasks(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.pack_maintenance
    ADD CONSTRAINT pack_maintenance_stream_id_fkey FOREIGN KEY (stream_id) REFERENCES public.streams(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.pack_member_access_windows
    ADD CONSTRAINT pack_member_access_windows_chunk_id_fkey FOREIGN KEY (chunk_id) REFERENCES public.chunks(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.pack_member_access_windows
    ADD CONSTRAINT pack_member_access_windows_pack_id_fkey FOREIGN KEY (pack_id) REFERENCES public.packs(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.pack_members
    ADD CONSTRAINT pack_members_chunk_id_fkey FOREIGN KEY (chunk_id) REFERENCES public.chunks(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.pack_members
    ADD CONSTRAINT pack_members_pack_id_fkey FOREIGN KEY (pack_id) REFERENCES public.packs(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.packs
    ADD CONSTRAINT packs_block_key_id_fkey FOREIGN KEY (block_key_id) REFERENCES public.managed_keys(id);

ALTER TABLE ONLY public.packs
    ADD CONSTRAINT packs_owner_task_fkey FOREIGN KEY (owner_task) REFERENCES public.tasks(id) ON DELETE SET NULL;

ALTER TABLE ONLY public.packs
    ADD CONSTRAINT packs_store_id_fkey FOREIGN KEY (storage_id) REFERENCES public.storages(id);

ALTER TABLE ONLY public.parts
    ADD CONSTRAINT parts_remote_node_id_fkey FOREIGN KEY (remote_node_id) REFERENCES public.nodes(id);

ALTER TABLE ONLY public.parts
    ADD CONSTRAINT parts_stream_id_fkey FOREIGN KEY (stream_id) REFERENCES public.streams(id);

ALTER TABLE ONLY public.parts
    ADD CONSTRAINT parts_upload_id_fkey FOREIGN KEY (upload_id) REFERENCES public.uploads(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.pending_uploads
    ADD CONSTRAINT pending_uploads_chunk_id_fkey FOREIGN KEY (chunk_id) REFERENCES public.chunks(id);

ALTER TABLE ONLY public.pending_uploads
    ADD CONSTRAINT pending_uploads_node_id_fkey FOREIGN KEY (node_id) REFERENCES public.nodes(id);

ALTER TABLE ONLY public.pending_uploads
    ADD CONSTRAINT pending_uploads_owner_task_fkey FOREIGN KEY (owner_task) REFERENCES public.tasks(id) ON DELETE SET NULL;

ALTER TABLE ONLY public.pending_uploads
    ADD CONSTRAINT pending_uploads_source_pack_fkey FOREIGN KEY (source_pack_id) REFERENCES public.packs(id);

ALTER TABLE ONLY public.pending_uploads
    ADD CONSTRAINT pending_uploads_staging_storage_fkey FOREIGN KEY (staging_storage_id) REFERENCES public.storages(id) ON DELETE SET NULL;

ALTER TABLE ONLY public.pending_uploads
    ADD CONSTRAINT pending_uploads_stream_id_fkey FOREIGN KEY (stream_id) REFERENCES public.streams(id) ON DELETE SET NULL;

ALTER TABLE ONLY public.project_members
    ADD CONSTRAINT project_members_project_id_fkey FOREIGN KEY (project_id) REFERENCES public.projects(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.project_members
    ADD CONSTRAINT project_members_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.web_users(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.quota_reservations
    ADD CONSTRAINT quota_reservations_bucket_id_fkey FOREIGN KEY (bucket_id) REFERENCES public.buckets(id);

ALTER TABLE ONLY public.quota_reservations
    ADD CONSTRAINT quota_reservations_output_stream_fkey FOREIGN KEY (result_stream_id) REFERENCES public.streams(id) ON DELETE SET NULL;

ALTER TABLE ONLY public.quota_reservations
    ADD CONSTRAINT quota_reservations_stream_id_fkey FOREIGN KEY (stream_id) REFERENCES public.streams(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.quota_reservations
    ADD CONSTRAINT quota_reservations_upload_id_fkey FOREIGN KEY (upload_id) REFERENCES public.uploads(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.quota_writes
    ADD CONSTRAINT quota_writes_reservation_id_fkey FOREIGN KEY (reservation_id) REFERENCES public.quota_reservations(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.quota_writes
    ADD CONSTRAINT quota_writes_stream_id_fkey FOREIGN KEY (stream_id) REFERENCES public.streams(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.raw_processing
    ADD CONSTRAINT raw_processing_source_id_fkey FOREIGN KEY (source_id) REFERENCES public.raw_sources(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.raw_processing
    ADD CONSTRAINT raw_processing_stream_id_fkey FOREIGN KEY (stream_id) REFERENCES public.streams(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.raw_ranges
    ADD CONSTRAINT raw_ranges_source_id_fkey FOREIGN KEY (source_id) REFERENCES public.raw_sources(id);

ALTER TABLE ONLY public.raw_ranges
    ADD CONSTRAINT raw_ranges_stream_id_fkey FOREIGN KEY (stream_id) REFERENCES public.streams(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.raw_sources
    ADD CONSTRAINT raw_sources_owner_stream_fkey FOREIGN KEY (owner_stream) REFERENCES public.streams(id) ON DELETE SET NULL;

ALTER TABLE ONLY public.raw_sources
    ADD CONSTRAINT raw_sources_processing_task_fkey FOREIGN KEY (processing_task) REFERENCES public.tasks(id) ON DELETE SET NULL;

ALTER TABLE ONLY public.raw_sources
    ADD CONSTRAINT raw_sources_storage_id_fkey FOREIGN KEY (storage_id) REFERENCES public.storages(id);

ALTER TABLE ONLY public.raw_uploads
    ADD CONSTRAINT raw_uploads_node_id_fkey FOREIGN KEY (node_id) REFERENCES public.nodes(id);

ALTER TABLE ONLY public.raw_uploads
    ADD CONSTRAINT raw_uploads_source_id_fkey FOREIGN KEY (source_id) REFERENCES public.raw_sources(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.runtime_settings
    ADD CONSTRAINT runtime_settings_updated_by_fkey FOREIGN KEY (updated_by_user_id) REFERENCES public.web_users(id) ON DELETE SET NULL;

ALTER TABLE ONLY public.session_refresh_tokens
    ADD CONSTRAINT session_refresh_tokens_replaced_by_fkey FOREIGN KEY (replaced_by) REFERENCES public.session_refresh_tokens(id) ON DELETE SET NULL;

ALTER TABLE ONLY public.session_refresh_tokens
    ADD CONSTRAINT session_refresh_tokens_session_id_fkey FOREIGN KEY (session_id) REFERENCES public.sessions(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.sessions
    ADD CONSTRAINT sessions_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.web_users(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.staging_objects
    ADD CONSTRAINT staging_objects_storage_id_fkey FOREIGN KEY (storage_id) REFERENCES public.storages(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.usage_history
    ADD CONSTRAINT storage_history_scope_id_fkey FOREIGN KEY (scope_id) REFERENCES public.usage_summary(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.storage_maintenance
    ADD CONSTRAINT storage_maintenance_storage_id_fkey FOREIGN KEY (storage_id) REFERENCES public.storages(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.storage_profiles
    ADD CONSTRAINT storage_profiles_storage_id_fkey FOREIGN KEY (storage_id) REFERENCES public.storages(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.storage_rate_state
    ADD CONSTRAINT storage_rate_state_storage_id_fkey FOREIGN KEY (storage_id) REFERENCES public.storages(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.storage_permits
    ADD CONSTRAINT store_permits_store_id_fkey FOREIGN KEY (storage_id) REFERENCES public.storages(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.storages
    ADD CONSTRAINT stores_current_key_id_fkey FOREIGN KEY (block_key_id) REFERENCES public.managed_keys(id);

ALTER TABLE ONLY public.stream_leases
    ADD CONSTRAINT stream_leases_stream_id_fkey FOREIGN KEY (stream_id) REFERENCES public.streams(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.streams
    ADD CONSTRAINT streams_bucket_id_fkey FOREIGN KEY (bucket_id) REFERENCES public.buckets(id);

ALTER TABLE ONLY public.streams
    ADD CONSTRAINT streams_owner_task_fkey FOREIGN KEY (owner_task_id) REFERENCES public.tasks(id) ON DELETE SET NULL;

ALTER TABLE ONLY public.tasks
    ADD CONSTRAINT tasks_bucket_id_fkey FOREIGN KEY (bucket_id) REFERENCES public.buckets(id) ON DELETE SET NULL;

ALTER TABLE ONLY public.tasks
    ADD CONSTRAINT tasks_lease_node_id_fkey FOREIGN KEY (lease_node_id) REFERENCES public.nodes(id) ON DELETE SET NULL;

ALTER TABLE ONLY public.tasks
    ADD CONSTRAINT tasks_parent_task_id_fkey FOREIGN KEY (parent_task_id) REFERENCES public.tasks(id) ON DELETE SET NULL;

ALTER TABLE ONLY public.tasks
    ADD CONSTRAINT tasks_target_node_id_fkey FOREIGN KEY (target_node_id) REFERENCES public.nodes(id) ON DELETE SET NULL;

ALTER TABLE ONLY public.token_bucket_grants
    ADD CONSTRAINT token_grants_bucket_id_fkey FOREIGN KEY (bucket_id) REFERENCES public.buckets(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.token_bucket_grants
    ADD CONSTRAINT token_grants_token_id_fkey FOREIGN KEY (token_id) REFERENCES public.api_tokens(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.uploads
    ADD CONSTRAINT uploads_bucket_id_fkey FOREIGN KEY (bucket_id) REFERENCES public.buckets(id);

ALTER TABLE ONLY public.uploads
    ADD CONSTRAINT uploads_output_stream_fkey FOREIGN KEY (output_stream) REFERENCES public.streams(id) ON DELETE SET NULL;

ALTER TABLE ONLY public.uploads
    ADD CONSTRAINT uploads_raw_source_id_fkey FOREIGN KEY (raw_source_id) REFERENCES public.raw_sources(id);

ALTER TABLE ONLY public.uploads
    ADD CONSTRAINT uploads_staging_storage_fkey FOREIGN KEY (staging_storage_id) REFERENCES public.storages(id) ON DELETE SET NULL;

ALTER TABLE ONLY public.web_uploads
    ADD CONSTRAINT web_uploads_upload_id_fkey FOREIGN KEY (upload_id) REFERENCES public.uploads(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.web_uploads
    ADD CONSTRAINT web_uploads_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.web_users(id) ON DELETE SET NULL;
