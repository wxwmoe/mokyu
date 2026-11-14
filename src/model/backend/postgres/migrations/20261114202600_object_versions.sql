ALTER TABLE buckets ADD COLUMN version_mode SMALLINT NOT NULL DEFAULT 0 CHECK (version_mode IN (0,1,2));
CREATE TABLE object_version_clocks (
    bucket_id UUID NOT NULL PRIMARY KEY,
    version_seq BIGINT NOT NULL DEFAULT 0,
    FOREIGN KEY (bucket_id) REFERENCES buckets(id) ON DELETE CASCADE
);
ALTER TABLE objects ADD COLUMN version_switch BOOLEAN NOT NULL DEFAULT FALSE;

CREATE TABLE object_versions (
    bucket_id UUID NOT NULL,
    object_key TEXT COLLATE "C" NOT NULL,
    seq BIGINT NOT NULL,
    version_id TEXT NOT NULL,
    stream_id UUID,
    modified_us BIGINT NOT NULL,
    PRIMARY KEY (bucket_id,object_key,seq),
    UNIQUE (bucket_id,object_key,version_id),
    FOREIGN KEY (bucket_id) REFERENCES buckets(id),
    FOREIGN KEY (stream_id) REFERENCES streams(id)
);
CREATE INDEX object_versions_stream ON object_versions(stream_id);

CREATE FUNCTION capture_object_version() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE mode_value SMALLINT;
    next_seq BIGINT;
BEGIN
    IF (TG_OP='INSERT' AND NEW.stream_id IS NULL) OR (TG_OP='UPDATE' AND (OLD.stream_id IS NOT DISTINCT FROM NEW.stream_id OR OLD.version_switch<>NEW.version_switch)) THEN
        RETURN NEW;
    END IF;
    SELECT version_mode INTO STRICT mode_value FROM buckets WHERE id=NEW.bucket_id FOR SHARE;
    IF mode_value>0 THEN
        UPDATE object_version_clocks SET version_seq=version_seq+1 WHERE bucket_id=NEW.bucket_id RETURNING version_seq INTO next_seq;
        IF mode_value=2 THEN
            DELETE FROM object_versions WHERE bucket_id=NEW.bucket_id AND object_key=NEW.object_key AND version_id='null';
        END IF;
        INSERT INTO object_versions(bucket_id,object_key,seq,version_id,stream_id,modified_us)
            VALUES(NEW.bucket_id,NEW.object_key,next_seq,CASE WHEN mode_value=1 THEN replace(gen_random_uuid()::text,'-','') ELSE 'null' END,NEW.stream_id,(extract(epoch FROM clock_timestamp())*1000000)::bigint);
    END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER versions_capture AFTER INSERT OR UPDATE OF stream_id ON objects FOR EACH ROW EXECUTE FUNCTION capture_object_version();

ALTER TABLE member_bucket_grants DROP CONSTRAINT member_grants_actions_check;
ALTER TABLE member_bucket_grants ADD CONSTRAINT member_grants_actions_check CHECK (actions <@ ARRAY['bucket.list','object.read','object.write','object.delete','object.acl','bucket.settings','storage.inspect','object.versions.list','object.versions.read','object.versions.delete','object.versions.restore']);

ALTER TABLE s3_credential_grants DROP CONSTRAINT grants_actions_check;
ALTER TABLE s3_credential_grants ADD CONSTRAINT grants_actions_check CHECK (actions <@ ARRAY['bucket.list','object.read','object.write','object.delete','object.acl','bucket.settings','storage.inspect','object.versions.list','object.versions.read','object.versions.delete','object.versions.restore']);

ALTER TABLE token_bucket_grants DROP CONSTRAINT token_grants_actions_check;
ALTER TABLE token_bucket_grants ADD CONSTRAINT token_grants_actions_check CHECK (actions <@ ARRAY['bucket.list','object.read','object.write','object.delete','object.acl','bucket.settings','storage.inspect','object.versions.list','object.versions.read','object.versions.delete','object.versions.restore']);

CREATE OR REPLACE VIEW user_bucket_access AS
 SELECT m.user_id,
    b.id AS bucket_id,
    ARRAY( SELECT action.action
           FROM unnest(
                CASE m.role
                    WHEN 'reader'::text THEN ARRAY['bucket.list'::text, 'object.read'::text, 'storage.inspect'::text]
                    WHEN 'writer'::text THEN ARRAY['bucket.list'::text, 'object.read'::text, 'object.write'::text, 'object.delete'::text, 'object.acl'::text, 'storage.inspect'::text, 'object.versions.list'::text, 'object.versions.read'::text]
                    ELSE ARRAY['bucket.list'::text, 'object.read'::text, 'object.write'::text, 'object.delete'::text, 'object.acl'::text, 'bucket.settings'::text, 'storage.inspect'::text, 'object.versions.list'::text, 'object.versions.read'::text, 'object.versions.delete'::text, 'object.versions.restore'::text]
                END) action(action)
          WHERE ((m.scope = 'all'::text) OR (action.action = ANY (COALESCE(g.actions, ARRAY[]::text[]))))) AS actions
   FROM ((public.project_members m
     JOIN public.buckets b ON ((b.project_id = m.project_id)))
     LEFT JOIN public.member_bucket_grants g ON (((g.user_id = m.user_id) AND (g.bucket_id = b.id))));

CREATE OR REPLACE VIEW token_bucket_access AS
 SELECT t.id AS token_id,
    g.bucket_id,
    ARRAY( SELECT action.action
           FROM unnest(g.actions) action(action)
          WHERE ((u.role = 'admin'::text) OR (action.action = ANY (COALESCE(a.actions, ARRAY[]::text[]))))) AS actions
   FROM (((public.api_tokens t
     JOIN public.web_users u ON ((u.id = t.user_id)))
     JOIN public.token_bucket_grants g ON ((g.token_id = t.id)))
     LEFT JOIN public.user_bucket_access a ON (((a.user_id = t.user_id) AND (a.bucket_id = g.bucket_id))));

ALTER TABLE journal_entries ADD COLUMN history_subject BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE journal_restore_items ADD COLUMN stage_key TEXT;
ALTER TABLE journal_restore_items ADD COLUMN logical_key TEXT COLLATE "C";
ALTER TABLE journal_restore_items ADD COLUMN history_id VARCHAR(32);
ALTER TABLE journal_restore_items ADD COLUMN history_seq BIGINT;
ALTER TABLE journal_restore_items ADD COLUMN history_time BIGINT;
ALTER TABLE journal_restore_items ADD COLUMN history_marker BOOLEAN NOT NULL DEFAULT FALSE;
CREATE FUNCTION journal_version_insert_fn() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE journal SMALLINT;
BEGIN
    SELECT journal_mode INTO journal FROM bucket_change_gates WHERE bucket_id=NEW.bucket_id FOR SHARE;
    IF journal=1 THEN
        UPDATE journal_buckets SET last_seq=last_seq+1 WHERE bucket_id=NEW.bucket_id;
        INSERT INTO journal_entries(encrypt_frames,bucket_id,seq,generation,operation,object_key,stream_id,payload_revision,attributes,history_subject) SELECT st.block_key_id IS NOT NULL,j.bucket_id,j.last_seq,j.generation,'put',('~h/' || NEW.version_id || '/' || NEW.object_key),NEW.stream_id,s.payload_revision,jsonb_build_object('size',COALESCE(s.size,0),'etag',COALESCE(s.etag,''),'metadata',COALESCE(s.metadata,'{}'::jsonb),'checksums',COALESCE(s.checksums,'{}'::jsonb),'payload',COALESCE(s.payload_type,'chunks'),'modified_us',NEW.modified_us,'history',jsonb_build_object('order',NEW.seq,'time',NEW.modified_us,'marker',CASE WHEN NEW.stream_id IS NULL THEN 1 ELSE 0 END)),TRUE FROM journal_buckets j JOIN storages st ON st.id=j.storage_id LEFT JOIN streams s ON s.id=NEW.stream_id WHERE j.bucket_id=NEW.bucket_id;
    ELSIF journal=2 THEN
        UPDATE journal_buckets SET dirty=TRUE WHERE bucket_id=NEW.bucket_id;
    END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER journal_version_insert AFTER INSERT ON object_versions FOR EACH ROW EXECUTE FUNCTION journal_version_insert_fn();
CREATE FUNCTION journal_version_remove_fn() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE journal SMALLINT;
BEGIN
    SELECT journal_mode INTO journal FROM bucket_change_gates WHERE bucket_id=OLD.bucket_id FOR SHARE;
    IF journal=1 THEN
        UPDATE journal_buckets SET last_seq=last_seq+1 WHERE bucket_id=OLD.bucket_id;
        INSERT INTO journal_entries(encrypt_frames,bucket_id,seq,generation,operation,object_key,stream_id,payload_revision,attributes,history_subject) SELECT st.block_key_id IS NOT NULL,j.bucket_id,j.last_seq,j.generation,'delete',('~h/' || OLD.version_id || '/' || OLD.object_key),NULL,NULL,NULL,TRUE FROM journal_buckets j JOIN storages st ON st.id=j.storage_id WHERE j.bucket_id=OLD.bucket_id;
    ELSIF journal=2 THEN
        UPDATE journal_buckets SET dirty=TRUE WHERE bucket_id=OLD.bucket_id;
    END IF;
    RETURN OLD;
END
$$;
CREATE TRIGGER journal_version_remove AFTER DELETE ON object_versions FOR EACH ROW EXECUTE FUNCTION journal_version_remove_fn();
CREATE FUNCTION journal_version_mode_fn() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE journal SMALLINT;
BEGIN
    SELECT journal_mode INTO journal FROM bucket_change_gates WHERE bucket_id=NEW.id FOR SHARE;
    IF journal=1 THEN
        UPDATE journal_buckets SET last_seq=last_seq+1 WHERE bucket_id=NEW.id;
        INSERT INTO journal_entries(encrypt_frames,bucket_id,seq,generation,operation,object_key,stream_id,payload_revision,attributes,history_subject) SELECT st.block_key_id IS NOT NULL,j.bucket_id,j.last_seq,j.generation,'put','~m',NULL,NULL,jsonb_build_object('size',0,'etag','','metadata','{}'::jsonb,'checksums','{}'::jsonb,'payload','chunks','modified_us',0,'history',jsonb_build_object('mode',NEW.version_mode)),TRUE FROM journal_buckets j JOIN storages st ON st.id=j.storage_id WHERE j.bucket_id=NEW.id;
    ELSIF journal=2 THEN
        UPDATE journal_buckets SET dirty=TRUE WHERE bucket_id=NEW.id;
    END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER journal_version_mode AFTER UPDATE ON buckets FOR EACH ROW WHEN (OLD.version_mode<>NEW.version_mode) EXECUTE FUNCTION journal_version_mode_fn();
UPDATE journal_restore_items SET logical_key=object_key,stage_key=object_key WHERE logical_key IS NULL;

CREATE OR REPLACE FUNCTION quota_object() RETURNS trigger
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
        PERFORM quota_adjust(b,CASE WHEN (SELECT version_mode FROM buckets WHERE id=b)>0 THEN 0 ELSE after_size-before_size END,credits-freed,-inflight,(after_id IS NOT NULL)::int-(before_id IS NOT NULL)::int);
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

CREATE OR REPLACE FUNCTION quota_reserve(s uuid, required bigint, padding boolean, upload uuid, part integer) RETURNS bigint
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
        IF old_id IS NOT NULL AND EXISTS(SELECT 1 FROM object_versions v JOIN buckets b ON b.id=v.bucket_id WHERE v.stream_id=old_id AND (b.version_mode=1 OR v.version_id<>'null')) THEN old_id:=NULL; END IF;
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

CREATE FUNCTION quota_version() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE b uuid; s uuid; delta bigint;
BEGIN
    IF TG_OP='INSERT' THEN b:=NEW.bucket_id; s:=NEW.stream_id; ELSE b:=OLD.bucket_id; s:=OLD.stream_id; END IF;
    IF s IS NOT NULL AND (SELECT version_mode FROM buckets WHERE id=b)>0 THEN
        SELECT size INTO STRICT delta FROM streams WHERE id=s;
        PERFORM quota_adjust(b,CASE WHEN TG_OP='INSERT' THEN delta ELSE -delta END,0,0,0);
    END IF;
    RETURN NULL;
END $$;
CREATE TRIGGER quota_version AFTER INSERT OR DELETE ON object_versions FOR EACH ROW EXECUTE FUNCTION quota_version();

CREATE TABLE object_write_receipts (
    stream_id UUID NOT NULL PRIMARY KEY REFERENCES streams(id) ON DELETE CASCADE,
    version_id VARCHAR(36) NOT NULL
);
CREATE FUNCTION version_write_receipt() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO object_write_receipts(stream_id,version_id) VALUES(NEW.stream_id,NEW.version_id) ON CONFLICT DO NOTHING;
    RETURN NULL;
END $$;
CREATE TRIGGER version_write_receipt AFTER INSERT ON object_versions FOR EACH ROW WHEN (NEW.seq>0 AND NEW.stream_id IS NOT NULL) EXECUTE FUNCTION version_write_receipt();

UPDATE mokyu_meta SET schema_version=20261114202600 WHERE singleton;
