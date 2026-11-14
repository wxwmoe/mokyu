ALTER TABLE journal_restore_items ADD COLUMN origin_stream UUID;
CREATE INDEX restore_item_origin ON journal_restore_items(job,source_bucket,origin_stream);

CREATE TABLE bucket_time_windows (
    bucket_id UUID PRIMARY KEY NOT NULL,
    seconds BIGINT NOT NULL CHECK(seconds>0),
    gaps TEXT NOT NULL,
    floor_us BIGINT NOT NULL,
    last_us BIGINT NOT NULL,
    seq BIGINT NOT NULL,
    FOREIGN KEY(bucket_id) REFERENCES buckets(id) ON DELETE CASCADE
);

CREATE TABLE object_time_points (
    bucket_id UUID NOT NULL,
    object_key TEXT COLLATE "C" NOT NULL,
    seq BIGINT NOT NULL,
    seen_us BIGINT NOT NULL,
    stream_id UUID,
    PRIMARY KEY(bucket_id,object_key,seq),
    FOREIGN KEY(bucket_id) REFERENCES bucket_time_windows(bucket_id) ON DELETE CASCADE,
    FOREIGN KEY(stream_id) REFERENCES streams(id)
);
CREATE INDEX time_points_clock ON object_time_points(bucket_id,seen_us,seq);
CREATE INDEX time_points_stream ON object_time_points(stream_id);

CREATE TABLE time_retained_streams (
    bucket_id UUID NOT NULL,
    stream_id UUID NOT NULL,
    bytes BIGINT NOT NULL,
    PRIMARY KEY(bucket_id,stream_id),
    FOREIGN KEY(bucket_id) REFERENCES buckets(id),
    FOREIGN KEY(stream_id) REFERENCES streams(id)
);

CREATE TABLE time_restore_plans (
    id UUID NOT NULL PRIMARY KEY,
    source_bucket UUID NOT NULL,
    target_bucket UUID NOT NULL,
    worker_token UUID,
    lease_until_us BIGINT NOT NULL DEFAULT 0,
    request_json TEXT NOT NULL,
    owner_json TEXT NOT NULL,
    digest TEXT NOT NULL,
    cut_seq BIGINT NOT NULL,
    at_us BIGINT NOT NULL,
    expires_us BIGINT NOT NULL,
    phase TEXT NOT NULL CHECK(phase IN ('preview','running','done','expired')),
    FOREIGN KEY(source_bucket) REFERENCES buckets(id) ON DELETE CASCADE,
    FOREIGN KEY(target_bucket) REFERENCES buckets(id) ON DELETE CASCADE
);
CREATE INDEX time_plan_expiry ON time_restore_plans(phase,expires_us);
CREATE TABLE time_restore_items (
    plan_id UUID NOT NULL,
    object_key TEXT COLLATE "C" NOT NULL,
    target_key TEXT COLLATE "C" NOT NULL,
    source_stream UUID,
    target_changed BOOLEAN NOT NULL DEFAULT FALSE,
    expected_stream UUID,
    item_state TEXT NOT NULL CHECK(item_state IN ('add','replace','remove','skip','missing','done','conflict')),
    PRIMARY KEY(plan_id,object_key),
    FOREIGN KEY(plan_id) REFERENCES time_restore_plans(id) ON DELETE CASCADE,
    FOREIGN KEY(source_stream) REFERENCES streams(id)
);
CREATE INDEX time_item_stream ON time_restore_items(source_stream);
CREATE INDEX time_plan_target ON time_restore_plans(target_bucket,phase,id);
CREATE INDEX time_item_target ON time_restore_items(target_key,plan_id);
CREATE INDEX time_item_next ON time_restore_items(plan_id,item_state,object_key);

CREATE FUNCTION capture_time_point() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF (TG_OP='INSERT' AND NEW.stream_id IS NULL) OR (TG_OP='UPDATE' AND OLD.stream_id IS NOT DISTINCT FROM NEW.stream_id) THEN RETURN NEW; END IF;
    PERFORM 1 FROM buckets WHERE id=NEW.bucket_id FOR SHARE;
    UPDATE time_restore_items SET target_changed=TRUE WHERE target_key=NEW.object_key AND item_state IN ('add','replace','remove') AND plan_id IN (SELECT id FROM time_restore_plans WHERE target_bucket=NEW.bucket_id AND phase IN ('preview','running'));
    UPDATE bucket_time_windows SET last_us=GREATEST(last_us+1,(extract(epoch FROM clock_timestamp())*1000000)::bigint),seq=seq+1 WHERE bucket_id=NEW.bucket_id;
    IF FOUND THEN INSERT INTO object_time_points(bucket_id,object_key,seq,seen_us,stream_id) SELECT bucket_id,NEW.object_key,seq,last_us,NEW.stream_id FROM bucket_time_windows WHERE bucket_id=NEW.bucket_id; END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER time_capture AFTER INSERT OR UPDATE OF stream_id ON objects FOR EACH ROW EXECUTE FUNCTION capture_time_point();
CREATE FUNCTION retain_time_stream() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_TABLE_NAME='objects' THEN INSERT INTO time_retained_streams(bucket_id,stream_id,bytes) SELECT OLD.bucket_id,OLD.stream_id,s.size FROM streams s WHERE s.id=OLD.stream_id AND OLD.stream_id IS NOT NULL AND (SELECT version_mode FROM buckets WHERE id=OLD.bucket_id)=0 AND EXISTS(SELECT 1 FROM object_time_points t WHERE t.bucket_id=OLD.bucket_id AND t.stream_id=OLD.stream_id) ON CONFLICT DO NOTHING; ELSE INSERT INTO time_retained_streams(bucket_id,stream_id,bytes) SELECT OLD.bucket_id,OLD.stream_id,s.size FROM streams s WHERE s.id=OLD.stream_id AND OLD.stream_id IS NOT NULL AND (SELECT version_mode FROM buckets WHERE id=OLD.bucket_id)>0 AND EXISTS(SELECT 1 FROM object_time_points t WHERE t.bucket_id=OLD.bucket_id AND t.stream_id=OLD.stream_id) AND NOT EXISTS(SELECT 1 FROM object_versions v WHERE v.bucket_id=OLD.bucket_id AND v.stream_id=OLD.stream_id) ON CONFLICT DO NOTHING; END IF;
    RETURN NULL;
END $$;
CREATE TRIGGER zz_time_old_head AFTER UPDATE OF stream_id ON objects FOR EACH ROW WHEN(OLD.stream_id IS DISTINCT FROM NEW.stream_id AND OLD.stream_id IS NOT NULL) EXECUTE FUNCTION retain_time_stream();
CREATE TRIGGER zz_time_old_version AFTER DELETE ON object_versions FOR EACH ROW WHEN(OLD.stream_id IS NOT NULL) EXECUTE FUNCTION retain_time_stream();
CREATE FUNCTION charge_time_stream() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP='INSERT' THEN PERFORM quota_adjust(NEW.bucket_id,NEW.bytes,0,0,0);
    ELSE PERFORM quota_adjust(OLD.bucket_id,-OLD.bytes,0,0,0); END IF;
    RETURN NULL;
END $$;
CREATE TRIGGER charge_time_stream AFTER INSERT OR DELETE ON time_retained_streams FOR EACH ROW EXECUTE FUNCTION charge_time_stream();

CREATE FUNCTION journal_time_insert_fn() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE journal SMALLINT;
BEGIN
    SELECT journal_mode INTO journal FROM bucket_change_gates WHERE bucket_id=NEW.bucket_id FOR SHARE;
    IF journal=1 THEN
        UPDATE journal_buckets SET last_seq=last_seq+1 WHERE bucket_id=NEW.bucket_id;
        INSERT INTO journal_entries(encrypt_frames,bucket_id,seq,generation,operation,object_key,stream_id,payload_revision,attributes,history_subject) SELECT st.block_key_id IS NOT NULL,j.bucket_id,j.last_seq,j.generation,'put',('~t/' || NEW.seq || '/' || NEW.object_key),NEW.stream_id,s.payload_revision,jsonb_build_object('size',COALESCE(s.size,0),'etag',COALESCE(s.etag,''),'metadata',COALESCE(s.metadata,'{}'::jsonb),'checksums',COALESCE(s.checksums,'{}'::jsonb),'payload',COALESCE(s.payload_type,'chunks'),'modified_us',NEW.seen_us,'history',jsonb_build_object('tick',NEW.seq,'time',NEW.seen_us,'marker',CASE WHEN NEW.stream_id IS NULL THEN 1 ELSE 0 END)),TRUE FROM journal_buckets j JOIN storages st ON st.id=j.storage_id LEFT JOIN streams s ON s.id=NEW.stream_id WHERE j.bucket_id=NEW.bucket_id;
    ELSIF journal=2 THEN
        UPDATE journal_buckets SET dirty=TRUE WHERE bucket_id=NEW.bucket_id;
    END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER journal_time_insert AFTER INSERT ON object_time_points FOR EACH ROW EXECUTE FUNCTION journal_time_insert_fn();
CREATE FUNCTION journal_time_remove_fn() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE journal SMALLINT;
BEGIN
    SELECT journal_mode INTO journal FROM bucket_change_gates WHERE bucket_id=OLD.bucket_id FOR SHARE;
    IF journal=1 THEN
        UPDATE journal_buckets SET last_seq=last_seq+1 WHERE bucket_id=OLD.bucket_id;
        INSERT INTO journal_entries(encrypt_frames,bucket_id,seq,generation,operation,object_key,stream_id,payload_revision,attributes,history_subject) SELECT st.block_key_id IS NOT NULL,j.bucket_id,j.last_seq,j.generation,'delete',('~t/' || OLD.seq || '/' || OLD.object_key),NULL,NULL,NULL,TRUE FROM journal_buckets j JOIN storages st ON st.id=j.storage_id WHERE j.bucket_id=OLD.bucket_id;
    ELSIF journal=2 THEN
        UPDATE journal_buckets SET dirty=TRUE WHERE bucket_id=OLD.bucket_id;
    END IF;
    RETURN OLD;
END
$$;
CREATE TRIGGER journal_time_remove AFTER DELETE ON object_time_points FOR EACH ROW EXECUTE FUNCTION journal_time_remove_fn();
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
        IF old_id IS NOT NULL AND EXISTS(SELECT 1 FROM object_time_points WHERE bucket_id=info.bucket_id AND stream_id=old_id) THEN old_id:=NULL; END IF;
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


UPDATE mokyu_meta SET schema_version=20261114231000 WHERE singleton;
