-- Accounting rows are separate from authorization rows, avoiding SHARE-to-UPDATE upgrades.
CREATE TABLE quota_accounts (
    kind text NOT NULL CHECK (kind IN ('project','bucket')),
    id uuid NOT NULL,
    used_bytes bigint NOT NULL DEFAULT 0 CHECK (used_bytes >= 0),
    reserved_bytes bigint NOT NULL DEFAULT 0 CHECK (reserved_bytes >= 0),
    inflight_bytes bigint NOT NULL DEFAULT 0 CHECK (inflight_bytes >= 0),
    object_count bigint NOT NULL DEFAULT 0 CHECK (object_count >= 0),
    bucket_count bigint NOT NULL DEFAULT 0 CHECK (bucket_count >= 0),
    byte_limit bigint CHECK (byte_limit >= 0),
    inflight_limit bigint CHECK (inflight_limit >= 0),
    bucket_limit bigint CHECK (bucket_limit >= 0),
    PRIMARY KEY(kind,id)
);
CREATE TABLE quota_reservations (
    id uuid PRIMARY KEY,
    bucket_id uuid NOT NULL REFERENCES buckets,
    object_key text COLLATE "C" NOT NULL,
    stream_id uuid UNIQUE REFERENCES streams ON DELETE CASCADE,
    upload_id uuid UNIQUE REFERENCES uploads ON DELETE CASCADE,
    output_stream uuid UNIQUE REFERENCES streams ON DELETE SET NULL,
    closed boolean NOT NULL DEFAULT false,
    base_stream uuid,
    credit_bytes bigint NOT NULL DEFAULT 0 CHECK (credit_bytes >= 0),
    logical_bytes bigint NOT NULL DEFAULT 0 CHECK (logical_bytes >= 0),
    inflight_bytes bigint NOT NULL DEFAULT 0 CHECK (inflight_bytes >= 0),
    CHECK ((stream_id IS NULL) <> (upload_id IS NULL))
);
CREATE UNIQUE INDEX quota_credit ON quota_reservations(bucket_id,object_key,base_stream) WHERE base_stream IS NOT NULL;
CREATE INDEX quota_reservations_bucket ON quota_reservations(bucket_id);
CREATE TABLE quota_writes (
    stream_id uuid PRIMARY KEY REFERENCES streams ON DELETE CASCADE,
    reservation_id uuid NOT NULL REFERENCES quota_reservations ON DELETE CASCADE,
    allocated_bytes bigint NOT NULL DEFAULT 0 CHECK (allocated_bytes >= 0),
    contribution_bytes bigint NOT NULL DEFAULT 0 CHECK (contribution_bytes >= 0)
);
CREATE INDEX quota_writes_reservation ON quota_writes(reservation_id);
ALTER TABLE parts ADD COLUMN quota_size bigint NOT NULL DEFAULT 0 CHECK (quota_size >= 0);

INSERT INTO quota_accounts(kind,id) SELECT 'bucket',id FROM buckets;
UPDATE quota_accounts q SET used_bytes=t.bytes,object_count=t.objects
FROM (SELECT o.bucket_id, sum(s.size)::bigint bytes,count(*) objects FROM objects o JOIN streams s ON s.id=o.stream_id GROUP BY o.bucket_id) t
WHERE q.kind='bucket' AND q.id=t.bucket_id;
UPDATE parts p SET quota_size=s.size FROM streams s WHERE p.stream_id=s.id;
INSERT INTO quota_reservations(id,bucket_id,object_key,upload_id,logical_bytes,inflight_bytes)
SELECT u.id,u.bucket_id,u.object_key,u.id,sum(p.quota_size),sum(p.quota_size)
FROM uploads u JOIN parts p ON p.upload_id=u.id WHERE u.state IN ('active','completing') GROUP BY u.id;
UPDATE quota_accounts q SET reserved_bytes=t.bytes,inflight_bytes=t.bytes
FROM (SELECT bucket_id,sum(logical_bytes)::bigint bytes FROM quota_reservations GROUP BY bucket_id) t
WHERE q.kind='bucket' AND q.id=t.bucket_id;
INSERT INTO quota_accounts(kind,id,used_bytes,reserved_bytes,inflight_bytes,object_count,bucket_count)
SELECT 'project',p.id,coalesce(sum(q.used_bytes),0),coalesce(sum(q.reserved_bytes),0),coalesce(sum(q.inflight_bytes),0),coalesce(sum(q.object_count),0),count(b.id)
FROM projects p LEFT JOIN buckets b ON b.project_id=p.id LEFT JOIN quota_accounts q ON q.kind='bucket' AND q.id=b.id GROUP BY p.id;

CREATE FUNCTION quota_lock(b uuid) RETURNS uuid LANGUAGE plpgsql AS $$
DECLARE p uuid;
BEGIN
    SELECT project_id INTO STRICT p FROM buckets WHERE id=b FOR SHARE;
    PERFORM 1 FROM quota_accounts WHERE kind='project' AND id=p FOR UPDATE;
    PERFORM 1 FROM quota_accounts WHERE kind='bucket' AND id=b FOR UPDATE;
    RETURN p;
END $$;
CREATE FUNCTION quota_adjust(b uuid, used_delta bigint, reserved_delta bigint, inflight_delta bigint, objects_delta bigint) RETURNS void LANGUAGE plpgsql AS $$
DECLARE p uuid; q quota_accounts;
BEGIN
    p := quota_lock(b);
    FOR q IN SELECT * FROM quota_accounts WHERE (kind='project' AND id=p) OR (kind='bucket' AND id=b) LOOP
        IF (used_delta+reserved_delta>0 AND q.byte_limit IS NOT NULL AND q.used_bytes+q.reserved_bytes+used_delta+reserved_delta>q.byte_limit)
          OR (inflight_delta>0 AND q.inflight_limit IS NOT NULL AND q.inflight_bytes+inflight_delta>q.inflight_limit) THEN
            RAISE EXCEPTION 'storage quota exceeded' USING ERRCODE='MKQ01';
        END IF;
    END LOOP;
    UPDATE quota_accounts SET used_bytes=used_bytes+used_delta,reserved_bytes=reserved_bytes+reserved_delta,
        inflight_bytes=inflight_bytes+inflight_delta,object_count=object_count+objects_delta
    WHERE (kind='project' AND id=p) OR (kind='bucket' AND id=b);
END $$;
CREATE FUNCTION quota_change(owner uuid, logical bigint, inflight bigint, credit bigint) RETURNS void LANGUAGE plpgsql AS $$
DECLARE r quota_reservations;
BEGIN
    SELECT * INTO STRICT r FROM quota_reservations WHERE id=owner;
    PERFORM quota_adjust(r.bucket_id,0,greatest(logical-credit,0)-greatest(r.logical_bytes-r.credit_bytes,0),inflight-r.inflight_bytes,0);
    UPDATE quota_reservations SET logical_bytes=logical,inflight_bytes=inflight,credit_bytes=credit WHERE id=owner;
END $$;
CREATE FUNCTION quota_drop(owner uuid) RETURNS void LANGUAGE plpgsql AS $$
DECLARE r quota_reservations;
BEGIN
    SELECT * INTO r FROM quota_reservations WHERE id=owner;
    IF NOT FOUND THEN RETURN; END IF;
    PERFORM quota_change(owner,0,0,0);
    DELETE FROM quota_reservations WHERE id=owner;
END $$;
CREATE FUNCTION quota_forget_write(s uuid) RETURNS void LANGUAGE plpgsql AS $$
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

CREATE FUNCTION quota_reserve(s uuid, required bigint, padding boolean, upload uuid, part integer) RETURNS bigint LANGUAGE plpgsql AS $$
DECLARE info streams; owner uuid; r quota_reservations; w quota_writes; old_id uuid; old_size bigint;
    accepted bigint:=0; target bigint; delta bigint; available bigint; q quota_accounts; project uuid;
BEGIN
    IF required<0 THEN RAISE EXCEPTION 'negative reservation'; END IF;
    IF upload IS NOT NULL THEN PERFORM 1 FROM uploads WHERE id=upload FOR SHARE; END IF;
    -- Keep cancellation cleanup from abandoning this stream before its reservation exists.
    SELECT * INTO STRICT info FROM streams WHERE id=s FOR SHARE;
    project:=quota_lock(info.bucket_id);
    IF info.state<>'writing' OR (upload IS NULL AND NOT EXISTS(SELECT 1 FROM objects WHERE bucket_id=info.bucket_id AND key=info.object_key AND write_epoch=s))
      OR (upload IS NOT NULL AND NOT EXISTS(SELECT 1 FROM parts p JOIN uploads u ON u.id=p.upload_id WHERE p.upload_id=upload AND p.part_number=part AND p.write_epoch=s AND u.state='active')) THEN
        RAISE EXCEPTION 'write was superseded' USING ERRCODE='MKQ02';
    END IF;
    owner:=coalesce(upload,s);
    IF NOT EXISTS(SELECT 1 FROM quota_reservations WHERE id=owner) THEN
        SELECT o.stream_id,st.size INTO old_id,old_size FROM objects o JOIN streams st ON st.id=o.stream_id WHERE o.bucket_id=info.bucket_id AND o.key=info.object_key;
        IF old_id IS NOT NULL AND EXISTS(SELECT 1 FROM quota_reservations WHERE bucket_id=info.bucket_id AND object_key=info.object_key AND base_stream=old_id) THEN old_id:=NULL; END IF;
        INSERT INTO quota_reservations(id,bucket_id,object_key,stream_id,upload_id,base_stream,credit_bytes)
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
        FOR q IN SELECT * FROM quota_accounts WHERE (kind='project' AND id=project) OR (kind='bucket' AND id=info.bucket_id) LOOP
            IF q.byte_limit IS NOT NULL THEN available:=least(available,greatest(q.byte_limit-q.used_bytes-q.reserved_bytes,0)+greatest(r.credit_bytes-r.logical_bytes,0)+greatest(accepted-w.allocated_bytes,0)); END IF;
            IF q.inflight_limit IS NOT NULL THEN available:=least(available,greatest(q.inflight_limit-q.inflight_bytes,0)); END IF;
        END LOOP;
        target:=greatest(required,least(target,w.allocated_bytes+available));
    END IF;
    delta:=greatest(target-accepted,0)-w.contribution_bytes;
    PERFORM quota_change(owner,r.logical_bytes+delta,r.inflight_bytes+target-w.allocated_bytes,r.credit_bytes);
    UPDATE quota_writes SET allocated_bytes=target,contribution_bytes=greatest(target-accepted,0) WHERE stream_id=s;
    RETURN target;
END $$;

CREATE FUNCTION quota_object() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE b uuid; k text; before_id uuid; after_id uuid; before_size bigint:=0; after_size bigint:=0;
    r quota_reservations; freed bigint:=0; inflight bigint:=0; credits bigint:=0; pending bigint:=0;
BEGIN
    b:=coalesce(NEW.bucket_id,OLD.bucket_id); k:=coalesce(NEW.key,OLD.key);
    PERFORM quota_lock(b);
    before_id:=OLD.stream_id; after_id:=NEW.stream_id;
    IF before_id IS DISTINCT FROM after_id THEN
        IF before_id IS NOT NULL THEN SELECT size INTO STRICT before_size FROM streams WHERE id=before_id; END IF;
        IF after_id IS NOT NULL THEN SELECT size INTO STRICT after_size FROM streams WHERE id=after_id; END IF;
        SELECT * INTO r FROM quota_reservations WHERE stream_id=after_id OR output_stream=after_id;
        IF FOUND THEN
            freed:=greatest(r.logical_bytes-r.credit_bytes,0); inflight:=r.inflight_bytes;
            IF r.upload_id IS NOT NULL THEN
                SELECT coalesce(sum(allocated_bytes),0) INTO pending FROM quota_writes WHERE reservation_id=r.id;
            END IF;
            IF pending=0 THEN DELETE FROM quota_reservations WHERE id=r.id;
            ELSE
                UPDATE quota_writes SET contribution_bytes=0 WHERE reservation_id=r.id;
                UPDATE quota_reservations SET logical_bytes=0,inflight_bytes=pending,credit_bytes=0,base_stream=NULL,closed=true,output_stream=NULL WHERE id=r.id;
                inflight:=inflight-pending;
            END IF;
        END IF;
        SELECT coalesce(sum(greatest(logical_bytes,0)-greatest(logical_bytes-credit_bytes,0)),0) INTO credits
        FROM quota_reservations WHERE bucket_id=b AND object_key=k AND base_stream=before_id;
        UPDATE quota_reservations SET credit_bytes=0,base_stream=NULL WHERE bucket_id=b AND object_key=k AND base_stream=before_id;
        PERFORM quota_adjust(b,after_size-before_size,credits-freed,-inflight,(after_id IS NOT NULL)::int-(before_id IS NOT NULL)::int);
    END IF;
    IF TG_OP='UPDATE' AND OLD.write_epoch<>NEW.write_epoch THEN
        SELECT * INTO r FROM quota_reservations WHERE stream_id=OLD.write_epoch;
        IF FOUND THEN
            PERFORM quota_change(r.id,0,r.inflight_bytes,0);
            UPDATE quota_reservations SET base_stream=NULL WHERE id=r.id;
            UPDATE quota_writes SET contribution_bytes=0 WHERE reservation_id=r.id;
        END IF;
    END IF;
    RETURN NULL;
END $$;
CREATE TRIGGER quota_object AFTER INSERT OR UPDATE OR DELETE ON objects FOR EACH ROW EXECUTE FUNCTION quota_object();

CREATE FUNCTION quota_part() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE u uuid; b uuid; r quota_reservations; w quota_writes; size bigint:=0;
BEGIN
    u:=coalesce(NEW.upload_id,OLD.upload_id);
    SELECT bucket_id INTO b FROM uploads WHERE id=u;
    IF b IS NULL THEN RETURN coalesce(NEW,OLD); END IF;
    PERFORM quota_lock(b);
    SELECT * INTO r FROM quota_reservations WHERE id=u;
    IF NOT FOUND OR r.closed THEN RETURN coalesce(NEW,OLD); END IF;
    IF TG_OP='UPDATE' AND OLD.write_epoch<>NEW.write_epoch THEN
        SELECT * INTO w FROM quota_writes WHERE stream_id=OLD.write_epoch;
        IF FOUND THEN
            PERFORM quota_change(u,r.logical_bytes-w.contribution_bytes,r.inflight_bytes,r.credit_bytes);
            UPDATE quota_writes SET contribution_bytes=0 WHERE stream_id=OLD.write_epoch;
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
CREATE TRIGGER quota_part BEFORE INSERT OR UPDATE OR DELETE ON parts FOR EACH ROW EXECUTE FUNCTION quota_part();
CREATE FUNCTION quota_stream_end() RETURNS trigger LANGUAGE plpgsql AS $$
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
CREATE TRIGGER quota_stream_end AFTER UPDATE OF state ON streams FOR EACH ROW EXECUTE FUNCTION quota_stream_end();
CREATE TRIGGER quota_stream_delete BEFORE DELETE ON streams FOR EACH ROW EXECUTE FUNCTION quota_stream_end();
CREATE FUNCTION quota_upload_end() RETURNS trigger LANGUAGE plpgsql AS $$
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
            UPDATE quota_reservations SET closed=true,base_stream=NULL,output_stream=NULL WHERE id=r.id;
            IF pending=0 THEN DELETE FROM quota_reservations WHERE id=r.id; END IF;
        END IF;
    END IF;
    RETURN NULL;
END $$;
CREATE TRIGGER quota_upload_end AFTER UPDATE OF state ON uploads FOR EACH ROW EXECUTE FUNCTION quota_upload_end();
CREATE TRIGGER quota_upload_delete BEFORE DELETE ON uploads FOR EACH ROW EXECUTE FUNCTION quota_upload_end();
CREATE FUNCTION quota_project() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP='INSERT' THEN INSERT INTO quota_accounts(kind,id) VALUES('project',NEW.id);
    ELSE DELETE FROM quota_accounts WHERE kind='project' AND id=OLD.id; END IF;
    RETURN NULL;
END $$;
CREATE TRIGGER quota_project AFTER INSERT OR DELETE ON projects FOR EACH ROW EXECUTE FUNCTION quota_project();
CREATE FUNCTION quota_bucket() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE q quota_accounts; p uuid;
BEGIN
    IF TG_OP='UPDATE' THEN
        IF OLD.project_id<>NEW.project_id THEN
            PERFORM 1 FROM quota_accounts WHERE kind='project' AND id IN (OLD.project_id,NEW.project_id) ORDER BY id FOR UPDATE;
            SELECT * INTO STRICT q FROM quota_accounts WHERE kind='bucket' AND id=NEW.id FOR UPDATE;
            IF q.used_bytes<>0 OR q.inflight_bytes<>0 OR q.object_count<>0 THEN RAISE EXCEPTION 'use the bucket transfer operation' USING ERRCODE='MKQ02'; END IF;
            SELECT * INTO STRICT q FROM quota_accounts WHERE kind='project' AND id=NEW.project_id;
            IF q.bucket_limit IS NOT NULL AND q.bucket_count>=q.bucket_limit THEN RAISE EXCEPTION 'storage quota exceeded' USING ERRCODE='MKQ01'; END IF;
            UPDATE quota_accounts SET bucket_count=bucket_count-1 WHERE kind='project' AND id=OLD.project_id;
            UPDATE quota_accounts SET bucket_count=bucket_count+1 WHERE kind='project' AND id=NEW.project_id;
        END IF;
        RETURN NULL;
    END IF;
    p:=coalesce(NEW.project_id,OLD.project_id);
    SELECT * INTO STRICT q FROM quota_accounts WHERE kind='project' AND id=p FOR UPDATE;
    IF TG_OP='INSERT' THEN
        IF q.bucket_limit IS NOT NULL AND q.bucket_count>=q.bucket_limit THEN RAISE EXCEPTION 'storage quota exceeded' USING ERRCODE='MKQ01'; END IF;
        UPDATE quota_accounts SET bucket_count=bucket_count+1 WHERE kind='project' AND id=p;
        INSERT INTO quota_accounts(kind,id) VALUES('bucket',NEW.id);
    ELSE
        UPDATE quota_accounts SET bucket_count=bucket_count-1 WHERE kind='project' AND id=p;
        DELETE FROM quota_accounts WHERE kind='bucket' AND id=OLD.id;
    END IF;
    RETURN NULL;
END $$;
CREATE TRIGGER quota_bucket AFTER INSERT OR DELETE OR UPDATE OF project_id ON buckets FOR EACH ROW EXECUTE FUNCTION quota_bucket();

UPDATE mokyu_meta SET schema_version=15;
