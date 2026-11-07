ALTER TABLE bucket_change_gates ADD COLUMN journal_mode SMALLINT NOT NULL DEFAULT 0 CHECK (journal_mode IN (0,1,2));

CREATE TABLE journal_buckets (
    bucket_id UUID PRIMARY KEY,
    storage_id UUID NOT NULL REFERENCES storages(id),
    bucket_name TEXT NOT NULL,
    generation BIGINT NOT NULL DEFAULT 1 CHECK (generation > 0),
    last_seq BIGINT NOT NULL DEFAULT 0 CHECK (last_seq >= 0),
    published_seq BIGINT NOT NULL DEFAULT 0 CHECK (published_seq >= 0 AND published_seq <= last_seq),
    scanning BOOLEAN NOT NULL DEFAULT TRUE,
    scan_after TEXT,
    scan_until TEXT,
    dirty BOOLEAN NOT NULL DEFAULT FALSE
);
CREATE INDEX journal_buckets_storage ON journal_buckets(storage_id,bucket_id);
CREATE INDEX journal_buckets_scan ON journal_buckets(scanning,bucket_id);

CREATE TABLE journal_entries (
    encrypt_frames BOOLEAN NOT NULL DEFAULT FALSE,
    bucket_id UUID NOT NULL,
    seq BIGINT NOT NULL CHECK (seq > 0),
    generation BIGINT NOT NULL CHECK (generation > 0),
    operation TEXT NOT NULL CHECK (operation IN ('start','ready','put','delete','pause','resume','drop')),
    object_key TEXT,
    stream_id UUID,
    payload_revision BIGINT,
    attributes JSONB,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT (clock_timestamp()),
    PRIMARY KEY (bucket_id,seq),
    FOREIGN KEY (bucket_id) REFERENCES journal_buckets(bucket_id),
    FOREIGN KEY (stream_id) REFERENCES streams(id)
);
CREATE INDEX journal_entries_stream ON journal_entries(stream_id);

CREATE FUNCTION journal_capture(p_bucket UUID, p_key TEXT, p_stream UUID) RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    mode smallint;
    n bigint;
    gen bigint;
BEGIN
    SELECT journal_mode INTO mode FROM bucket_change_gates WHERE bucket_id=p_bucket FOR SHARE;
    IF mode=2 THEN
        UPDATE journal_buckets SET dirty=TRUE WHERE bucket_id=p_bucket AND NOT dirty;
    ELSIF mode=1 THEN
        UPDATE journal_buckets SET last_seq=last_seq+1 WHERE bucket_id=p_bucket
            RETURNING last_seq,generation INTO STRICT n,gen;
        INSERT INTO journal_entries(encrypt_frames,bucket_id,seq,generation,operation,object_key,stream_id,payload_revision,attributes)
        SELECT (SELECT s.block_key_id IS NOT NULL FROM journal_buckets b JOIN storages s ON s.id=b.storage_id WHERE b.bucket_id=p_bucket),p_bucket,n,gen,CASE WHEN p_stream IS NULL THEN 'delete' ELSE 'put' END,p_key,p_stream,
            s.payload_revision,CASE WHEN p_stream IS NOT NULL THEN jsonb_build_object('size',s.size,'etag',s.etag,'metadata',s.metadata,'checksums',s.checksums,'payload',s.payload_type,'modified_us',(extract(epoch FROM s.touched_at)*1000000)::bigint) END
        FROM (SELECT 1) AS one LEFT JOIN streams s ON s.id=p_stream;
    END IF;
END
$$;

CREATE FUNCTION journal_object_change() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP='DELETE' THEN
        PERFORM journal_capture(OLD.bucket_id,OLD.object_key,NULL);
    ELSE
        PERFORM journal_capture(NEW.bucket_id,NEW.object_key,NEW.stream_id);
    END IF;
    RETURN NULL;
END
$$;

CREATE FUNCTION journal_stream_change() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF (OLD.payload_revision,OLD.metadata,OLD.checksums,OLD.etag,OLD.size)
        IS DISTINCT FROM (NEW.payload_revision,NEW.metadata,NEW.checksums,NEW.etag,NEW.size)
        AND EXISTS (SELECT 1 FROM objects WHERE bucket_id=NEW.bucket_id AND object_key=NEW.object_key AND stream_id=NEW.id) THEN
        PERFORM journal_capture(NEW.bucket_id,NEW.object_key,NEW.id);
    END IF;
    RETURN NULL;
END
$$;

CREATE TRIGGER journal_insert AFTER INSERT ON objects FOR EACH ROW
WHEN (NEW.stream_id IS NOT NULL) EXECUTE FUNCTION journal_object_change();

CREATE TRIGGER journal_update AFTER UPDATE ON objects FOR EACH ROW
WHEN (OLD.stream_id IS DISTINCT FROM NEW.stream_id) EXECUTE FUNCTION journal_object_change();

CREATE TRIGGER journal_delete AFTER DELETE ON objects FOR EACH ROW
WHEN (OLD.stream_id IS NOT NULL) EXECUTE FUNCTION journal_object_change();

CREATE TRIGGER journal_stream AFTER UPDATE OF payload_revision,metadata,checksums,etag,size ON streams
FOR EACH ROW EXECUTE FUNCTION journal_stream_change();

CREATE FUNCTION journal_bucket_drop() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF (SELECT journal_mode FROM bucket_change_gates WHERE bucket_id=OLD.id FOR UPDATE)>0 THEN
        UPDATE journal_buckets SET last_seq=last_seq+1,scanning=FALSE,scan_after=NULL,scan_until=NULL WHERE bucket_id=OLD.id;
        INSERT INTO journal_entries(encrypt_frames,bucket_id,generation,seq,operation)
            SELECT (SELECT st.block_key_id IS NOT NULL FROM storages st WHERE st.id=journal_buckets.storage_id),bucket_id,generation,last_seq,'drop' FROM journal_buckets WHERE bucket_id=OLD.id;
    END IF;
    RETURN OLD;
END
$$;

CREATE TRIGGER journal_drop BEFORE DELETE ON buckets FOR EACH ROW EXECUTE FUNCTION journal_bucket_drop();

UPDATE mokyu_meta SET schema_version = 20261107092631;
