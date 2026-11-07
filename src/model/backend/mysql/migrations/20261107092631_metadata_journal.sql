ALTER TABLE bucket_change_gates ADD COLUMN journal_mode SMALLINT NOT NULL DEFAULT 0 CHECK (journal_mode IN (0,1,2));

CREATE TABLE journal_buckets (
    bucket_id BINARY(16) PRIMARY KEY,
    storage_id BINARY(16) NOT NULL,
    bucket_name VARCHAR(128) NOT NULL,
    generation BIGINT NOT NULL DEFAULT 1 CHECK (generation > 0),
    last_seq BIGINT NOT NULL DEFAULT 0 CHECK (last_seq >= 0),
    published_seq BIGINT NOT NULL DEFAULT 0,
    CHECK (published_seq >= 0 AND published_seq <= last_seq),
    scanning BOOLEAN NOT NULL DEFAULT TRUE,
    scan_after VARBINARY(1024),
    scan_until VARBINARY(1024),
    dirty BOOLEAN NOT NULL DEFAULT FALSE,
    FOREIGN KEY (storage_id) REFERENCES storages(id)
);
CREATE INDEX journal_buckets_storage ON journal_buckets(storage_id,bucket_id);
CREATE INDEX journal_buckets_scan ON journal_buckets(scanning,bucket_id);

CREATE TABLE journal_entries (
    encrypt_frames BOOLEAN NOT NULL DEFAULT FALSE,
    bucket_id BINARY(16) NOT NULL,
    seq BIGINT NOT NULL CHECK (seq > 0),
    generation BIGINT NOT NULL CHECK (generation > 0),
    operation VARCHAR(8) NOT NULL CHECK (operation IN ('start','ready','put','delete','pause','resume','drop')),
    object_key VARBINARY(1024),
    stream_id BIGINT,
    payload_revision BIGINT,
    attributes JSON,
    recorded_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (bucket_id,seq),
    FOREIGN KEY (bucket_id) REFERENCES journal_buckets(bucket_id),
    FOREIGN KEY (stream_id) REFERENCES streams(id)
);
CREATE INDEX journal_entries_stream ON journal_entries(stream_id);

CREATE TRIGGER journal_insert AFTER INSERT ON objects FOR EACH ROW
BEGIN
    DECLARE mode_value SMALLINT DEFAULT 0;
    IF NEW.stream_id IS NOT NULL THEN
        SELECT journal_mode INTO mode_value FROM bucket_change_gates WHERE bucket_id=NEW.bucket_id LOCK IN SHARE MODE;
        IF mode_value=2 THEN
            UPDATE journal_buckets SET dirty=TRUE WHERE bucket_id=NEW.bucket_id AND NOT dirty;
        ELSEIF mode_value=1 THEN
            UPDATE journal_buckets SET last_seq=last_seq+1 WHERE bucket_id=NEW.bucket_id;
            INSERT INTO journal_entries(encrypt_frames,bucket_id,seq,generation,operation,object_key,stream_id,payload_revision,attributes)
        SELECT (SELECT st.block_key_id IS NOT NULL FROM storages st WHERE st.id=j.storage_id),j.bucket_id,j.last_seq,j.generation,'put',NEW.object_key,NEW.stream_id,
            s.payload_revision,CASE WHEN s.id IS NOT NULL THEN JSON_OBJECT('size',s.size,'etag',s.etag,'metadata',JSON_EXTRACT(s.metadata,'$'),'checksums',JSON_EXTRACT(s.checksums,'$'),'payload',s.payload_type,'modified_us',TIMESTAMPDIFF(MICROSECOND,'1970-01-01',s.touched_at)) END
        FROM journal_buckets j LEFT JOIN streams s ON s.id=NEW.stream_id
        WHERE j.bucket_id=NEW.bucket_id;
        END IF;
    END IF;
END;

CREATE TRIGGER journal_update AFTER UPDATE ON objects FOR EACH ROW
BEGIN
    DECLARE mode_value SMALLINT DEFAULT 0;
    IF NOT (OLD.stream_id <=> NEW.stream_id) THEN
        SELECT journal_mode INTO mode_value FROM bucket_change_gates WHERE bucket_id=NEW.bucket_id LOCK IN SHARE MODE;
        IF mode_value=2 THEN
            UPDATE journal_buckets SET dirty=TRUE WHERE bucket_id=NEW.bucket_id AND NOT dirty;
        ELSEIF mode_value=1 THEN
            UPDATE journal_buckets SET last_seq=last_seq+1 WHERE bucket_id=NEW.bucket_id;
            INSERT INTO journal_entries(encrypt_frames,bucket_id,seq,generation,operation,object_key,stream_id,payload_revision,attributes)
        SELECT (SELECT st.block_key_id IS NOT NULL FROM storages st WHERE st.id=j.storage_id),j.bucket_id,j.last_seq,j.generation,CASE WHEN NEW.stream_id IS NULL THEN 'delete' ELSE 'put' END,NEW.object_key,NEW.stream_id,
            s.payload_revision,CASE WHEN s.id IS NOT NULL THEN JSON_OBJECT('size',s.size,'etag',s.etag,'metadata',JSON_EXTRACT(s.metadata,'$'),'checksums',JSON_EXTRACT(s.checksums,'$'),'payload',s.payload_type,'modified_us',TIMESTAMPDIFF(MICROSECOND,'1970-01-01',s.touched_at)) END
        FROM journal_buckets j LEFT JOIN streams s ON s.id=NEW.stream_id
        WHERE j.bucket_id=NEW.bucket_id;
        END IF;
    END IF;
END;

CREATE TRIGGER journal_delete AFTER DELETE ON objects FOR EACH ROW
BEGIN
    DECLARE mode_value SMALLINT DEFAULT 0;
    IF OLD.stream_id IS NOT NULL THEN
        SELECT journal_mode INTO mode_value FROM bucket_change_gates WHERE bucket_id=OLD.bucket_id LOCK IN SHARE MODE;
        IF mode_value=2 THEN
            UPDATE journal_buckets SET dirty=TRUE WHERE bucket_id=OLD.bucket_id AND NOT dirty;
        ELSEIF mode_value=1 THEN
            UPDATE journal_buckets SET last_seq=last_seq+1 WHERE bucket_id=OLD.bucket_id;
            INSERT INTO journal_entries(encrypt_frames,bucket_id,seq,generation,operation,object_key,stream_id,payload_revision,attributes)
        SELECT (SELECT st.block_key_id IS NOT NULL FROM storages st WHERE st.id=j.storage_id),j.bucket_id,j.last_seq,j.generation,'delete',OLD.object_key,NULL,
            NULL,NULL
        FROM journal_buckets j
        WHERE j.bucket_id=OLD.bucket_id;
        END IF;
    END IF;
END;

CREATE TRIGGER journal_stream AFTER UPDATE ON streams FOR EACH ROW
BEGIN
    DECLARE mode_value SMALLINT DEFAULT 0;
    IF (OLD.payload_revision<>NEW.payload_revision OR NOT (OLD.metadata <=> NEW.metadata) OR NOT (OLD.checksums <=> NEW.checksums) OR OLD.etag<>NEW.etag OR OLD.size<>NEW.size) AND EXISTS (SELECT 1 FROM objects WHERE bucket_id=NEW.bucket_id AND object_key=NEW.object_key AND stream_id=NEW.id) THEN
        SELECT journal_mode INTO mode_value FROM bucket_change_gates WHERE bucket_id=NEW.bucket_id LOCK IN SHARE MODE;
        IF mode_value=2 THEN
            UPDATE journal_buckets SET dirty=TRUE WHERE bucket_id=NEW.bucket_id AND NOT dirty;
        ELSEIF mode_value=1 THEN
            UPDATE journal_buckets SET last_seq=last_seq+1 WHERE bucket_id=NEW.bucket_id;
            INSERT INTO journal_entries(encrypt_frames,bucket_id,seq,generation,operation,object_key,stream_id,payload_revision,attributes)
        SELECT (SELECT st.block_key_id IS NOT NULL FROM storages st WHERE st.id=j.storage_id),j.bucket_id,j.last_seq,j.generation,'put',NEW.object_key,NEW.id,
            NEW.payload_revision,JSON_OBJECT('size',NEW.size,'etag',NEW.etag,'metadata',JSON_EXTRACT(NEW.metadata,'$'),'checksums',JSON_EXTRACT(NEW.checksums,'$'),'payload',NEW.payload_type,'modified_us',TIMESTAMPDIFF(MICROSECOND,'1970-01-01',NEW.touched_at))
        FROM journal_buckets j
        WHERE j.bucket_id=NEW.bucket_id;
        END IF;
    END IF;
END;

CREATE TRIGGER journal_drop BEFORE DELETE ON buckets FOR EACH ROW
BEGIN
    DECLARE mode_value SMALLINT DEFAULT 0;
    SELECT journal_mode INTO mode_value FROM bucket_change_gates WHERE bucket_id=OLD.id FOR UPDATE;
    IF mode_value>0 THEN
        UPDATE journal_buckets SET last_seq=last_seq+1,scanning=FALSE,scan_after=NULL,scan_until=NULL WHERE bucket_id=OLD.id;
        INSERT INTO journal_entries(encrypt_frames,bucket_id,generation,seq,operation)
            SELECT (SELECT st.block_key_id IS NOT NULL FROM storages st WHERE st.id=journal_buckets.storage_id),bucket_id,generation,last_seq,'drop' FROM journal_buckets WHERE bucket_id=OLD.id;
    END IF;
END;

UPDATE mokyu_meta SET schema_version = 20261107092631;
