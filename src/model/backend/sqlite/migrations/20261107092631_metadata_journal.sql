ALTER TABLE bucket_change_gates ADD COLUMN journal_mode INTEGER NOT NULL DEFAULT 0 CHECK (journal_mode IN (0,1,2));

CREATE TABLE journal_buckets (
    bucket_id BLOB PRIMARY KEY,
    storage_id BLOB NOT NULL REFERENCES storages(id),
    bucket_name TEXT NOT NULL,
    generation INTEGER NOT NULL DEFAULT 1 CHECK (generation > 0),
    last_seq INTEGER NOT NULL DEFAULT 0 CHECK (last_seq >= 0),
    published_seq INTEGER NOT NULL DEFAULT 0 CHECK (published_seq >= 0 AND published_seq <= last_seq),
    scanning INTEGER NOT NULL DEFAULT TRUE,
    scan_after BLOB,
    scan_until BLOB,
    dirty INTEGER NOT NULL DEFAULT FALSE
) STRICT, WITHOUT ROWID;
CREATE INDEX journal_buckets_storage ON journal_buckets(storage_id,bucket_id);
CREATE INDEX journal_buckets_scan ON journal_buckets(scanning,bucket_id);

CREATE TABLE journal_entries (
    encrypt_frames INTEGER NOT NULL DEFAULT 0 CHECK (encrypt_frames IN (0,1)),
    bucket_id BLOB NOT NULL,
    seq INTEGER NOT NULL CHECK (seq > 0),
    generation INTEGER NOT NULL CHECK (generation > 0),
    operation TEXT NOT NULL CHECK (operation IN ('start','ready','put','delete','pause','resume','drop')),
    object_key BLOB,
    stream_id INTEGER,
    payload_revision INTEGER,
    attributes TEXT,
    recorded_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec')*1000000 AS INTEGER)),
    PRIMARY KEY (bucket_id,seq),
    FOREIGN KEY (bucket_id) REFERENCES journal_buckets(bucket_id),
    FOREIGN KEY (stream_id) REFERENCES streams(id)
) STRICT, WITHOUT ROWID;
CREATE INDEX journal_entries_stream ON journal_entries(stream_id);

CREATE TRIGGER journal_insert AFTER INSERT ON objects
WHEN NEW.stream_id IS NOT NULL AND (SELECT journal_mode FROM bucket_change_gates WHERE bucket_id=NEW.bucket_id)>0
BEGIN
    UPDATE journal_buckets SET last_seq=last_seq+1 WHERE bucket_id=NEW.bucket_id AND (SELECT journal_mode FROM bucket_change_gates WHERE bucket_id=NEW.bucket_id)=1;
    UPDATE journal_buckets SET dirty=TRUE WHERE bucket_id=NEW.bucket_id AND NOT dirty AND (SELECT journal_mode FROM bucket_change_gates WHERE bucket_id=NEW.bucket_id)=2;
    INSERT INTO journal_entries(encrypt_frames,bucket_id,seq,generation,operation,object_key,stream_id,payload_revision,attributes)
        SELECT (SELECT st.block_key_id IS NOT NULL FROM storages st WHERE st.id=j.storage_id),j.bucket_id,j.last_seq,j.generation,'put',NEW.object_key,NEW.stream_id,
            s.payload_revision,CASE WHEN s.id IS NOT NULL THEN JSON_OBJECT('size',s.size,'etag',s.etag,'metadata',json(s.metadata),'checksums',json(s.checksums),'payload',s.payload_type,'modified_us',s.touched_at) END
        FROM journal_buckets j LEFT JOIN streams s ON s.id=NEW.stream_id
        WHERE j.bucket_id=NEW.bucket_id AND (SELECT journal_mode FROM bucket_change_gates WHERE bucket_id=NEW.bucket_id)=1;
END;

CREATE TRIGGER journal_update AFTER UPDATE OF stream_id ON objects
WHEN OLD.stream_id IS NOT NEW.stream_id AND (SELECT journal_mode FROM bucket_change_gates WHERE bucket_id=NEW.bucket_id)>0
BEGIN
    UPDATE journal_buckets SET last_seq=last_seq+1 WHERE bucket_id=NEW.bucket_id AND (SELECT journal_mode FROM bucket_change_gates WHERE bucket_id=NEW.bucket_id)=1;
    UPDATE journal_buckets SET dirty=TRUE WHERE bucket_id=NEW.bucket_id AND NOT dirty AND (SELECT journal_mode FROM bucket_change_gates WHERE bucket_id=NEW.bucket_id)=2;
    INSERT INTO journal_entries(encrypt_frames,bucket_id,seq,generation,operation,object_key,stream_id,payload_revision,attributes)
        SELECT (SELECT st.block_key_id IS NOT NULL FROM storages st WHERE st.id=j.storage_id),j.bucket_id,j.last_seq,j.generation,CASE WHEN NEW.stream_id IS NULL THEN 'delete' ELSE 'put' END,NEW.object_key,NEW.stream_id,
            s.payload_revision,CASE WHEN s.id IS NOT NULL THEN JSON_OBJECT('size',s.size,'etag',s.etag,'metadata',json(s.metadata),'checksums',json(s.checksums),'payload',s.payload_type,'modified_us',s.touched_at) END
        FROM journal_buckets j LEFT JOIN streams s ON s.id=NEW.stream_id
        WHERE j.bucket_id=NEW.bucket_id AND (SELECT journal_mode FROM bucket_change_gates WHERE bucket_id=NEW.bucket_id)=1;
END;

CREATE TRIGGER journal_delete AFTER DELETE ON objects
WHEN OLD.stream_id IS NOT NULL AND (SELECT journal_mode FROM bucket_change_gates WHERE bucket_id=OLD.bucket_id)>0
BEGIN
    UPDATE journal_buckets SET last_seq=last_seq+1 WHERE bucket_id=OLD.bucket_id AND (SELECT journal_mode FROM bucket_change_gates WHERE bucket_id=OLD.bucket_id)=1;
    UPDATE journal_buckets SET dirty=TRUE WHERE bucket_id=OLD.bucket_id AND NOT dirty AND (SELECT journal_mode FROM bucket_change_gates WHERE bucket_id=OLD.bucket_id)=2;
    INSERT INTO journal_entries(encrypt_frames,bucket_id,seq,generation,operation,object_key,stream_id,payload_revision,attributes)
        SELECT (SELECT st.block_key_id IS NOT NULL FROM storages st WHERE st.id=j.storage_id),j.bucket_id,j.last_seq,j.generation,'delete',OLD.object_key,NULL,
            NULL,NULL
        FROM journal_buckets j
        WHERE j.bucket_id=OLD.bucket_id AND (SELECT journal_mode FROM bucket_change_gates WHERE bucket_id=OLD.bucket_id)=1;
END;

CREATE TRIGGER journal_stream AFTER UPDATE OF payload_revision,metadata,checksums,etag,size ON streams
WHEN (OLD.payload_revision<>NEW.payload_revision OR OLD.metadata<>NEW.metadata OR OLD.checksums<>NEW.checksums OR OLD.etag<>NEW.etag OR OLD.size<>NEW.size) AND EXISTS (SELECT 1 FROM objects WHERE bucket_id=NEW.bucket_id AND object_key=NEW.object_key AND stream_id=NEW.id) AND (SELECT journal_mode FROM bucket_change_gates WHERE bucket_id=NEW.bucket_id)>0
BEGIN
    UPDATE journal_buckets SET last_seq=last_seq+1 WHERE bucket_id=NEW.bucket_id AND (SELECT journal_mode FROM bucket_change_gates WHERE bucket_id=NEW.bucket_id)=1;
    UPDATE journal_buckets SET dirty=TRUE WHERE bucket_id=NEW.bucket_id AND NOT dirty AND (SELECT journal_mode FROM bucket_change_gates WHERE bucket_id=NEW.bucket_id)=2;
    INSERT INTO journal_entries(encrypt_frames,bucket_id,seq,generation,operation,object_key,stream_id,payload_revision,attributes)
        SELECT (SELECT st.block_key_id IS NOT NULL FROM storages st WHERE st.id=j.storage_id),j.bucket_id,j.last_seq,j.generation,'put',NEW.object_key,NEW.id,
            NEW.payload_revision,JSON_OBJECT('size',NEW.size,'etag',NEW.etag,'metadata',json(NEW.metadata),'checksums',json(NEW.checksums),'payload',NEW.payload_type,'modified_us',NEW.touched_at)
        FROM journal_buckets j
        WHERE j.bucket_id=NEW.bucket_id AND (SELECT journal_mode FROM bucket_change_gates WHERE bucket_id=NEW.bucket_id)=1;
END;

CREATE TRIGGER journal_drop BEFORE DELETE ON buckets
WHEN (SELECT journal_mode FROM bucket_change_gates WHERE bucket_id=OLD.id)>0
BEGIN
    UPDATE journal_buckets SET last_seq=last_seq+1,scanning=FALSE,scan_after=NULL,scan_until=NULL WHERE bucket_id=OLD.id;
    INSERT INTO journal_entries(encrypt_frames,bucket_id,generation,seq,operation)
        SELECT (SELECT st.block_key_id IS NOT NULL FROM storages st WHERE st.id=journal_buckets.storage_id),bucket_id,generation,last_seq,'drop' FROM journal_buckets WHERE bucket_id=OLD.id;
END;

UPDATE mokyu_meta SET schema_version = 20261107092631;
