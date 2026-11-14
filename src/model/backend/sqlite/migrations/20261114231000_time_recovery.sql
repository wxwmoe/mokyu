ALTER TABLE journal_restore_items ADD COLUMN origin_stream BLOB;
CREATE INDEX restore_item_origin ON journal_restore_items(job,source_bucket,origin_stream);

CREATE TABLE bucket_time_windows (
    bucket_id BLOB PRIMARY KEY NOT NULL,
    seconds INTEGER NOT NULL CHECK(seconds>0),
    gaps TEXT NOT NULL,
    floor_us INTEGER NOT NULL,
    last_us INTEGER NOT NULL,
    seq INTEGER NOT NULL,
    FOREIGN KEY(bucket_id) REFERENCES buckets(id) ON DELETE CASCADE
) STRICT, WITHOUT ROWID;

CREATE TABLE object_time_points (
    bucket_id BLOB NOT NULL,
    object_key BLOB NOT NULL,
    seq INTEGER NOT NULL,
    seen_us INTEGER NOT NULL,
    stream_id INTEGER,
    PRIMARY KEY(bucket_id,object_key,seq),
    FOREIGN KEY(bucket_id) REFERENCES bucket_time_windows(bucket_id) ON DELETE CASCADE,
    FOREIGN KEY(stream_id) REFERENCES streams(id)
) STRICT, WITHOUT ROWID;
CREATE INDEX time_points_clock ON object_time_points(bucket_id,seen_us,seq);
CREATE INDEX time_points_stream ON object_time_points(stream_id);

CREATE TABLE time_retained_streams (
    bucket_id BLOB NOT NULL,
    stream_id INTEGER NOT NULL,
    bytes INTEGER NOT NULL,
    PRIMARY KEY(bucket_id,stream_id),
    FOREIGN KEY(bucket_id) REFERENCES buckets(id),
    FOREIGN KEY(stream_id) REFERENCES streams(id)
) STRICT, WITHOUT ROWID;

CREATE TABLE time_restore_plans (
    id BLOB NOT NULL PRIMARY KEY,
    source_bucket BLOB NOT NULL,
    target_bucket BLOB NOT NULL,
    worker_token BLOB,
    lease_until_us INTEGER NOT NULL DEFAULT 0,
    request_json TEXT NOT NULL,
    owner_json TEXT NOT NULL,
    digest TEXT NOT NULL,
    cut_seq INTEGER NOT NULL,
    at_us INTEGER NOT NULL,
    expires_us INTEGER NOT NULL,
    phase TEXT NOT NULL CHECK(phase IN ('preview','running','done','expired')),
    FOREIGN KEY(source_bucket) REFERENCES buckets(id) ON DELETE CASCADE,
    FOREIGN KEY(target_bucket) REFERENCES buckets(id) ON DELETE CASCADE
) STRICT, WITHOUT ROWID;
CREATE INDEX time_plan_expiry ON time_restore_plans(phase,expires_us);
CREATE TABLE time_restore_items (
    plan_id BLOB NOT NULL,
    object_key BLOB NOT NULL,
    target_key BLOB NOT NULL,
    source_stream INTEGER,
    target_changed INTEGER NOT NULL DEFAULT 0,
    expected_stream INTEGER,
    item_state TEXT NOT NULL CHECK(item_state IN ('add','replace','remove','skip','missing','done','conflict')),
    PRIMARY KEY(plan_id,object_key),
    FOREIGN KEY(plan_id) REFERENCES time_restore_plans(id) ON DELETE CASCADE,
    FOREIGN KEY(source_stream) REFERENCES streams(id)
) STRICT, WITHOUT ROWID;
CREATE INDEX time_item_stream ON time_restore_items(source_stream);
CREATE INDEX time_plan_target ON time_restore_plans(target_bucket,phase,id);
CREATE INDEX time_item_target ON time_restore_items(target_key,plan_id);
CREATE INDEX time_item_next ON time_restore_items(plan_id,item_state,object_key);

CREATE TRIGGER time_guard_insert AFTER INSERT ON objects WHEN NEW.stream_id IS NOT NULL
BEGIN
    UPDATE time_restore_items SET target_changed=TRUE WHERE target_key=NEW.object_key AND item_state IN ('add','replace','remove') AND plan_id IN (SELECT id FROM time_restore_plans WHERE target_bucket=NEW.bucket_id AND phase IN ('preview','running'));
END;
CREATE TRIGGER time_guard_update AFTER UPDATE OF stream_id ON objects WHEN OLD.stream_id IS NOT NEW.stream_id
BEGIN
    UPDATE time_restore_items SET target_changed=TRUE WHERE target_key=NEW.object_key AND item_state IN ('add','replace','remove') AND plan_id IN (SELECT id FROM time_restore_plans WHERE target_bucket=NEW.bucket_id AND phase IN ('preview','running'));
END;
CREATE TRIGGER time_capture_insert AFTER INSERT ON objects WHEN NEW.stream_id IS NOT NULL AND EXISTS(SELECT 1 FROM bucket_time_windows WHERE bucket_id=NEW.bucket_id)
BEGIN
    UPDATE bucket_time_windows SET last_us=MAX(last_us+1,CAST(unixepoch('subsec')*1000000 AS INTEGER)),seq=seq+1 WHERE bucket_id=NEW.bucket_id;
    INSERT INTO object_time_points(bucket_id,object_key,seq,seen_us,stream_id) SELECT bucket_id,NEW.object_key,seq,last_us,NEW.stream_id FROM bucket_time_windows WHERE bucket_id=NEW.bucket_id;
END;
CREATE TRIGGER time_capture_update AFTER UPDATE ON objects WHEN OLD.stream_id IS NOT NEW.stream_id AND EXISTS(SELECT 1 FROM bucket_time_windows WHERE bucket_id=NEW.bucket_id)
BEGIN
    UPDATE bucket_time_windows SET last_us=MAX(last_us+1,CAST(unixepoch('subsec')*1000000 AS INTEGER)),seq=seq+1 WHERE bucket_id=NEW.bucket_id;
    INSERT INTO object_time_points(bucket_id,object_key,seq,seen_us,stream_id) SELECT bucket_id,NEW.object_key,seq,last_us,NEW.stream_id FROM bucket_time_windows WHERE bucket_id=NEW.bucket_id;
END;
CREATE TRIGGER time_old_head AFTER UPDATE ON objects WHEN OLD.stream_id IS NOT NULL AND OLD.stream_id IS NOT NEW.stream_id
BEGIN
    INSERT OR IGNORE INTO time_retained_streams(bucket_id,stream_id,bytes) SELECT OLD.bucket_id,OLD.stream_id,s.size FROM streams s WHERE s.id=OLD.stream_id AND OLD.stream_id IS NOT NULL AND (SELECT version_mode FROM buckets WHERE id=OLD.bucket_id)=0 AND EXISTS(SELECT 1 FROM object_time_points t WHERE t.bucket_id=OLD.bucket_id AND t.stream_id=OLD.stream_id);
END;
CREATE TRIGGER time_old_version AFTER DELETE ON object_versions WHEN OLD.stream_id IS NOT NULL
BEGIN
    INSERT OR IGNORE INTO time_retained_streams(bucket_id,stream_id,bytes) SELECT OLD.bucket_id,OLD.stream_id,s.size FROM streams s WHERE s.id=OLD.stream_id AND OLD.stream_id IS NOT NULL AND (SELECT version_mode FROM buckets WHERE id=OLD.bucket_id)>0 AND EXISTS(SELECT 1 FROM object_time_points t WHERE t.bucket_id=OLD.bucket_id AND t.stream_id=OLD.stream_id) AND NOT EXISTS(SELECT 1 FROM object_versions v WHERE v.bucket_id=OLD.bucket_id AND v.stream_id=OLD.stream_id);
END;
CREATE TRIGGER charge_time_stream_insert AFTER INSERT ON time_retained_streams
BEGIN
    UPDATE quota_scopes SET used_bytes=used_bytes+NEW.bytes WHERE (scope='bucket' AND scope_id=NEW.bucket_id) OR (scope='project' AND scope_id=(SELECT project_id FROM buckets WHERE id=NEW.bucket_id));
END;
CREATE TRIGGER charge_time_stream_delete AFTER DELETE ON time_retained_streams
BEGIN
    UPDATE quota_scopes SET used_bytes=used_bytes-OLD.bytes WHERE (scope='bucket' AND scope_id=OLD.bucket_id) OR (scope='project' AND scope_id=(SELECT project_id FROM buckets WHERE id=OLD.bucket_id));
END;

CREATE TRIGGER journal_time_insert AFTER INSERT ON object_time_points WHEN (SELECT journal_mode FROM bucket_change_gates WHERE bucket_id=NEW.bucket_id)>0
BEGIN
    UPDATE journal_buckets SET last_seq=last_seq+1 WHERE bucket_id=NEW.bucket_id AND (SELECT journal_mode FROM bucket_change_gates WHERE bucket_id=NEW.bucket_id)=1;
    INSERT INTO journal_entries(encrypt_frames,bucket_id,seq,generation,operation,object_key,stream_id,payload_revision,attributes,history_subject) SELECT st.block_key_id IS NOT NULL,j.bucket_id,j.last_seq,j.generation,'put',CAST(('~t/' || NEW.seq || '/' || NEW.object_key) AS BLOB),NEW.stream_id,s.payload_revision,json_object('size',COALESCE(s.size,0),'etag',COALESCE(s.etag,''),'metadata',json(COALESCE(s.metadata,'{}')),'checksums',json(COALESCE(s.checksums,'{}')),'payload',COALESCE(s.payload_type,'chunks'),'modified_us',NEW.seen_us,'history',json_object('tick',NEW.seq,'time',NEW.seen_us,'marker',CASE WHEN NEW.stream_id IS NULL THEN 1 ELSE 0 END)),TRUE FROM journal_buckets j JOIN storages st ON st.id=j.storage_id LEFT JOIN streams s ON s.id=NEW.stream_id WHERE j.bucket_id=NEW.bucket_id AND (SELECT journal_mode FROM bucket_change_gates WHERE bucket_id=NEW.bucket_id)=1;
    UPDATE journal_buckets SET dirty=TRUE WHERE bucket_id=NEW.bucket_id AND (SELECT journal_mode FROM bucket_change_gates WHERE bucket_id=NEW.bucket_id)=2;
END;
CREATE TRIGGER journal_time_remove AFTER DELETE ON object_time_points WHEN (SELECT journal_mode FROM bucket_change_gates WHERE bucket_id=OLD.bucket_id)>0
BEGIN
    UPDATE journal_buckets SET last_seq=last_seq+1 WHERE bucket_id=OLD.bucket_id AND (SELECT journal_mode FROM bucket_change_gates WHERE bucket_id=OLD.bucket_id)=1;
    INSERT INTO journal_entries(encrypt_frames,bucket_id,seq,generation,operation,object_key,stream_id,payload_revision,attributes,history_subject) SELECT st.block_key_id IS NOT NULL,j.bucket_id,j.last_seq,j.generation,'delete',CAST(('~t/' || OLD.seq || '/' || OLD.object_key) AS BLOB),NULL,NULL,NULL,TRUE FROM journal_buckets j JOIN storages st ON st.id=j.storage_id WHERE j.bucket_id=OLD.bucket_id AND (SELECT journal_mode FROM bucket_change_gates WHERE bucket_id=OLD.bucket_id)=1;
    UPDATE journal_buckets SET dirty=TRUE WHERE bucket_id=OLD.bucket_id AND (SELECT journal_mode FROM bucket_change_gates WHERE bucket_id=OLD.bucket_id)=2;
END;

UPDATE mokyu_meta SET schema_version=20261114231000 WHERE id=1;
