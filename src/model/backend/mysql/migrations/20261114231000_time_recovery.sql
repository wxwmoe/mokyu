ALTER TABLE journal_restore_items ADD COLUMN origin_stream BINARY(16);
CREATE INDEX restore_item_origin ON journal_restore_items(job,source_bucket,origin_stream);

CREATE TABLE bucket_time_windows (
    bucket_id BINARY(16) PRIMARY KEY NOT NULL,
    seconds BIGINT NOT NULL CHECK(seconds>0),
    gaps TEXT NOT NULL,
    floor_us BIGINT NOT NULL,
    last_us BIGINT NOT NULL,
    seq BIGINT NOT NULL,
    FOREIGN KEY(bucket_id) REFERENCES buckets(id) ON DELETE CASCADE
);

CREATE TABLE object_time_points (
    bucket_id BINARY(16) NOT NULL,
    object_key VARBINARY(1024) NOT NULL,
    seq BIGINT NOT NULL,
    seen_us BIGINT NOT NULL,
    stream_id BIGINT,
    PRIMARY KEY(bucket_id,object_key,seq),
    FOREIGN KEY(bucket_id) REFERENCES bucket_time_windows(bucket_id) ON DELETE CASCADE,
    FOREIGN KEY(stream_id) REFERENCES streams(id)
);
CREATE INDEX time_points_clock ON object_time_points(bucket_id,seen_us,seq);
CREATE INDEX time_points_stream ON object_time_points(stream_id);

CREATE TABLE time_retained_streams (
    bucket_id BINARY(16) NOT NULL,
    stream_id BIGINT NOT NULL,
    bytes BIGINT NOT NULL,
    PRIMARY KEY(bucket_id,stream_id),
    FOREIGN KEY(bucket_id) REFERENCES buckets(id),
    FOREIGN KEY(stream_id) REFERENCES streams(id)
);

CREATE TABLE time_restore_plans (
    id BINARY(16) NOT NULL PRIMARY KEY,
    source_bucket BINARY(16) NOT NULL,
    target_bucket BINARY(16) NOT NULL,
    worker_token BINARY(16),
    lease_until_us BIGINT NOT NULL DEFAULT 0,
    request_json TEXT NOT NULL,
    owner_json TEXT NOT NULL,
    digest VARCHAR(120) NOT NULL,
    cut_seq BIGINT NOT NULL,
    at_us BIGINT NOT NULL,
    expires_us BIGINT NOT NULL,
    phase VARCHAR(120) NOT NULL CHECK(phase IN ('preview','running','done','expired')),
    FOREIGN KEY(source_bucket) REFERENCES buckets(id) ON DELETE CASCADE,
    FOREIGN KEY(target_bucket) REFERENCES buckets(id) ON DELETE CASCADE
);
CREATE INDEX time_plan_expiry ON time_restore_plans(phase,expires_us);
CREATE TABLE time_restore_items (
    plan_id BINARY(16) NOT NULL,
    object_key VARBINARY(1024) NOT NULL,
    target_key VARBINARY(1024) NOT NULL,
    source_stream BIGINT,
    target_changed BOOLEAN NOT NULL DEFAULT FALSE,
    expected_stream BIGINT,
    item_state VARCHAR(120) NOT NULL CHECK(item_state IN ('add','replace','remove','skip','missing','done','conflict')),
    PRIMARY KEY(plan_id,object_key),
    FOREIGN KEY(plan_id) REFERENCES time_restore_plans(id) ON DELETE CASCADE,
    FOREIGN KEY(source_stream) REFERENCES streams(id)
);
CREATE INDEX time_item_stream ON time_restore_items(source_stream);
CREATE INDEX time_plan_target ON time_restore_plans(target_bucket,phase,id);
CREATE INDEX time_item_target ON time_restore_items(target_key,plan_id);
CREATE INDEX time_item_next ON time_restore_items(plan_id,item_state,object_key);

CREATE TRIGGER time_capture_insert AFTER INSERT ON objects FOR EACH ROW
BEGIN
    DECLARE gate BINARY(16);
    IF NEW.stream_id IS NOT NULL THEN
        SELECT id INTO gate FROM buckets WHERE id=NEW.bucket_id LOCK IN SHARE MODE;
        UPDATE time_restore_items SET target_changed=TRUE WHERE target_key=NEW.object_key AND item_state IN ('add','replace','remove') AND plan_id IN (SELECT id FROM time_restore_plans WHERE target_bucket=NEW.bucket_id AND phase IN ('preview','running'));
        UPDATE bucket_time_windows SET last_us=GREATEST(last_us+1,TIMESTAMPDIFF(MICROSECOND,'1970-01-01',UTC_TIMESTAMP(6))),seq=seq+1 WHERE bucket_id=NEW.bucket_id;
        IF ROW_COUNT()>0 THEN INSERT INTO object_time_points(bucket_id,object_key,seq,seen_us,stream_id) SELECT bucket_id,NEW.object_key,seq,last_us,NEW.stream_id FROM bucket_time_windows WHERE bucket_id=NEW.bucket_id; END IF;
    END IF;
END;
CREATE TRIGGER time_capture_update AFTER UPDATE ON objects FOR EACH ROW
BEGIN
    DECLARE gate BINARY(16);
    IF NOT (OLD.stream_id <=> NEW.stream_id) THEN
        SELECT id INTO gate FROM buckets WHERE id=NEW.bucket_id LOCK IN SHARE MODE;
        UPDATE time_restore_items SET target_changed=TRUE WHERE target_key=NEW.object_key AND item_state IN ('add','replace','remove') AND plan_id IN (SELECT id FROM time_restore_plans WHERE target_bucket=NEW.bucket_id AND phase IN ('preview','running'));
        UPDATE bucket_time_windows SET last_us=GREATEST(last_us+1,TIMESTAMPDIFF(MICROSECOND,'1970-01-01',UTC_TIMESTAMP(6))),seq=seq+1 WHERE bucket_id=NEW.bucket_id;
        IF ROW_COUNT()>0 THEN INSERT INTO object_time_points(bucket_id,object_key,seq,seen_us,stream_id) SELECT bucket_id,NEW.object_key,seq,last_us,NEW.stream_id FROM bucket_time_windows WHERE bucket_id=NEW.bucket_id; END IF;
    END IF;
END;
CREATE TRIGGER time_old_head AFTER UPDATE ON objects FOR EACH ROW
BEGIN
    IF OLD.stream_id IS NOT NULL AND NOT (OLD.stream_id <=> NEW.stream_id) THEN INSERT IGNORE INTO time_retained_streams(bucket_id,stream_id,bytes) SELECT OLD.bucket_id,OLD.stream_id,s.size FROM streams s WHERE s.id=OLD.stream_id AND OLD.stream_id IS NOT NULL AND (SELECT version_mode FROM buckets WHERE id=OLD.bucket_id)=0 AND EXISTS(SELECT 1 FROM object_time_points t WHERE t.bucket_id=OLD.bucket_id AND t.stream_id=OLD.stream_id); END IF;
END;
CREATE TRIGGER time_old_version AFTER DELETE ON object_versions FOR EACH ROW
BEGIN
    IF OLD.stream_id IS NOT NULL THEN INSERT IGNORE INTO time_retained_streams(bucket_id,stream_id,bytes) SELECT OLD.bucket_id,OLD.stream_id,s.size FROM streams s WHERE s.id=OLD.stream_id AND OLD.stream_id IS NOT NULL AND (SELECT version_mode FROM buckets WHERE id=OLD.bucket_id)>0 AND EXISTS(SELECT 1 FROM object_time_points t WHERE t.bucket_id=OLD.bucket_id AND t.stream_id=OLD.stream_id) AND NOT EXISTS(SELECT 1 FROM object_versions v WHERE v.bucket_id=OLD.bucket_id AND v.stream_id=OLD.stream_id); END IF;
END;
CREATE TRIGGER charge_time_stream_insert AFTER INSERT ON time_retained_streams FOR EACH ROW
BEGIN
    UPDATE quota_scopes SET used_bytes=used_bytes+NEW.bytes WHERE (scope='bucket' AND scope_id=NEW.bucket_id) OR (scope='project' AND scope_id=(SELECT project_id FROM buckets WHERE id=NEW.bucket_id));
END;
CREATE TRIGGER charge_time_stream_delete AFTER DELETE ON time_retained_streams FOR EACH ROW
BEGIN
    UPDATE quota_scopes SET used_bytes=used_bytes-OLD.bytes WHERE (scope='bucket' AND scope_id=OLD.bucket_id) OR (scope='project' AND scope_id=(SELECT project_id FROM buckets WHERE id=OLD.bucket_id));
END;

CREATE TRIGGER journal_time_insert AFTER INSERT ON object_time_points FOR EACH ROW
BEGIN
    DECLARE journal SMALLINT;
        SELECT journal_mode INTO journal FROM bucket_change_gates WHERE bucket_id=NEW.bucket_id LOCK IN SHARE MODE;
        IF journal=1 THEN
        UPDATE journal_buckets SET last_seq=last_seq+1 WHERE bucket_id=NEW.bucket_id;
        INSERT INTO journal_entries(encrypt_frames,bucket_id,seq,generation,operation,object_key,stream_id,payload_revision,attributes,history_subject) SELECT st.block_key_id IS NOT NULL,j.bucket_id,j.last_seq,j.generation,'put',CONCAT('~t/',NEW.seq,'/',NEW.object_key),NEW.stream_id,s.payload_revision,JSON_OBJECT('size',COALESCE(s.size,0),'etag',COALESCE(s.etag,''),'metadata',COALESCE(JSON_EXTRACT(s.metadata,'$'),JSON_OBJECT()),'checksums',COALESCE(JSON_EXTRACT(s.checksums,'$'),JSON_OBJECT()),'payload',COALESCE(s.payload_type,'chunks'),'modified_us',NEW.seen_us,'history',JSON_OBJECT('tick',NEW.seq,'time',NEW.seen_us,'marker',CASE WHEN NEW.stream_id IS NULL THEN 1 ELSE 0 END)),TRUE FROM journal_buckets j JOIN storages st ON st.id=j.storage_id LEFT JOIN streams s ON s.id=NEW.stream_id WHERE j.bucket_id=NEW.bucket_id;
    ELSEIF journal=2 THEN
        UPDATE journal_buckets SET dirty=TRUE WHERE bucket_id=NEW.bucket_id;
    END IF;
END;
CREATE TRIGGER journal_time_remove AFTER DELETE ON object_time_points FOR EACH ROW
BEGIN
    DECLARE journal SMALLINT;
        SELECT journal_mode INTO journal FROM bucket_change_gates WHERE bucket_id=OLD.bucket_id LOCK IN SHARE MODE;
        IF journal=1 THEN
        UPDATE journal_buckets SET last_seq=last_seq+1 WHERE bucket_id=OLD.bucket_id;
        INSERT INTO journal_entries(encrypt_frames,bucket_id,seq,generation,operation,object_key,stream_id,payload_revision,attributes,history_subject) SELECT st.block_key_id IS NOT NULL,j.bucket_id,j.last_seq,j.generation,'delete',CONCAT('~t/',OLD.seq,'/',OLD.object_key),NULL,NULL,NULL,TRUE FROM journal_buckets j JOIN storages st ON st.id=j.storage_id WHERE j.bucket_id=OLD.bucket_id;
    ELSEIF journal=2 THEN
        UPDATE journal_buckets SET dirty=TRUE WHERE bucket_id=OLD.bucket_id;
    END IF;
END;

UPDATE mokyu_meta SET schema_version=20261114231000 WHERE id=1;
