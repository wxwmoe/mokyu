CREATE TABLE pack_read_shapes (
    stream_id UUID NOT NULL REFERENCES streams(id) ON DELETE CASCADE,
    start_bytes BIGINT NOT NULL,
    end_bytes BIGINT NOT NULL,
    requests BIGINT NOT NULL,
    observed_hour BIGINT NOT NULL,
    saved_gets BIGINT NOT NULL,
    PRIMARY KEY (stream_id, start_bytes, end_bytes, observed_hour),
    CHECK (start_bytes >= 0 AND end_bytes > start_bytes AND requests > 0 AND saved_gets >= 0)
);
CREATE INDEX pack_shapes_expiry ON pack_read_shapes(observed_hour);

CREATE TABLE pack_repack_queue (
    task_id UUID NOT NULL,
    stream_id UUID NOT NULL,
    saved_gets BIGINT NOT NULL,
    PRIMARY KEY (task_id,stream_id),
    FOREIGN KEY (task_id) REFERENCES tasks(id) ON DELETE CASCADE
);
CREATE INDEX pack_repack_priority ON pack_repack_queue(task_id,saved_gets DESC,stream_id);

DELETE FROM pack_changes WHERE reason='reuse';
DELETE FROM pack_maintenance WHERE reason='repack';
DELETE FROM maintenance_controls WHERE policy IN ('reuse','range');

CREATE OR REPLACE FUNCTION enqueue_pack_extents() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    PERFORM id FROM chunks WHERE id IN (SELECT chunk_id FROM changed) ORDER BY id FOR NO KEY UPDATE;
    UPDATE chunks SET reference_changed_at=now() WHERE id IN (SELECT chunk_id FROM changed);
    IF TG_OP='DELETE' THEN
        INSERT INTO pack_changes(pack_id,reason)
        SELECT DISTINCT c.pack_id,'reclaim' FROM changed e JOIN chunks c ON c.id=e.chunk_id
        WHERE c.pack_id IS NOT NULL
        ON CONFLICT(pack_id) DO UPDATE SET generation=pack_changes.generation+1,next_check_at=now(),reason='reclaim';
    END IF;
    RETURN NULL;
END $$;

CREATE OR REPLACE FUNCTION enqueue_pack_stream() RETURNS trigger
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
    END IF;
    RETURN NULL;
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
            coalesce(p.compression_enabled::text, ''),
            coalesce(p.compression_strategy, ''),
            coalesce(p.compression_level::text, ''),
            coalesce(p.compression_sample_level::text, ''),
            coalesce(p.compression_min_savings_percent::text, ''),
            coalesce(p.compression_min_savings_bytes::text, ''),
            coalesce(p.pack_max_size, ''),
            'pack'),
        'sha256');
    RETURN NEW;
END
$$;

UPDATE mokyu_meta SET schema_version = 20261108051001;
