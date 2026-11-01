CREATE OR REPLACE FUNCTION public.enqueue_pack_extents() RETURNS trigger
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
UPDATE mokyu_meta SET schema_version=46 WHERE singleton;
