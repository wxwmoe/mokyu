\set ON_ERROR_STOP on

-- Run with psql autocommit, after enabling maintenance and stopping the gateway.
-- Only restore GC timestamps; backend deletion still uses the configured grace.
SET lock_timeout = '5s';
DO $$
DECLARE
    cursor_id bigint := 0;
    upper_id bigint;
    batch_end bigint;
    changed bigint;
    marked bigint := 0;
BEGIN
    IF NOT pg_try_advisory_lock(734922709851001) THEN
        RAISE EXCEPTION 'stop the gateway before marking unreferenced chunks';
    END IF;
    IF (SELECT maintenance FROM mokyu_meta WHERE singleton) IS NOT TRUE THEN
        RAISE EXCEPTION 'enable maintenance before stopping the gateway';
    END IF;
    SELECT max(id) INTO upper_id FROM chunks;
    LOOP
        SELECT max(id) INTO batch_end FROM (
            SELECT id FROM chunks WHERE id > cursor_id AND id <= upper_id
            ORDER BY id LIMIT 1000
        ) page;
        EXIT WHEN batch_end IS NULL;
        UPDATE chunks c SET unreferenced_at = clock_timestamp()
        WHERE id > cursor_id AND id <= batch_end
            AND state = 'ready' AND owner_stream IS NULL AND unreferenced_at IS NULL
            AND NOT EXISTS (SELECT 1 FROM extents WHERE chunk_id = c.id);
        GET DIAGNOSTICS changed = ROW_COUNT;
        marked := marked + changed;
        cursor_id := batch_end;
        COMMIT;
    END LOOP;
    RAISE NOTICE 'marked % unreferenced chunks through id %', marked, upper_id;
END
$$;
SELECT pg_advisory_unlock(734922709851001);
RESET lock_timeout;
