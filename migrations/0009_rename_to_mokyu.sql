DO $$
DECLARE
    constraint_name text;
BEGIN
    IF EXISTS (SELECT 1 FROM gateway_meta WHERE backend_initialized)
        OR EXISTS (SELECT 1 FROM key_fingerprints)
        OR EXISTS (SELECT 1 FROM buckets)
        OR EXISTS (SELECT 1 FROM credentials)
        OR EXISTS (SELECT 1 FROM streams)
        OR EXISTS (SELECT 1 FROM chunks)
        OR EXISTS (SELECT 1 FROM packs)
        OR EXISTS (SELECT 1 FROM web_users)
        OR EXISTS (SELECT 1 FROM tasks)
    THEN
        RAISE EXCEPTION 'legacy storage format cannot be upgraded to Mokyu; use a fresh database, data directory and backend namespace';
    END IF;

    ALTER TABLE gateway_meta RENAME TO mokyu_meta;
    FOR constraint_name IN
        SELECT conname FROM pg_constraint
        WHERE conrelid='mokyu_meta'::regclass AND starts_with(conname,'gateway_meta_')
    LOOP
        EXECUTE format('ALTER TABLE mokyu_meta RENAME CONSTRAINT %I TO %I',
            constraint_name, replace(constraint_name,'gateway_meta_','mokyu_meta_'));
    END LOOP;
END;
$$;

UPDATE mokyu_meta SET schema_version=9;
