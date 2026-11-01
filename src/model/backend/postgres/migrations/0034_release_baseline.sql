-- Check under the migrator lock, before changing any legacy data or history.
DO $$
DECLARE
    relation_name text;
    predicate text;
    used boolean;
BEGIN
    FOR relation_name IN
        SELECT tablename FROM pg_tables
        WHERE schemaname = 'public'
          AND tablename NOT IN (
              '_sqlx_migrations', 'mokyu_meta', 'key_fingerprints',
              'runtime_settings', 'maintenance_controls'
          )
        ORDER BY tablename
    LOOP
        -- Keep writers out until the baseline and its history are committed.
        EXECUTE format('LOCK TABLE public.%I IN SHARE MODE', relation_name);
        predicate := CASE relation_name
            WHEN 'projects' THEN
                'id <> ''00000000-0000-0000-0000-000000000001'''
            WHEN 'quota_scopes' THEN
                'scope <> ''project'' OR scope_id <> ''00000000-0000-0000-0000-000000000001''
                 OR used_bytes <> 0 OR reserved_bytes <> 0 OR inflight_bytes <> 0
                 OR object_count <> 0 OR bucket_count <> 0 OR cached_bytes <> 0'
            WHEN 'catalog_build' THEN
                'scanned <> 0 OR cursor_bucket IS NOT NULL OR cursor_key IS NOT NULL'
            WHEN 'usage_summary' THEN
                'id <> ''global'' OR bucket_ids IS NOT NULL OR as_of IS NOT NULL OR data IS NOT NULL'
            ELSE 'true'
        END;
        EXECUTE format('SELECT EXISTS(SELECT 1 FROM public.%I WHERE %s)', relation_name, predicate)
            INTO used;
        IF used THEN
            RAISE EXCEPTION '0.0.4 data cannot be upgraded in place (table %); create a fresh 0.0.5 deployment and migrate logical objects through S3', relation_name;
        END IF;
    END LOOP;
END $$;

UPDATE mokyu_meta SET schema_version = 34;
