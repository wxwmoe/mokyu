-- The selected compression policy includes its enabled switch.
CREATE OR REPLACE FUNCTION refresh_encoding_fingerprint() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    p storage_profiles;
BEGIN
    SELECT * INTO p FROM storage_profiles WHERE storage_id = NEW.storage_id;
    NEW.encoding_fingerprint := digest(
        concat_ws(':',
            NEW.block_key_id::text,
            NEW.encryption_algorithm,
            NEW.standalone_compressed::text,
            coalesce(p.compression_enabled::text, ''),
            coalesce(p.compression_strategy, ''),
            coalesce(p.compression_level::text, ''),
            coalesce(p.compression_sample_level::text, ''),
            coalesce(p.compression_min_savings_percent::text, ''),
            coalesce(p.compression_min_savings_bytes::text, ''),
            'standalone'),
        'sha256');
    RETURN NEW;
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
            coalesce(p.pack_range_enabled::text, ''),
            coalesce(p.pack_range_min_downloads::text, ''),
            coalesce(p.pack_range_min_savings_percent::text, ''),
            coalesce(p.pack_range_min_savings_bytes, ''),
            'pack'),
        'sha256');
    RETURN NEW;
END
$$;

UPDATE mokyu_meta SET schema_version=39;
