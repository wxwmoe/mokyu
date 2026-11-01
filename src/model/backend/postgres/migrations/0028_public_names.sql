-- Public storage and administrator names are finalized before runtime settings.
ALTER TABLE mokyu_meta RENAME COLUMN backend_identity TO storage_identity;
ALTER TABLE mokyu_meta RENAME COLUMN backend_initialized TO storage_initialized;
ALTER TABLE manage_setup RENAME TO admin_setup;
ALTER TABLE storage_targets RENAME TO stores;

ALTER TABLE buckets RENAME COLUMN storage_target_id TO store_id;
ALTER TABLE chunks RENAME COLUMN storage_target_id TO store_id;
ALTER TABLE packs RENAME COLUMN storage_target_id TO store_id;
ALTER TABLE chunk_locations RENAME COLUMN storage_target_id TO store_id;

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'storage_targets_pkey') THEN
        ALTER TABLE stores RENAME CONSTRAINT storage_targets_pkey TO stores_pkey;
    END IF;
    IF EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'buckets_storage_target_id_fkey') THEN
        ALTER TABLE buckets RENAME CONSTRAINT buckets_storage_target_id_fkey TO buckets_store_id_fkey;
    END IF;
    IF EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'chunks_storage_target_id_fkey') THEN
        ALTER TABLE chunks RENAME CONSTRAINT chunks_storage_target_id_fkey TO chunks_store_id_fkey;
    END IF;
    IF EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'packs_storage_target_id_fkey') THEN
        ALTER TABLE packs RENAME CONSTRAINT packs_storage_target_id_fkey TO packs_store_id_fkey;
    END IF;
    IF EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'chunk_locations_storage_target_id_fkey') THEN
        ALTER TABLE chunk_locations RENAME CONSTRAINT chunk_locations_storage_target_id_fkey TO chunk_locations_store_id_fkey;
    END IF;
END
$$;

DO $$
BEGIN
    IF to_regclass('public.buckets_storage_target') IS NOT NULL THEN
        ALTER INDEX buckets_storage_target RENAME TO buckets_store;
    END IF;
    IF to_regclass('public.chunks_storage_target') IS NOT NULL THEN
        ALTER INDEX chunks_storage_target RENAME TO chunks_store;
    END IF;
    IF to_regclass('public.packs_storage_target') IS NOT NULL THEN
        ALTER INDEX packs_storage_target RENAME TO packs_store;
    END IF;
    IF to_regclass('public.chunk_locations_storage_target') IS NOT NULL THEN
        ALTER INDEX chunk_locations_storage_target RENAME TO chunk_locations_store;
    END IF;
END
$$;

CREATE OR REPLACE FUNCTION prevent_bucket_storage_target_change() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.store_id IS DISTINCT FROM OLD.store_id THEN
        RAISE EXCEPTION 'bucket store is immutable';
    END IF;
    RETURN NEW;
END
$$;
ALTER FUNCTION prevent_bucket_storage_target_change() RENAME TO prevent_bucket_store_change;
ALTER TRIGGER buckets_storage_target_immutable ON buckets RENAME TO buckets_store_immutable;

CREATE OR REPLACE FUNCTION validate_storage_binding() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    expected uuid;
BEGIN
    SELECT store_id INTO expected FROM chunks WHERE id = NEW.chunk_id;
    IF expected IS NULL OR NEW.store_id IS DISTINCT FROM expected THEN
        RAISE EXCEPTION 'store binding does not match its parent';
    END IF;
    RETURN NEW;
END
$$;

CREATE OR REPLACE FUNCTION validate_pack_member_binding() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    pack_store uuid;
    chunk_store uuid;
BEGIN
    SELECT store_id INTO pack_store FROM packs WHERE id = NEW.pack_id;
    SELECT store_id INTO chunk_store FROM chunks WHERE id = NEW.chunk_id;
    IF pack_store IS NULL OR chunk_store IS NULL OR pack_store IS DISTINCT FROM chunk_store THEN
        RAISE EXCEPTION 'pack member store binding does not match its pack';
    END IF;
    RETURN NEW;
END
$$;

UPDATE mokyu_meta SET schema_version=28;
