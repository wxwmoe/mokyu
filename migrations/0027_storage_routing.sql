-- Storage bindings are immutable per bucket and consistent across locations and packs.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM buckets WHERE storage_target_id IS NULL)
        OR EXISTS (SELECT 1 FROM chunks WHERE storage_target_id IS NULL)
        OR EXISTS (SELECT 1 FROM packs WHERE storage_target_id IS NULL)
        OR EXISTS (SELECT 1 FROM chunk_locations WHERE storage_target_id IS NULL) THEN
        RAISE EXCEPTION 'all storage rows must be bound before migration 0027';
    END IF;
END
$$;

ALTER TABLE buckets ALTER COLUMN storage_target_id SET NOT NULL;
ALTER TABLE chunks ALTER COLUMN storage_target_id SET NOT NULL;
ALTER TABLE packs ALTER COLUMN storage_target_id SET NOT NULL;
ALTER TABLE chunk_locations ALTER COLUMN storage_target_id SET NOT NULL;

DROP INDEX chunks_dedup;
CREATE UNIQUE INDEX chunks_dedup ON chunks(storage_target_id,hash,raw_size,algorithm,key_id)
    WHERE state IN ('preparing','uploading','ready');

CREATE FUNCTION prevent_bucket_storage_target_change() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.storage_target_id IS DISTINCT FROM OLD.storage_target_id THEN
        RAISE EXCEPTION 'bucket storage target is immutable';
    END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER buckets_storage_target_immutable
    BEFORE UPDATE OF storage_target_id ON buckets
    FOR EACH ROW EXECUTE FUNCTION prevent_bucket_storage_target_change();

CREATE FUNCTION validate_storage_binding() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    expected uuid;
BEGIN
    SELECT storage_target_id INTO expected FROM chunks WHERE id = NEW.chunk_id;
    IF expected IS NULL OR NEW.storage_target_id IS DISTINCT FROM expected THEN
        RAISE EXCEPTION 'storage binding does not match its parent';
    END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER chunk_locations_storage_binding
    BEFORE INSERT OR UPDATE OF chunk_id,storage_target_id ON chunk_locations
    FOR EACH ROW EXECUTE FUNCTION validate_storage_binding();

CREATE FUNCTION validate_pack_member_binding() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    pack_target uuid;
    chunk_target uuid;
BEGIN
    SELECT storage_target_id INTO pack_target FROM packs WHERE id = NEW.pack_id;
    SELECT storage_target_id INTO chunk_target FROM chunks WHERE id = NEW.chunk_id;
    IF pack_target IS NULL OR chunk_target IS NULL OR pack_target IS DISTINCT FROM chunk_target THEN
        RAISE EXCEPTION 'pack member storage binding does not match its pack';
    END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER pack_members_storage_binding
    BEFORE INSERT OR UPDATE OF pack_id,chunk_id ON pack_members
    FOR EACH ROW EXECUTE FUNCTION validate_pack_member_binding();

UPDATE mokyu_meta SET schema_version=27;
