-- Finish task ownership, block-key lifecycle, bootstrap expiry, and shared store permits.
ALTER TABLE streams
    ADD COLUMN owner_task uuid REFERENCES tasks ON DELETE SET NULL,
    ADD COLUMN owner_lease uuid;
CREATE INDEX streams_task_owner ON streams(owner_task) WHERE owner_task IS NOT NULL;

ALTER TABLE stores
    ADD COLUMN current_key_id uuid REFERENCES managed_keys(id),
    ADD CONSTRAINT stores_current_block_key
        CHECK (current_key_id IS NULL OR current_key_id <> '00000000-0000-0000-0000-000000000000');

ALTER TABLE managed_keys DROP CONSTRAINT managed_keys_active_encryption;
DROP INDEX managed_keys_one_active_encryption;
ALTER TABLE managed_keys DROP CONSTRAINT managed_keys_purpose_check;
UPDATE managed_keys SET purpose = 'block' WHERE purpose = 'encryption';
ALTER TABLE managed_keys
    ADD CONSTRAINT managed_keys_purpose_check
        CHECK (purpose IN ('access','signing','block'));
ALTER TABLE managed_keys DROP COLUMN active;
ALTER TABLE managed_keys ALTER COLUMN secret_encrypted DROP NOT NULL;

CREATE FUNCTION validate_store_current_key() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.current_key_id IS NOT NULL THEN
        PERFORM id FROM managed_keys WHERE id = NEW.current_key_id FOR SHARE;
    END IF;
    IF NEW.current_key_id IS NOT NULL AND NOT EXISTS (
        SELECT 1 FROM managed_keys
        WHERE id = NEW.current_key_id AND purpose = 'block' AND revoked_at IS NULL
    ) THEN
        RAISE EXCEPTION 'store current key must be an active block key';
    END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER stores_current_block_key_validate
    BEFORE INSERT OR UPDATE OF current_key_id ON stores
    FOR EACH ROW EXECUTE FUNCTION validate_store_current_key();

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM chunks
        WHERE state IN ('preparing','uploading','ready')
        GROUP BY store_id,hash,raw_size
        HAVING count(*) > 1
    ) THEN
        RAISE EXCEPTION 'logical chunk duplicates exist; reconcile store_id/hash/raw_size before migration 0030';
    END IF;
END
$$;

DROP INDEX chunks_dedup;
CREATE UNIQUE INDEX chunks_dedup
    ON chunks(store_id,hash,raw_size)
    WHERE state IN ('preparing','uploading','ready');

ALTER TABLE tasks DROP CONSTRAINT tasks_kind_check;
ALTER TABLE tasks ADD CONSTRAINT tasks_kind_check
    CHECK (kind IN ('purge','sweep','integrity','pack','unpack','upload','cache_flush','gc','cleanup','storage_migration','rekey'));

ALTER TABLE admin_setup
    ADD COLUMN expires_at timestamptz NOT NULL DEFAULT now() + interval '1 hour';

ALTER TABLE nodes DROP CONSTRAINT IF EXISTS nodes_kind_check;
ALTER TABLE nodes ADD CONSTRAINT nodes_kind_check CHECK (kind IN ('control','worker','data'));

CREATE TABLE store_permits (
    token uuid PRIMARY KEY,
    store_id uuid NOT NULL REFERENCES stores(id) ON DELETE CASCADE,
    direction text NOT NULL CHECK (direction IN ('read', 'upload', 'control')),
    holder uuid NOT NULL,
    acquired_at timestamptz NOT NULL DEFAULT now(),
    expires_at timestamptz NOT NULL
);
CREATE INDEX store_permits_active ON store_permits(store_id, direction, expires_at);

UPDATE mokyu_meta SET schema_version=30;
