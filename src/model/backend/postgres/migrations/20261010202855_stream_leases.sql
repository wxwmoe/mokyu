-- Shared stream leases protect in-flight writes and reads across cluster nodes.
CREATE TABLE stream_leases (
    id uuid PRIMARY KEY,
    stream_id uuid NOT NULL REFERENCES streams ON DELETE CASCADE,
    lease_type text NOT NULL CHECK (lease_type IN ('read', 'write')),
    expires_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX stream_write_lease
    ON stream_leases(stream_id)
    WHERE lease_type = 'write';
CREATE INDEX stream_lease_expiry ON stream_leases(expires_at);
CREATE INDEX stream_lease_stream ON stream_leases(stream_id, expires_at);

CREATE FUNCTION clear_stream_write_lease() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.state <> 'writing' THEN
        DELETE FROM stream_leases
        WHERE stream_id = NEW.id AND lease_type = 'write';
    END IF;
    RETURN NEW;
END
$$;
CREATE TRIGGER streams_clear_write_lease
    AFTER UPDATE OF state ON streams
    FOR EACH ROW EXECUTE FUNCTION clear_stream_write_lease();

UPDATE mokyu_meta SET schema_version = 32;
