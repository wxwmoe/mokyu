CREATE TABLE multipart_tails (
    fragment_id uuid NOT NULL PRIMARY KEY,
    upload_id uuid NOT NULL,
    node_id uuid NOT NULL,
    created_at BIGINT NOT NULL,
    object_key TEXT NOT NULL UNIQUE,
    requested BOOLEAN NOT NULL DEFAULT FALSE,
    token uuid,
    retry_at BIGINT NOT NULL DEFAULT 0,
    source_error TEXT,
    FOREIGN KEY (fragment_id) REFERENCES fragments(id) ON DELETE CASCADE,
    FOREIGN KEY (upload_id) REFERENCES uploads(id) ON DELETE CASCADE,
    FOREIGN KEY (node_id) REFERENCES nodes(id)
);
CREATE INDEX multipart_tails_owner ON multipart_tails(node_id,requested,retry_at,created_at);
CREATE INDEX multipart_tails_upload ON multipart_tails(upload_id);

CREATE FUNCTION notify_multipart_tail() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    PERFORM pg_notify('mokyu_tasks','work:'||NEW.node_id::text);
    RETURN NEW;
END;
$$;
CREATE TRIGGER multipart_tail_requested AFTER UPDATE OF requested ON multipart_tails
FOR EACH ROW WHEN (NEW.requested AND NOT OLD.requested) EXECUTE FUNCTION notify_multipart_tail();

UPDATE mokyu_meta SET schema_version=20261114110200 WHERE singleton;
