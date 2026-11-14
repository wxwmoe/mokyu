CREATE TABLE node_handoffs (
    node_id uuid PRIMARY KEY,
    operation uuid NOT NULL,
    revision BIGINT NOT NULL CHECK(revision>0),
    state TEXT NOT NULL CHECK(state IN ('serving','preparing','offline_ready','resuming')),
    FOREIGN KEY (node_id) REFERENCES nodes(id) ON DELETE CASCADE
);

CREATE FUNCTION notify_node_handoff() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    PERFORM pg_notify('mokyu_tasks','node-state');
    PERFORM pg_notify('mokyu_tasks','work:'||NEW.node_id::text);
    RETURN NEW;
END;
$$;
CREATE TRIGGER node_handoff_changed AFTER INSERT OR UPDATE ON node_handoffs
FOR EACH ROW EXECUTE FUNCTION notify_node_handoff();

UPDATE mokyu_meta SET schema_version=20261114113200 WHERE singleton;
