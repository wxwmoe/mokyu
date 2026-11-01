ALTER TABLE raw_uploads ADD COLUMN node_id uuid REFERENCES nodes(id);
ALTER TABLE raw_uploads ADD COLUMN lease_expires_at timestamptz;
CREATE INDEX raw_uploads_node ON raw_uploads(node_id) WHERE node_id IS NOT NULL;

ALTER TABLE pending_uploads ADD COLUMN node_id uuid REFERENCES nodes(id);
CREATE INDEX pending_uploads_node ON pending_uploads(node_id) WHERE node_id IS NOT NULL;

UPDATE mokyu_meta SET schema_version=56;
