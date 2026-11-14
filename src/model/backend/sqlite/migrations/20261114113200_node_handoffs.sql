CREATE TABLE node_handoffs (
    node_id BLOB PRIMARY KEY,
    operation BLOB NOT NULL,
    revision INTEGER NOT NULL CHECK(revision>0),
    state TEXT NOT NULL CHECK(state IN ('serving','preparing','offline_ready','resuming')),
    FOREIGN KEY (node_id) REFERENCES nodes(id) ON DELETE CASCADE
) STRICT, WITHOUT ROWID;

UPDATE mokyu_meta SET schema_version=20261114113200 WHERE id=1;
