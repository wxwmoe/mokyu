ALTER TABLE node_write_permits ADD COLUMN batch_id INTEGER NOT NULL DEFAULT 0;
CREATE TABLE node_write_batches (
    node_id BLOB NOT NULL,
    operation_id BLOB NOT NULL,
    batch_id INTEGER NOT NULL,
    request_hash BLOB NOT NULL,
    end_bytes INTEGER NOT NULL,
    plan TEXT NOT NULL,
    ready INTEGER NOT NULL DEFAULT 0,
    commit_hash BLOB,
    PRIMARY KEY (node_id,operation_id,batch_id),
    FOREIGN KEY (node_id,operation_id) REFERENCES node_writes(node_id,operation_id) ON DELETE CASCADE
) STRICT, WITHOUT ROWID;

UPDATE mokyu_meta SET schema_version = 20261102220116;
