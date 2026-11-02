CREATE TABLE node_receipts (
    node_id BLOB NOT NULL,
    operation_id BLOB NOT NULL,
    action TEXT NOT NULL,
    credential_generation INTEGER NOT NULL,
    request_hash BLOB NOT NULL,
    result TEXT NOT NULL,
    created_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec')*1000000 AS INTEGER)),
    PRIMARY KEY (node_id,operation_id),
    FOREIGN KEY (node_id) REFERENCES nodes(id)
) STRICT, WITHOUT ROWID;
CREATE INDEX node_receipts_expiry ON node_receipts(created_at);

UPDATE mokyu_meta SET schema_version = 20261102213026;
