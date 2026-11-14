CREATE TABLE multipart_tails (
    fragment_id INTEGER NOT NULL PRIMARY KEY,
    upload_id BLOB NOT NULL,
    node_id BLOB NOT NULL,
    created_at INTEGER NOT NULL,
    object_key TEXT NOT NULL UNIQUE,
    requested INTEGER NOT NULL DEFAULT FALSE,
    token BLOB,
    retry_at INTEGER NOT NULL DEFAULT 0,
    source_error TEXT,
    FOREIGN KEY (fragment_id) REFERENCES fragments(id) ON DELETE CASCADE,
    FOREIGN KEY (upload_id) REFERENCES uploads(id) ON DELETE CASCADE,
    FOREIGN KEY (node_id) REFERENCES nodes(id)
) STRICT, WITHOUT ROWID;
CREATE INDEX multipart_tails_owner ON multipart_tails(node_id,requested,retry_at,created_at);
CREATE INDEX multipart_tails_upload ON multipart_tails(upload_id);

UPDATE mokyu_meta SET schema_version=20261114110200 WHERE id=1;
