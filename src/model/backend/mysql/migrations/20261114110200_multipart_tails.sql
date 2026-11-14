CREATE TABLE multipart_tails (
    fragment_id BIGINT NOT NULL PRIMARY KEY,
    upload_id BINARY(16) NOT NULL,
    node_id BINARY(16) NOT NULL,
    created_at BIGINT NOT NULL,
    object_key VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL UNIQUE,
    requested BOOLEAN NOT NULL DEFAULT FALSE,
    token BINARY(16),
    retry_at BIGINT NOT NULL DEFAULT 0,
    source_error VARCHAR(32),
    FOREIGN KEY (fragment_id) REFERENCES fragments(id) ON DELETE CASCADE,
    FOREIGN KEY (upload_id) REFERENCES uploads(id) ON DELETE CASCADE,
    FOREIGN KEY (node_id) REFERENCES nodes(id)
) ENGINE=InnoDB;
CREATE INDEX multipart_tails_owner ON multipart_tails(node_id,requested,retry_at,created_at);
CREATE INDEX multipart_tails_upload ON multipart_tails(upload_id);

UPDATE mokyu_meta SET schema_version=20261114110200 WHERE id=1;
