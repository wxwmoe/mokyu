CREATE TABLE node_handoffs (
    node_id BINARY(16) PRIMARY KEY,
    operation BINARY(16) NOT NULL,
    revision BIGINT NOT NULL CHECK(revision>0),
    state VARCHAR(16) NOT NULL CHECK(state IN ('serving','preparing','offline_ready','resuming')),
    FOREIGN KEY (node_id) REFERENCES nodes(id) ON DELETE CASCADE
) ENGINE=InnoDB;

UPDATE mokyu_meta SET schema_version=20261114113200 WHERE id=1;
