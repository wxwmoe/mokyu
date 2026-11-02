CREATE TABLE node_receipts (
    node_id BINARY(16) NOT NULL,
    operation_id BINARY(16) NOT NULL,
    action VARCHAR(16) NOT NULL,
    credential_generation BIGINT NOT NULL,
    request_hash BINARY(32) NOT NULL,
    result JSON NOT NULL,
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (node_id,operation_id),
    FOREIGN KEY (node_id) REFERENCES nodes(id)
) ENGINE=InnoDB;
CREATE INDEX node_receipts_expiry ON node_receipts(created_at);

UPDATE mokyu_meta SET schema_version = 20261102213026;
