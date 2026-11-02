ALTER TABLE node_write_permits ADD COLUMN batch_id BIGINT NOT NULL DEFAULT 0;
CREATE TABLE node_write_batches (
    node_id BINARY(16) NOT NULL,
    operation_id BINARY(16) NOT NULL,
    batch_id BIGINT NOT NULL,
    request_hash BINARY(32) NOT NULL,
    end_bytes BIGINT NOT NULL,
    plan JSON NOT NULL,
    ready BOOLEAN NOT NULL DEFAULT 0,
    commit_hash BINARY(32),
    PRIMARY KEY (node_id,operation_id,batch_id),
    FOREIGN KEY (node_id,operation_id) REFERENCES node_writes(node_id,operation_id) ON DELETE CASCADE
) ENGINE=InnoDB;

UPDATE mokyu_meta SET schema_version = 20261102220116;
