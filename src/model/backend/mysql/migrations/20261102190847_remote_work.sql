ALTER TABLE mokyu_meta ADD COLUMN remote_work BOOLEAN NOT NULL DEFAULT FALSE;
CREATE TABLE node_work (
    node_id BINARY(16) NOT NULL,
    operation_id BINARY(16) NOT NULL,
    boot_id BINARY(16) NOT NULL,
    fence BINARY(16) NOT NULL,
    request_hash BINARY(32) NOT NULL,
    task_id BINARY(16),
    token BINARY(16) NOT NULL,
    plan JSON,
    materials_encrypted MEDIUMBLOB,
    result_hash BINARY(32),
    expires_at DATETIME(6) NOT NULL,
    PRIMARY KEY (node_id,operation_id),
    KEY node_work_expiry (expires_at),
    KEY node_work_task (task_id),
    CONSTRAINT node_work_node_fk FOREIGN KEY (node_id) REFERENCES nodes(id),
    CONSTRAINT node_work_task_fk FOREIGN KEY (task_id) REFERENCES tasks(id) ON DELETE SET NULL
) ENGINE=InnoDB;

UPDATE mokyu_meta SET schema_version = 20261102190847;
