CREATE TABLE nodes (
    id BINARY(16) NOT NULL PRIMARY KEY,
    name VARCHAR(128) NOT NULL UNIQUE,
    role VARCHAR(8) CHARACTER SET ascii COLLATE ascii_bin NOT NULL CHECK (role IN ('control','data','worker','node')),
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    credential_generation BIGINT NOT NULL CHECK (credential_generation > 0),
    credential_encrypted VARBINARY(256),
    run_state JSON,
    report JSON,
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    last_seen_at DATETIME(6),
    control_slot TINYINT GENERATED ALWAYS AS (IF(role='control',1,NULL)) STORED UNIQUE
) ENGINE=InnoDB;

CREATE TABLE node_boots (
    node_id BINARY(16) NOT NULL,
    boot_id BINARY(16) NOT NULL,
    PRIMARY KEY (node_id,boot_id),
    CONSTRAINT node_boots_node_fk FOREIGN KEY (node_id) REFERENCES nodes(id) ON DELETE CASCADE
) ENGINE=InnoDB;

UPDATE mokyu_meta SET schema_version = 20261102183444;
