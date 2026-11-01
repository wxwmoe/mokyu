CREATE TABLE IF NOT EXISTS usage_summary (
    id VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL PRIMARY KEY,
    requested_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    as_of DATETIME(6),
    data JSON,
    INDEX usage_requested (requested_at,id)
) ENGINE=InnoDB;

INSERT INTO usage_summary(id) VALUES('global')
ON DUPLICATE KEY UPDATE id=usage_summary.id;

CREATE TABLE IF NOT EXISTS usage_buckets (
    scope_id VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    bucket_id BINARY(16) NOT NULL,
    PRIMARY KEY (scope_id,bucket_id),
    CONSTRAINT usage_buckets_scope FOREIGN KEY (scope_id) REFERENCES usage_summary(id) ON DELETE CASCADE
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS usage_history (
    scope_id VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    snapshot_at DATETIME(6) NOT NULL,
    data JSON NOT NULL,
    PRIMARY KEY (scope_id,snapshot_at),
    INDEX usage_history_expiry (snapshot_at),
    CONSTRAINT usage_history_scope FOREIGN KEY (scope_id) REFERENCES usage_summary(id) ON DELETE CASCADE
) ENGINE=InnoDB;

UPDATE mokyu_meta SET schema_version=16 WHERE id=1;
