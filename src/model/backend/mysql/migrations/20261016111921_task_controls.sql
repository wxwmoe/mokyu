CREATE TABLE IF NOT EXISTS maintenance_controls (
    policy VARCHAR(7) CHARACTER SET ascii COLLATE ascii_bin NOT NULL PRIMARY KEY,
    paused BOOLEAN NOT NULL DEFAULT FALSE,
    last_scheduled_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    CONSTRAINT maintenance_controls_policy CHECK (policy IN ('pack','reuse','reclaim','repack','range','gc','cleanup'))
) ENGINE=InnoDB;

INSERT IGNORE INTO maintenance_controls(policy,paused)
    SELECT 'gc',gc_paused FROM mokyu_meta WHERE id=1;
INSERT IGNORE INTO maintenance_controls(policy) VALUES ('pack'),('reuse'),('reclaim'),('repack'),('range'),('cleanup');

UPDATE mokyu_meta SET schema_version=10 WHERE id=1;
