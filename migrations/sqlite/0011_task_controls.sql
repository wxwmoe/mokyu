CREATE TABLE maintenance_controls (
    policy TEXT NOT NULL PRIMARY KEY,
    paused INTEGER CHECK (paused IN (0,1)) NOT NULL DEFAULT FALSE,
    last_scheduled_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec')*1000000 AS INTEGER)),
    updated_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec')*1000000 AS INTEGER)),
    CONSTRAINT maintenance_controls_policy CHECK (policy IN ('pack','reuse','reclaim','repack','range','gc','cleanup'))
) STRICT, WITHOUT ROWID;



INSERT OR IGNORE INTO maintenance_controls(policy,paused)
    SELECT 'gc',gc_paused FROM mokyu_meta WHERE id=1;
INSERT OR IGNORE INTO maintenance_controls(policy) VALUES ('pack'),('reuse'),('reclaim'),('repack'),('range'),('cleanup');

UPDATE mokyu_meta SET schema_version=11 WHERE id=1;
