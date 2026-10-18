CREATE TABLE audit_events_next (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at INTEGER NOT NULL DEFAULT(CAST(unixepoch('subsec')*1000000 AS INTEGER)),
    finished_at INTEGER,
    actor_id BLOB,
    actor_label TEXT NOT NULL,
    token_id BLOB,
    source TEXT NOT NULL CHECK(source IN ('web','token','cli','s3')),
    action TEXT NOT NULL,
    target TEXT NOT NULL DEFAULT '',
    project_id BLOB,
    bucket_id BLOB,
    outcome TEXT NOT NULL DEFAULT 'unknown' CHECK(outcome IN ('unknown','succeeded','failed','partial')),
    request_id TEXT,
    http_status INTEGER,
    detail TEXT NOT NULL DEFAULT '{}' CHECK(json_valid(detail))
) STRICT;
INSERT INTO audit_events_next SELECT * FROM audit_events;
INSERT INTO sqlite_sequence(name,seq)
SELECT 'audit_events_next',seq FROM sqlite_sequence WHERE name='audit_events'
AND NOT EXISTS(SELECT 1 FROM sqlite_sequence WHERE name='audit_events_next');
UPDATE sqlite_sequence SET seq=MAX(seq,COALESCE((SELECT seq FROM sqlite_sequence WHERE name='audit_events'),0))
WHERE name='audit_events_next';
DROP TABLE audit_events;
ALTER TABLE audit_events_next RENAME TO audit_events;
CREATE INDEX audit_actor ON audit_events(actor_id,id);
CREATE INDEX audit_created ON audit_events(created_at,id);
CREATE INDEX audit_security ON audit_events(actor_id,action,outcome,created_at);
UPDATE mokyu_meta SET schema_version=21 WHERE id=1;
