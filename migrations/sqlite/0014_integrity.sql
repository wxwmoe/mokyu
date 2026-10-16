CREATE TABLE integrity_issues (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id BLOB CHECK (task_id IS NULL OR length(task_id)=16) NOT NULL,
    subject TEXT NOT NULL,
    code TEXT NOT NULL,
    chunk_id INTEGER,
    location_id BLOB CHECK (location_id IS NULL OR length(location_id)=16),
    stream_id BLOB CHECK (stream_id IS NULL OR length(stream_id)=16),
    bucket_id BLOB CHECK (bucket_id IS NULL OR length(bucket_id)=16),
    object_key BLOB,
    detail TEXT CHECK (json_valid(detail)) NOT NULL DEFAULT (JSON_OBJECT()),
    created_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec')*1000000 AS INTEGER)),
    CONSTRAINT integrity_issues_task FOREIGN KEY (task_id) REFERENCES tasks(id) ON DELETE CASCADE
) STRICT;

CREATE UNIQUE INDEX integrity_issues_subject ON integrity_issues(task_id,subject,code);
CREATE INDEX integrity_issues_page ON integrity_issues(task_id,id);
CREATE INDEX integrity_issues_code ON integrity_issues(task_id,code,id);

UPDATE mokyu_meta SET schema_version=14 WHERE id=1;
