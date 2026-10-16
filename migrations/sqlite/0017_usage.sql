CREATE TABLE usage_summary (
    id TEXT NOT NULL PRIMARY KEY,
    requested_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec')*1000000 AS INTEGER)),
    as_of INTEGER,
    data TEXT CHECK (json_valid(data))
) STRICT, WITHOUT ROWID;

CREATE INDEX usage_requested ON usage_summary(requested_at,id);

INSERT INTO usage_summary(id) VALUES('global')
ON CONFLICT(id) DO NOTHING;

CREATE TABLE usage_buckets (
    scope_id TEXT NOT NULL,
    bucket_id BLOB CHECK (bucket_id IS NULL OR length(bucket_id)=16) NOT NULL,
    PRIMARY KEY (scope_id,bucket_id),
    CONSTRAINT usage_buckets_scope FOREIGN KEY (scope_id) REFERENCES usage_summary(id) ON DELETE CASCADE
) STRICT, WITHOUT ROWID;



CREATE TABLE usage_history (
    scope_id TEXT NOT NULL,
    snapshot_at INTEGER NOT NULL,
    data TEXT CHECK (json_valid(data)) NOT NULL,
    PRIMARY KEY (scope_id,snapshot_at),
    CONSTRAINT usage_history_scope FOREIGN KEY (scope_id) REFERENCES usage_summary(id) ON DELETE CASCADE
) STRICT, WITHOUT ROWID;

CREATE INDEX usage_history_expiry ON usage_history(snapshot_at);

UPDATE mokyu_meta SET schema_version=17 WHERE id=1;
