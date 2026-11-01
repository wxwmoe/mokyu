CREATE TABLE IF NOT EXISTS quota_scopes (
    scope TEXT NOT NULL,
    scope_id BLOB CHECK (length(scope_id)=16) NOT NULL,
    used_bytes INTEGER NOT NULL DEFAULT 0,
    reserved_bytes INTEGER NOT NULL DEFAULT 0,
    inflight_bytes INTEGER NOT NULL DEFAULT 0,
    object_count INTEGER NOT NULL DEFAULT 0,
    bucket_count INTEGER NOT NULL DEFAULT 0,
    logical_bytes_limit INTEGER,
    inflight_bytes_limit INTEGER,
    bucket_count_limit INTEGER,
    PRIMARY KEY (scope,scope_id),
    CONSTRAINT quota_scope CHECK (scope IN ('project','bucket')),
    CONSTRAINT quota_bytes CHECK (used_bytes>=0 AND reserved_bytes>=0 AND inflight_bytes>=0),
    CONSTRAINT quota_counts CHECK (object_count>=0 AND bucket_count>=0),
    CONSTRAINT quota_limits CHECK ((logical_bytes_limit IS NULL OR logical_bytes_limit>=0) AND (inflight_bytes_limit IS NULL OR inflight_bytes_limit>=0) AND (bucket_count_limit IS NULL OR bucket_count_limit>=0))
) STRICT, WITHOUT ROWID;

INSERT INTO quota_scopes(scope,scope_id) SELECT 'project',id FROM projects;

UPDATE mokyu_meta SET schema_version=5 WHERE id=1;
