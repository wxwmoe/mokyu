CREATE TABLE runtime_samples (
    sampled_at INTEGER NOT NULL PRIMARY KEY,
    data TEXT CHECK (json_valid(data)) NOT NULL
) STRICT, WITHOUT ROWID;



UPDATE mokyu_meta SET schema_version=16 WHERE id=1;
