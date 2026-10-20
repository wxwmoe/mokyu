CREATE TABLE key_replacements (
    id BLOB PRIMARY KEY,
    actor TEXT NOT NULL,
    plan TEXT NOT NULL CHECK(json_valid(plan)),
    result TEXT CHECK(result IS NULL OR json_valid(result)),
    expires_at INTEGER NOT NULL
) WITHOUT ROWID;
CREATE INDEX key_replacements_expiry ON key_replacements(expires_at);
UPDATE mokyu_meta SET schema_version=28 WHERE id=1;
