CREATE TABLE mokyu_meta (
    id INTEGER PRIMARY KEY CHECK(id=1),
    schema_version INTEGER NOT NULL CHECK(schema_version>0),
    deployment_id BLOB NOT NULL DEFAULT(randomblob(16)) CHECK(length(deployment_id)=16),
    deployment_key_fingerprint BLOB CHECK(length(deployment_key_fingerprint)=32),
    gc_paused INTEGER NOT NULL DEFAULT 0 CHECK(gc_paused IN (0,1)),
    maintenance INTEGER NOT NULL DEFAULT 0 CHECK(maintenance IN (0,1)),
    project_management INTEGER NOT NULL DEFAULT 0 CHECK(project_management IN (0,1)),
    pack_creation_paused INTEGER NOT NULL DEFAULT 0 CHECK(pack_creation_paused IN (0,1))
) STRICT;

CREATE TABLE runtime_settings (
    section TEXT NOT NULL PRIMARY KEY,
    value TEXT NOT NULL CHECK(json_valid(value)),
    revision INTEGER NOT NULL DEFAULT 1 CHECK(revision>0),
    updated_at INTEGER NOT NULL DEFAULT(CAST(unixepoch('subsec')*1000000 AS INTEGER)),
    is_default INTEGER NOT NULL DEFAULT 1 CHECK(is_default IN (0,1))
) STRICT, WITHOUT ROWID;

INSERT INTO mokyu_meta(id,schema_version) VALUES(1,1);
