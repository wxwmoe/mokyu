CREATE TABLE nodes (
    id BLOB NOT NULL PRIMARY KEY CHECK (length(id)=16),
    name TEXT NOT NULL UNIQUE,
    role TEXT NOT NULL CHECK (role IN ('control','data','worker','node')),
    enabled INTEGER NOT NULL DEFAULT 1 CHECK (enabled IN (0,1)),
    credential_generation INTEGER NOT NULL CHECK (credential_generation>0),
    credential_encrypted BLOB,
    run_state TEXT CHECK (run_state IS NULL OR json_valid(run_state)),
    report TEXT CHECK (report IS NULL OR json_valid(report)),
    created_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec')*1000000 AS INTEGER)),
    last_seen_at INTEGER
) STRICT, WITHOUT ROWID;
CREATE UNIQUE INDEX nodes_active_control ON nodes(role) WHERE role='control';

CREATE TABLE node_boots (
    node_id BLOB NOT NULL REFERENCES nodes(id) ON DELETE CASCADE,
    boot_id BLOB NOT NULL CHECK (length(boot_id)=16),
    PRIMARY KEY (node_id,boot_id)
) STRICT, WITHOUT ROWID;

UPDATE mokyu_meta SET schema_version = 20261102183444;
