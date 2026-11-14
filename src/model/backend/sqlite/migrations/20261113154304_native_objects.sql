CREATE TABLE storage_objects (
    namespace_id BLOB NOT NULL,
    payload_id BLOB NOT NULL,
    path_hash BLOB NOT NULL CHECK(length(path_hash)=32),
    path TEXT NOT NULL,
    revision INTEGER NOT NULL DEFAULT 0 CHECK(revision>=0),
    head BLOB REFERENCES storage_payloads(operation),
    pending BLOB REFERENCES storage_payloads(operation),
    modified INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY(namespace_id,payload_id),
    UNIQUE(namespace_id,path_hash)
) STRICT, WITHOUT ROWID;

CREATE TABLE storage_object_writes (
    operation BLOB NOT NULL PRIMARY KEY REFERENCES storage_payloads(operation),
    owner BLOB NOT NULL,
    condition TEXT NOT NULL CHECK(json_valid(condition)),
    previous BLOB REFERENCES storage_payloads(operation)
) STRICT, WITHOUT ROWID;

UPDATE mokyu_meta SET schema_version=20261113154304 WHERE id=1;
