CREATE TABLE storage_objects_new (
    namespace_id BLOB NOT NULL,
    payload_id BLOB NOT NULL,
    path_hash BLOB NOT NULL CHECK(length(path_hash)=32),
    path TEXT NOT NULL CHECK(length(CAST(path AS BLOB))<=1024),
    revision INTEGER NOT NULL DEFAULT 0 CHECK(revision>=0),
    head BLOB REFERENCES storage_payloads(operation),
    pending BLOB REFERENCES storage_payloads(operation),
    modified INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY(namespace_id,payload_id),
    UNIQUE(namespace_id,path_hash)
) STRICT, WITHOUT ROWID;
INSERT INTO storage_objects_new SELECT * FROM storage_objects;
DROP TABLE storage_objects;
ALTER TABLE storage_objects_new RENAME TO storage_objects;
CREATE INDEX storage_objects_listing ON storage_objects(namespace_id,path) WHERE head IS NOT NULL;
UPDATE mokyu_meta SET schema_version=20261113211629 WHERE id=1;
