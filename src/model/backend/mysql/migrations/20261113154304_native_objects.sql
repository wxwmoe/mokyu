CREATE TABLE storage_objects (
    namespace_id BINARY(16) NOT NULL,
    payload_id BINARY(16) NOT NULL,
    path_hash BINARY(32) NOT NULL,
    path TEXT NOT NULL,
    revision BIGINT NOT NULL DEFAULT 0,
    head BINARY(16),
    pending BINARY(16),
    modified BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY(namespace_id,payload_id),
    CONSTRAINT storage_object_path UNIQUE(namespace_id,path_hash),
    CONSTRAINT storage_object_revision CHECK(revision>=0),
    CONSTRAINT storage_object_head FOREIGN KEY(head) REFERENCES storage_payloads(operation),
    CONSTRAINT storage_object_pending FOREIGN KEY(pending) REFERENCES storage_payloads(operation)
);

CREATE TABLE storage_object_writes (
    operation BINARY(16) NOT NULL PRIMARY KEY,
    owner BINARY(16) NOT NULL,
    `condition` JSON NOT NULL,
    previous BINARY(16),
    CONSTRAINT storage_object_write FOREIGN KEY(operation) REFERENCES storage_payloads(operation),
    CONSTRAINT storage_object_previous FOREIGN KEY(previous) REFERENCES storage_payloads(operation)
);

UPDATE mokyu_meta SET schema_version=20261113154304 WHERE id=1;
