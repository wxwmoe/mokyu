CREATE TABLE storage_objects (
    namespace_id UUID NOT NULL,
    payload_id UUID NOT NULL,
    path_hash BYTEA NOT NULL CHECK(octet_length(path_hash)=32),
    path TEXT NOT NULL,
    revision BIGINT NOT NULL DEFAULT 0 CHECK(revision>=0),
    head UUID REFERENCES storage_payloads(operation),
    pending UUID REFERENCES storage_payloads(operation),
    modified BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY(namespace_id,payload_id),
    UNIQUE(namespace_id,path_hash)
);

CREATE TABLE storage_object_writes (
    operation UUID NOT NULL PRIMARY KEY REFERENCES storage_payloads(operation),
    owner UUID NOT NULL,
    condition JSONB NOT NULL,
    previous UUID REFERENCES storage_payloads(operation)
);

UPDATE mokyu_meta SET schema_version=20261113154304 WHERE singleton;
