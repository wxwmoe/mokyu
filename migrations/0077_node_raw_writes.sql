CREATE TABLE node_write_raw (
    node_id uuid NOT NULL,
    operation_id uuid NOT NULL,
    request_hash bytea NOT NULL CHECK (octet_length(request_hash)=32),
    source_id uuid NOT NULL,
    size bigint NOT NULL CHECK (size BETWEEN 0 AND 5368709120),
    digest bytea NOT NULL CHECK (octet_length(digest)=16),
    content_hash bytea CHECK (octet_length(content_hash)=32),
    committed boolean NOT NULL,
    PRIMARY KEY (node_id,operation_id),
    FOREIGN KEY (node_id,operation_id) REFERENCES node_writes(node_id,operation_id) ON DELETE CASCADE
);

ALTER TABLE node_write_permits ALTER COLUMN batch_id DROP NOT NULL;
ALTER TABLE node_write_permits ADD FOREIGN KEY (node_id,operation_id) REFERENCES node_writes(node_id,operation_id) ON DELETE CASCADE;

UPDATE mokyu_meta SET schema_version=77 WHERE singleton;
