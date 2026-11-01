ALTER TABLE node_writes ADD COLUMN upload_id uuid;
ALTER TABLE node_writes ADD COLUMN part_number integer CHECK (part_number BETWEEN 1 AND 10000);
ALTER TABLE node_writes ADD COLUMN checksum_algorithm text;
ALTER TABLE node_writes ADD CHECK ((upload_id IS NULL)=(part_number IS NULL));
ALTER TABLE node_writes ADD CHECK (upload_id IS NULL OR copy_source IS NULL);

CREATE TABLE node_write_tails (
    node_id uuid NOT NULL,
    operation_id uuid NOT NULL,
    request_hash bytea NOT NULL CHECK (octet_length(request_hash)=32),
    fragment_id uuid NOT NULL,
    storage_id uuid NOT NULL,
    object_key text NOT NULL,
    offset_bytes bigint NOT NULL CHECK (offset_bytes>=0),
    size integer NOT NULL CHECK (size>0),
    committed boolean NOT NULL DEFAULT false,
    PRIMARY KEY (node_id,operation_id),
    FOREIGN KEY (node_id,operation_id) REFERENCES node_writes(node_id,operation_id) ON DELETE CASCADE
);

UPDATE mokyu_meta SET schema_version=81 WHERE singleton;
