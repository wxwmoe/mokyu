ALTER TABLE node_writes ADD COLUMN completion_upload uuid;
ALTER TABLE node_writes ADD CHECK (completion_upload IS NULL OR (upload_id IS NULL AND copy_source IS NULL));
ALTER TABLE node_read_permits DROP CONSTRAINT node_read_permits_pkey;
ALTER TABLE node_read_permits ADD PRIMARY KEY (token,read_id);

CREATE TABLE node_completions (
    upload_id uuid PRIMARY KEY REFERENCES uploads(id) ON DELETE CASCADE,
    manifest_hash text NOT NULL,
    conditions jsonb NOT NULL,
    summary jsonb NOT NULL
);

CREATE TABLE node_completion_parts (
    upload_id uuid NOT NULL REFERENCES node_completions(upload_id) ON DELETE CASCADE,
    ordinal integer NOT NULL CHECK (ordinal BETWEEN 0 AND 9999),
    part_number integer NOT NULL CHECK (part_number BETWEEN 1 AND 10000),
    stream_id uuid NOT NULL,
    size bigint NOT NULL CHECK (size>=0),
    etag text NOT NULL,
    checksums jsonb NOT NULL,
    PRIMARY KEY (upload_id,ordinal),
    UNIQUE (upload_id,part_number)
);

UPDATE mokyu_meta SET schema_version=82 WHERE singleton;
