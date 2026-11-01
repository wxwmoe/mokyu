ALTER TABLE node_writes ADD COLUMN expected_size bigint CHECK (expected_size >= 0);
ALTER TABLE node_writes ADD COLUMN next_batch bigint NOT NULL DEFAULT 1 CHECK (next_batch > 0);
ALTER TABLE node_writes ADD COLUMN next_offset bigint NOT NULL DEFAULT 0 CHECK (next_offset >= 0);
ALTER TABLE node_writes ADD COLUMN publish_hash bytea CHECK (octet_length(publish_hash)=32);
ALTER TABLE node_writes ADD COLUMN result jsonb CHECK (jsonb_typeof(result)='object' AND octet_length(result::text)<=4096);
ALTER TABLE node_writes DROP CONSTRAINT node_writes_state_check;
ALTER TABLE node_writes ADD CHECK (state IN ('active','aborted','published'));
ALTER TABLE node_writes ADD CHECK ((publish_hash IS NULL)=(result IS NULL));
ALTER TABLE node_writes ADD CHECK ((state='published')=(result IS NOT NULL));

CREATE TABLE node_write_batches (
    node_id uuid NOT NULL,
    operation_id uuid NOT NULL,
    batch_id bigint NOT NULL CHECK (batch_id > 0),
    request_hash bytea NOT NULL CHECK (octet_length(request_hash)=32),
    end_bytes bigint NOT NULL CHECK (end_bytes > 0),
    plan jsonb NOT NULL CHECK (jsonb_typeof(plan)='array' AND octet_length(plan::text)<=262144),
    ready boolean NOT NULL DEFAULT false,
    commit_hash bytea CHECK (octet_length(commit_hash)=32),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (node_id,operation_id,batch_id),
    FOREIGN KEY (node_id,operation_id) REFERENCES node_writes(node_id,operation_id) ON DELETE CASCADE,
    CHECK (commit_hash IS NULL OR ready)
);
CREATE INDEX node_write_batches_pending ON node_write_batches(node_id,operation_id) WHERE commit_hash IS NULL;

CREATE TABLE node_write_permits (
    token uuid PRIMARY KEY REFERENCES storage_permits(token) ON DELETE CASCADE,
    node_id uuid NOT NULL,
    operation_id uuid NOT NULL,
    batch_id bigint NOT NULL,
    FOREIGN KEY (node_id,operation_id,batch_id) REFERENCES node_write_batches(node_id,operation_id,batch_id) ON DELETE CASCADE
);
CREATE INDEX node_write_permits_batch ON node_write_permits(node_id,operation_id,batch_id);

UPDATE mokyu_meta SET schema_version=76 WHERE singleton;
