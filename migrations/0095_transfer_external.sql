ALTER TABLE transfer_runs ALTER COLUMN storage_id DROP NOT NULL;
ALTER TABLE transfer_runs ALTER COLUMN source_storage_id DROP NOT NULL;
ALTER TABLE transfer_runs ALTER COLUMN profile_revision DROP NOT NULL;
CREATE INDEX transfer_items_source_key ON transfer_items(task_id,source_key);

CREATE TABLE transfer_connections (
    task_id uuid NOT NULL,
    external_id uuid NOT NULL,
    revision BIGINT NOT NULL CHECK (revision>0),
    encrypted bytea NOT NULL,
    PRIMARY KEY (task_id,external_id),
    FOREIGN KEY (task_id) REFERENCES transfer_runs(task_id) ON DELETE CASCADE,
    FOREIGN KEY (external_id) REFERENCES external_buckets(id)
);
CREATE INDEX transfer_connections_external ON transfer_connections(external_id);

CREATE TABLE transfer_external_items (
    item_id uuid PRIMARY KEY,
    state jsonb NOT NULL,
    FOREIGN KEY (item_id) REFERENCES transfer_items(id) ON DELETE CASCADE
);

CREATE TABLE transfer_external_parts (
    item_id uuid NOT NULL,
    part_number INTEGER NOT NULL CHECK (part_number BETWEEN 1 AND 10000),
    size_bytes BIGINT NOT NULL CHECK (size_bytes>=0 AND size_bytes<=5368709120),
    etag TEXT NOT NULL,
    digest bytea NOT NULL CHECK (length(digest)=32),
    PRIMARY KEY (item_id,part_number),
    FOREIGN KEY (item_id) REFERENCES transfer_external_items(item_id) ON DELETE CASCADE
);

UPDATE mokyu_meta SET schema_version=95 WHERE singleton;
