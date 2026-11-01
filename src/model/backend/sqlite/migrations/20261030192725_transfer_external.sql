ALTER TABLE transfer_runs ADD COLUMN external_storage_id BLOB;
UPDATE transfer_runs SET external_storage_id=storage_id;
ALTER TABLE transfer_runs DROP COLUMN storage_id;
ALTER TABLE transfer_runs RENAME COLUMN external_storage_id TO storage_id;
ALTER TABLE transfer_runs ADD COLUMN external_source_storage_id BLOB;
UPDATE transfer_runs SET external_source_storage_id=source_storage_id;
ALTER TABLE transfer_runs DROP COLUMN source_storage_id;
ALTER TABLE transfer_runs RENAME COLUMN external_source_storage_id TO source_storage_id;
ALTER TABLE transfer_runs ADD COLUMN external_profile_revision BIGINT;
UPDATE transfer_runs SET external_profile_revision=profile_revision;
ALTER TABLE transfer_runs DROP COLUMN profile_revision;
ALTER TABLE transfer_runs RENAME COLUMN external_profile_revision TO profile_revision;
CREATE INDEX transfer_items_source_key ON transfer_items(task_id,source_key);

CREATE TABLE transfer_connections (
    task_id BLOB NOT NULL,
    external_id BLOB NOT NULL,
    revision BIGINT NOT NULL CHECK (revision>0),
    encrypted BLOB NOT NULL,
    PRIMARY KEY (task_id,external_id),
    FOREIGN KEY (task_id) REFERENCES transfer_runs(task_id) ON DELETE CASCADE,
    FOREIGN KEY (external_id) REFERENCES external_buckets(id)
);
CREATE INDEX transfer_connections_external ON transfer_connections(external_id);

CREATE TABLE transfer_external_items (
    item_id INTEGER PRIMARY KEY,
    state TEXT NOT NULL,
    FOREIGN KEY (item_id) REFERENCES transfer_items(id) ON DELETE CASCADE
);

CREATE TABLE transfer_external_parts (
    item_id INTEGER NOT NULL,
    part_number INTEGER NOT NULL CHECK (part_number BETWEEN 1 AND 10000),
    size_bytes BIGINT NOT NULL CHECK (size_bytes>=0 AND size_bytes<=5368709120),
    etag TEXT NOT NULL,
    digest BLOB NOT NULL CHECK (length(digest)=32),
    PRIMARY KEY (item_id,part_number),
    FOREIGN KEY (item_id) REFERENCES transfer_external_items(item_id) ON DELETE CASCADE
);

UPDATE mokyu_meta SET schema_version=49 WHERE id=1;
