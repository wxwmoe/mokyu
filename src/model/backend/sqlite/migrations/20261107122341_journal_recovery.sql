ALTER TABLE managed_keys ADD COLUMN recovery_read_only INTEGER NOT NULL DEFAULT FALSE;

CREATE TABLE journal_recoveries (
    id BLOB NOT NULL PRIMARY KEY,
    state TEXT NOT NULL CHECK (state IN ('locked','preview','queued','verifying','importing','claiming','completed','failed','cancelled')),
    source BLOB NOT NULL,
    selection TEXT,
    progress TEXT NOT NULL,
    error_detail TEXT,
    revision INTEGER NOT NULL DEFAULT 1 CHECK (revision>0),
    source_storage BLOB NOT NULL,
    target_storage BLOB,
    lease_token BLOB,
    lease_until INTEGER,
    created_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec')*1000000 AS INTEGER)),
    updated_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec')*1000000 AS INTEGER))
) STRICT, WITHOUT ROWID;
CREATE INDEX journal_recoveries_work ON journal_recoveries(state,created_at);
CREATE INDEX journal_recoveries_source ON journal_recoveries(source_storage,state);

CREATE TABLE journal_restore_buckets (
    job BLOB NOT NULL,
    source_bucket BLOB NOT NULL,
    target_bucket BLOB NOT NULL UNIQUE,
    target_name TEXT NOT NULL,
    generation INTEGER NOT NULL,
    watermark INTEGER NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('planned','building','published')),
    next_key BLOB,
    restored_objects INTEGER NOT NULL DEFAULT 0,
    restored_bytes INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (job,source_bucket),
    FOREIGN KEY (job) REFERENCES journal_recoveries(id)
) STRICT, WITHOUT ROWID;

CREATE TABLE journal_restore_materials (
    job BLOB NOT NULL,
    identity_hash BLOB NOT NULL,
    unit TEXT NOT NULL,
    address BLOB NOT NULL,
    native_id INTEGER NOT NULL,
    content_hash BLOB NOT NULL,
    PRIMARY KEY (job,identity_hash),
    FOREIGN KEY (job) REFERENCES journal_recoveries(id)
) STRICT, WITHOUT ROWID;

CREATE INDEX journal_restore_addresses ON journal_restore_materials(address);

CREATE TABLE journal_restore_items (
    job BLOB NOT NULL,
    source_bucket BLOB NOT NULL,
    object_key BLOB NOT NULL,
    size INTEGER NOT NULL,
    etag TEXT NOT NULL,
    metadata TEXT NOT NULL,
    checksums TEXT NOT NULL,
    payload TEXT NOT NULL,
    modified_us INTEGER NOT NULL,
    content_type TEXT NOT NULL,
    category TEXT NOT NULL,
    PRIMARY KEY (job,source_bucket,object_key),
    FOREIGN KEY (job,source_bucket) REFERENCES journal_restore_buckets(job,source_bucket)
) STRICT, WITHOUT ROWID;

CREATE TABLE journal_restore_ranges (
    job BLOB NOT NULL,
    source_bucket BLOB NOT NULL,
    object_key BLOB NOT NULL,
    offset_bytes INTEGER NOT NULL,
    length INTEGER NOT NULL,
    source_offset INTEGER NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN ('raw','chunk','fragment')),
    native_id INTEGER NOT NULL,
    PRIMARY KEY (job,source_bucket,object_key,offset_bytes),
    FOREIGN KEY (job,source_bucket,object_key) REFERENCES journal_restore_items(job,source_bucket,object_key) ON DELETE CASCADE
) STRICT, WITHOUT ROWID;

CREATE TABLE journal_restore_scopes (
    storage_id BLOB NOT NULL,
    job BLOB NOT NULL,
    protect_unknown INTEGER NOT NULL DEFAULT TRUE,
    PRIMARY KEY (storage_id,job),
    FOREIGN KEY (job) REFERENCES journal_recoveries(id)
) STRICT, WITHOUT ROWID;

CREATE TABLE journal_restore_orphans (
    job BLOB NOT NULL,
    address BLOB NOT NULL,
    file_path TEXT NOT NULL,
    byte_count INTEGER NOT NULL CHECK (byte_count>=0),
    PRIMARY KEY (job,address),
    FOREIGN KEY (job) REFERENCES journal_recoveries(id)
) STRICT, WITHOUT ROWID;

CREATE VIEW journal_protected_addresses AS
SELECT address,unit FROM journal_holds
UNION ALL
SELECT m.address,m.unit FROM journal_restore_materials m JOIN journal_recoveries j ON j.id=m.job
WHERE j.state NOT IN ('completed','cancelled');

UPDATE mokyu_meta SET schema_version = 20261107122341;
