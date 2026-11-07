ALTER TABLE journal_stores ADD COLUMN snapshot_id BLOB;
ALTER TABLE journal_stores ADD COLUMN snapshot_revision INTEGER NOT NULL DEFAULT 0;
ALTER TABLE journal_stores ADD COLUMN snapshot_log_bytes INTEGER NOT NULL DEFAULT 0;
ALTER TABLE journal_stores ADD COLUMN snapshot_auto_due INTEGER NOT NULL DEFAULT FALSE;
ALTER TABLE journal_stores ADD COLUMN snapshot_delete_bytes INTEGER NOT NULL DEFAULT 0;
ALTER TABLE journal_stores ADD COLUMN snapshot_invalid_bytes INTEGER NOT NULL DEFAULT 0;

ALTER TABLE journal_stores ADD COLUMN snapshot_token BLOB;
ALTER TABLE journal_stores ADD COLUMN snapshot_lease_until INTEGER;

ALTER TABLE journal_stores ADD COLUMN snapshot_gc_error TEXT;
ALTER TABLE journal_buckets ADD COLUMN confirmed_log_bytes INTEGER NOT NULL DEFAULT 0;
UPDATE journal_buckets SET confirmed_log_bytes=COALESCE((SELECT SUM(m.encoded_bytes) FROM journal_file_entries m JOIN journal_files f ON f.id=m.file_id WHERE m.bucket_id=journal_buckets.bucket_id AND f.state='published'),0);
UPDATE journal_stores SET snapshot_log_bytes=COALESCE((SELECT SUM(f.byte_count) FROM journal_files f WHERE f.storage_id=journal_stores.storage_id AND f.state='published'),0);

CREATE TABLE journal_snapshots (
    encrypt_frames INTEGER NOT NULL DEFAULT 0 CHECK (encrypt_frames IN (0,1)),
    id BLOB PRIMARY KEY NOT NULL,
    storage_id BLOB NOT NULL REFERENCES journal_stores(storage_id),
    kind TEXT NOT NULL CHECK (kind IN ('append','compact','new')),
    state TEXT NOT NULL CHECK (state IN ('queued','building','prepared','current','retained','failed','gone')),
    parent BLOB,
    revision INTEGER NOT NULL DEFAULT 0,
    sequence_no INTEGER NOT NULL DEFAULT 0,
    digest BLOB,
    epoch BLOB,
    page_limit INTEGER NOT NULL DEFAULT 0,
    covered_bytes INTEGER NOT NULL DEFAULT 0,
    manifest BLOB,
    root_index BLOB,
    manifest_etag TEXT,
    manifest_version TEXT,
    index_etag TEXT,
    checkpoint_etag TEXT,
    checkpoint_version TEXT,
    manifest_size INTEGER NOT NULL DEFAULT 0,
    index_size INTEGER NOT NULL DEFAULT 0,
    planned_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec')*1000000 AS INTEGER)),
    confirmed_at INTEGER,
    gc_after INTEGER,
    lease_token BLOB,
    lease_until INTEGER,
    attempts INTEGER NOT NULL DEFAULT 0,
    retry_after INTEGER,
    error_detail TEXT,
    UNIQUE (storage_id,id)
) STRICT, WITHOUT ROWID;
CREATE INDEX journal_snapshot_queue ON journal_snapshots(storage_id,state,planned_at);

CREATE TABLE journal_snapshot_buckets (
    snapshot_id BLOB NOT NULL REFERENCES journal_snapshots(id),
    bucket_id BLOB NOT NULL,
    bucket_name TEXT NOT NULL,
    generation INTEGER NOT NULL,
    watermark INTEGER NOT NULL,
    included INTEGER NOT NULL,
    mode_revision INTEGER NOT NULL,
    PRIMARY KEY (snapshot_id,bucket_id)
) STRICT, WITHOUT ROWID;

CREATE TABLE journal_snapshot_pages (
    owner BLOB NOT NULL REFERENCES journal_snapshots(id),
    page_no INTEGER NOT NULL,
    family INTEGER NOT NULL,
    bucket_id BLOB NOT NULL,
    description TEXT NOT NULL,
    encrypted BLOB,
    byte_count INTEGER NOT NULL,
    etag TEXT,
    remote_version TEXT,
    gc_state TEXT NOT NULL DEFAULT 'live' CHECK (gc_state IN ('live','pending','gone')),
    PRIMARY KEY (owner,page_no)
) STRICT, WITHOUT ROWID;
CREATE INDEX journal_snapshot_family ON journal_snapshot_pages(owner,family);
CREATE TABLE journal_snapshot_refs (
    snapshot_id BLOB NOT NULL REFERENCES journal_snapshots(id),
    position_no INTEGER NOT NULL,
    owner BLOB NOT NULL,
    page_no INTEGER NOT NULL,
    PRIMARY KEY (snapshot_id,position_no),
    UNIQUE (snapshot_id,owner,page_no),
    FOREIGN KEY (owner,page_no) REFERENCES journal_snapshot_pages(owner,page_no)
) STRICT, WITHOUT ROWID;
CREATE INDEX journal_snapshot_ref_owner ON journal_snapshot_refs(owner,page_no);

CREATE TABLE journal_snapshot_members (
    owner BLOB NOT NULL,
    family INTEGER NOT NULL,
    bucket_id BLOB NOT NULL,
    generation INTEGER NOT NULL,
    seq INTEGER NOT NULL,
    object_key BLOB NOT NULL,
    deleted INTEGER NOT NULL,
    encoded_bytes INTEGER NOT NULL,
    PRIMARY KEY (owner,family,bucket_id,seq),
    FOREIGN KEY (owner,family) REFERENCES journal_snapshot_pages(owner,page_no)
) STRICT, WITHOUT ROWID;
CREATE INDEX journal_snapshot_member_key ON journal_snapshot_members(bucket_id,object_key,seq);

CREATE TABLE journal_snapshot_current (
    storage_id BLOB NOT NULL,
    bucket_id BLOB NOT NULL,
    object_key BLOB NOT NULL,
    generation INTEGER NOT NULL,
    seq INTEGER NOT NULL,
    deleted INTEGER NOT NULL,
    PRIMARY KEY (storage_id,bucket_id,object_key)
) STRICT, WITHOUT ROWID;

CREATE TABLE journal_snapshot_usage (
    storage_id BLOB NOT NULL,
    bucket_id BLOB NOT NULL,
    page_bytes INTEGER NOT NULL,
    invalid_bytes INTEGER NOT NULL,
    PRIMARY KEY (storage_id,bucket_id)
) STRICT, WITHOUT ROWID;

CREATE TABLE journal_snapshot_work (
    snapshot_id BLOB NOT NULL REFERENCES journal_snapshots(id),
    bucket_id BLOB NOT NULL,
    object_key BLOB NOT NULL,
    generation INTEGER NOT NULL,
    seq INTEGER NOT NULL,
    deleted INTEGER NOT NULL,
    output_row INTEGER NOT NULL DEFAULT TRUE,
    PRIMARY KEY (snapshot_id,bucket_id,object_key)
) STRICT, WITHOUT ROWID;

CREATE TABLE journal_snapshot_inputs (
    bucket_id BLOB NOT NULL,
    seq INTEGER NOT NULL,
    page INTEGER NOT NULL,
    encrypted BLOB NOT NULL,
    PRIMARY KEY (bucket_id,seq,page)
) STRICT, WITHOUT ROWID;

CREATE TABLE journal_snapshot_holds (
    bucket_id BLOB NOT NULL,
    seq INTEGER NOT NULL,
    storage_id BLOB NOT NULL,
    unit TEXT NOT NULL,
    address BLOB NOT NULL,
    block_key BLOB,
    PRIMARY KEY (bucket_id,seq,storage_id,unit,address)
) STRICT, WITHOUT ROWID;
CREATE INDEX journal_snapshot_hold_address ON journal_snapshot_holds(address,unit);

CREATE TABLE journal_snapshot_gc (
    storage_id BLOB NOT NULL,
    object_id BLOB NOT NULL,
    page_no INTEGER NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN ('segment','page','manifest','checkpoint')),
    file_path TEXT NOT NULL,
    etag TEXT NOT NULL,
    remote_version TEXT,
    byte_count INTEGER NOT NULL,
    reclaim_at INTEGER NOT NULL,
    PRIMARY KEY (storage_id,object_id,kind,page_no)
) STRICT, WITHOUT ROWID;
CREATE INDEX journal_snapshot_gc_due ON journal_snapshot_gc(storage_id,reclaim_at);

DROP VIEW journal_protected_addresses;
CREATE VIEW journal_protected_addresses AS
SELECT address,unit FROM journal_holds
UNION ALL
SELECT address,unit FROM journal_snapshot_holds
UNION ALL
SELECT m.address,m.unit FROM journal_restore_materials m JOIN journal_recoveries j ON j.id=m.job
WHERE j.state NOT IN ('completed','cancelled');

UPDATE mokyu_meta SET schema_version = 20261107134057;
