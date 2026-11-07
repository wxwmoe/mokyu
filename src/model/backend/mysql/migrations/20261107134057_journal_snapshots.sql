ALTER TABLE journal_stores ADD COLUMN snapshot_id BINARY(16);
ALTER TABLE journal_stores ADD COLUMN snapshot_revision BIGINT NOT NULL DEFAULT 0;
ALTER TABLE journal_stores ADD COLUMN snapshot_log_bytes BIGINT NOT NULL DEFAULT 0;
ALTER TABLE journal_stores ADD COLUMN snapshot_auto_due BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE journal_stores ADD COLUMN snapshot_delete_bytes BIGINT NOT NULL DEFAULT 0;
ALTER TABLE journal_stores ADD COLUMN snapshot_invalid_bytes BIGINT NOT NULL DEFAULT 0;

ALTER TABLE journal_stores ADD COLUMN snapshot_token BINARY(16);
ALTER TABLE journal_stores ADD COLUMN snapshot_lease_until DATETIME(6);

ALTER TABLE journal_stores ADD COLUMN snapshot_gc_error TEXT;
ALTER TABLE journal_buckets ADD COLUMN confirmed_log_bytes BIGINT NOT NULL DEFAULT 0;
UPDATE journal_buckets SET confirmed_log_bytes=COALESCE((SELECT SUM(m.encoded_bytes) FROM journal_file_entries m JOIN journal_files f ON f.id=m.file_id WHERE m.bucket_id=journal_buckets.bucket_id AND f.state='published'),0);
UPDATE journal_stores SET snapshot_log_bytes=COALESCE((SELECT SUM(f.byte_count) FROM journal_files f WHERE f.storage_id=journal_stores.storage_id AND f.state='published'),0);

CREATE TABLE journal_snapshots (
    encrypt_frames BOOLEAN NOT NULL DEFAULT FALSE,
    id BINARY(16) PRIMARY KEY NOT NULL,
    storage_id BINARY(16) NOT NULL,
    kind VARCHAR(32) NOT NULL CHECK (kind IN ('append','compact','new')),
    state VARCHAR(32) NOT NULL CHECK (state IN ('queued','building','prepared','current','retained','failed','gone')),
    parent BINARY(16),
    revision BIGINT NOT NULL DEFAULT 0,
    sequence_no BIGINT NOT NULL DEFAULT 0,
    digest BINARY(32),
    epoch BINARY(16),
    page_limit BIGINT NOT NULL DEFAULT 0,
    covered_bytes BIGINT NOT NULL DEFAULT 0,
    manifest LONGBLOB,
    root_index LONGBLOB,
    manifest_etag VARCHAR(255),
    manifest_version VARCHAR(255),
    index_etag VARCHAR(255),
    checkpoint_etag VARCHAR(255),
    checkpoint_version VARCHAR(255),
    manifest_size BIGINT NOT NULL DEFAULT 0,
    index_size BIGINT NOT NULL DEFAULT 0,
    planned_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    confirmed_at DATETIME(6),
    gc_after DATETIME(6),
    lease_token BINARY(16),
    lease_until DATETIME(6),
    attempts BIGINT NOT NULL DEFAULT 0,
    retry_after DATETIME(6),
    error_detail TEXT,
    UNIQUE (storage_id,id),
    FOREIGN KEY (storage_id) REFERENCES journal_stores(storage_id)
);
CREATE INDEX journal_snapshot_queue ON journal_snapshots(storage_id,state,planned_at);

CREATE TABLE journal_snapshot_buckets (
    snapshot_id BINARY(16) NOT NULL,
    bucket_id BINARY(16) NOT NULL,
    bucket_name VARCHAR(255) NOT NULL,
    generation BIGINT NOT NULL,
    watermark BIGINT NOT NULL,
    included BOOLEAN NOT NULL,
    mode_revision BIGINT NOT NULL,
    PRIMARY KEY (snapshot_id,bucket_id),
    FOREIGN KEY (snapshot_id) REFERENCES journal_snapshots(id)
);

CREATE TABLE journal_snapshot_pages (
    owner BINARY(16) NOT NULL,
    page_no BIGINT NOT NULL,
    family BIGINT NOT NULL,
    bucket_id BINARY(16) NOT NULL,
    description JSON NOT NULL,
    encrypted LONGBLOB,
    byte_count BIGINT NOT NULL,
    etag VARCHAR(255),
    remote_version VARCHAR(255),
    gc_state VARCHAR(32) NOT NULL DEFAULT 'live' CHECK (gc_state IN ('live','pending','gone')),
    PRIMARY KEY (owner,page_no),
    FOREIGN KEY (owner) REFERENCES journal_snapshots(id)
);
CREATE INDEX journal_snapshot_family ON journal_snapshot_pages(owner,family);
CREATE TABLE journal_snapshot_refs (
    snapshot_id BINARY(16) NOT NULL,
    position_no BIGINT NOT NULL,
    owner BINARY(16) NOT NULL,
    page_no BIGINT NOT NULL,
    PRIMARY KEY (snapshot_id,position_no),
    UNIQUE (snapshot_id,owner,page_no),
    FOREIGN KEY (owner,page_no) REFERENCES journal_snapshot_pages(owner,page_no),
    FOREIGN KEY (snapshot_id) REFERENCES journal_snapshots(id)
);
CREATE INDEX journal_snapshot_ref_owner ON journal_snapshot_refs(owner,page_no);

CREATE TABLE journal_snapshot_members (
    owner BINARY(16) NOT NULL,
    family BIGINT NOT NULL,
    bucket_id BINARY(16) NOT NULL,
    generation BIGINT NOT NULL,
    seq BIGINT NOT NULL,
    object_key VARBINARY(1024) NOT NULL,
    deleted BOOLEAN NOT NULL,
    encoded_bytes BIGINT NOT NULL,
    PRIMARY KEY (owner,family,bucket_id,seq),
    FOREIGN KEY (owner,family) REFERENCES journal_snapshot_pages(owner,page_no)
);
CREATE INDEX journal_snapshot_member_key ON journal_snapshot_members(bucket_id,object_key,seq);

CREATE TABLE journal_snapshot_current (
    storage_id BINARY(16) NOT NULL,
    bucket_id BINARY(16) NOT NULL,
    object_key VARBINARY(1024) NOT NULL,
    generation BIGINT NOT NULL,
    seq BIGINT NOT NULL,
    deleted BOOLEAN NOT NULL,
    PRIMARY KEY (storage_id,bucket_id,object_key)
);

CREATE TABLE journal_snapshot_usage (
    storage_id BINARY(16) NOT NULL,
    bucket_id BINARY(16) NOT NULL,
    page_bytes BIGINT NOT NULL,
    invalid_bytes BIGINT NOT NULL,
    PRIMARY KEY (storage_id,bucket_id)
);

CREATE TABLE journal_snapshot_work (
    snapshot_id BINARY(16) NOT NULL,
    bucket_id BINARY(16) NOT NULL,
    object_key VARBINARY(1024) NOT NULL,
    generation BIGINT NOT NULL,
    seq BIGINT NOT NULL,
    deleted BOOLEAN NOT NULL,
    output_row BOOLEAN NOT NULL DEFAULT TRUE,
    PRIMARY KEY (snapshot_id,bucket_id,object_key),
    FOREIGN KEY (snapshot_id) REFERENCES journal_snapshots(id)
);

CREATE TABLE journal_snapshot_inputs (
    bucket_id BINARY(16) NOT NULL,
    seq BIGINT NOT NULL,
    page INT NOT NULL,
    encrypted LONGBLOB NOT NULL,
    PRIMARY KEY (bucket_id,seq,page)
);

CREATE TABLE journal_snapshot_holds (
    bucket_id BINARY(16) NOT NULL,
    seq BIGINT NOT NULL,
    storage_id BINARY(16) NOT NULL,
    unit VARCHAR(32) NOT NULL,
    address BINARY(16) NOT NULL,
    block_key BINARY(16),
    PRIMARY KEY (bucket_id,seq,storage_id,unit,address)
);
CREATE INDEX journal_snapshot_hold_address ON journal_snapshot_holds(address,unit);

CREATE TABLE journal_snapshot_gc (
    storage_id BINARY(16) NOT NULL,
    object_id BINARY(16) NOT NULL,
    page_no BIGINT NOT NULL,
    kind VARCHAR(32) NOT NULL CHECK (kind IN ('segment','page','manifest','checkpoint')),
    file_path VARCHAR(512) NOT NULL,
    etag VARCHAR(255) NOT NULL,
    remote_version VARCHAR(255),
    byte_count BIGINT NOT NULL,
    reclaim_at DATETIME(6) NOT NULL,
    PRIMARY KEY (storage_id,object_id,kind,page_no)
);
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
