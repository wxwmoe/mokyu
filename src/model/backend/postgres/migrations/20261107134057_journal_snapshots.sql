ALTER TABLE journal_stores ADD COLUMN snapshot_id UUID;
ALTER TABLE journal_stores ADD COLUMN snapshot_revision BIGINT NOT NULL DEFAULT 0;
ALTER TABLE journal_stores ADD COLUMN snapshot_log_bytes BIGINT NOT NULL DEFAULT 0;
ALTER TABLE journal_stores ADD COLUMN snapshot_auto_due BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE journal_stores ADD COLUMN snapshot_delete_bytes BIGINT NOT NULL DEFAULT 0;
ALTER TABLE journal_stores ADD COLUMN snapshot_invalid_bytes BIGINT NOT NULL DEFAULT 0;

ALTER TABLE journal_stores ADD COLUMN snapshot_token UUID;
ALTER TABLE journal_stores ADD COLUMN snapshot_lease_until TIMESTAMPTZ;

ALTER TABLE journal_stores ADD COLUMN snapshot_gc_error TEXT;
ALTER TABLE journal_buckets ADD COLUMN confirmed_log_bytes BIGINT NOT NULL DEFAULT 0;
UPDATE journal_buckets SET confirmed_log_bytes=COALESCE((SELECT SUM(m.encoded_bytes) FROM journal_file_entries m JOIN journal_files f ON f.id=m.file_id WHERE m.bucket_id=journal_buckets.bucket_id AND f.state='published'),0);
UPDATE journal_stores SET snapshot_log_bytes=COALESCE((SELECT SUM(f.byte_count) FROM journal_files f WHERE f.storage_id=journal_stores.storage_id AND f.state='published'),0);

CREATE TABLE journal_snapshots (
    encrypt_frames BOOLEAN NOT NULL DEFAULT FALSE,
    id UUID PRIMARY KEY NOT NULL,
    storage_id UUID NOT NULL REFERENCES journal_stores(storage_id),
    kind TEXT NOT NULL CHECK (kind IN ('append','compact','new')),
    state TEXT NOT NULL CHECK (state IN ('queued','building','prepared','current','retained','failed','gone')),
    parent UUID,
    revision BIGINT NOT NULL DEFAULT 0,
    sequence_no BIGINT NOT NULL DEFAULT 0,
    digest BYTEA,
    epoch UUID,
    page_limit BIGINT NOT NULL DEFAULT 0,
    covered_bytes BIGINT NOT NULL DEFAULT 0,
    manifest BYTEA,
    root_index BYTEA,
    manifest_etag TEXT,
    manifest_version TEXT,
    index_etag TEXT,
    checkpoint_etag TEXT,
    checkpoint_version TEXT,
    manifest_size BIGINT NOT NULL DEFAULT 0,
    index_size BIGINT NOT NULL DEFAULT 0,
    planned_at TIMESTAMPTZ NOT NULL DEFAULT (clock_timestamp()),
    confirmed_at TIMESTAMPTZ,
    gc_after TIMESTAMPTZ,
    lease_token UUID,
    lease_until TIMESTAMPTZ,
    attempts BIGINT NOT NULL DEFAULT 0,
    retry_after TIMESTAMPTZ,
    error_detail TEXT,
    UNIQUE (storage_id,id)
);
CREATE INDEX journal_snapshot_queue ON journal_snapshots(storage_id,state,planned_at);

CREATE TABLE journal_snapshot_buckets (
    snapshot_id UUID NOT NULL REFERENCES journal_snapshots(id),
    bucket_id UUID NOT NULL,
    bucket_name TEXT NOT NULL,
    generation BIGINT NOT NULL,
    watermark BIGINT NOT NULL,
    included BOOLEAN NOT NULL,
    mode_revision BIGINT NOT NULL,
    PRIMARY KEY (snapshot_id,bucket_id)
);

CREATE TABLE journal_snapshot_pages (
    owner UUID NOT NULL REFERENCES journal_snapshots(id),
    page_no BIGINT NOT NULL,
    family BIGINT NOT NULL,
    bucket_id UUID NOT NULL,
    description JSONB NOT NULL,
    encrypted BYTEA,
    byte_count BIGINT NOT NULL,
    etag TEXT,
    remote_version TEXT,
    gc_state TEXT NOT NULL DEFAULT 'live' CHECK (gc_state IN ('live','pending','gone')),
    PRIMARY KEY (owner,page_no)
);
CREATE INDEX journal_snapshot_family ON journal_snapshot_pages(owner,family);
CREATE TABLE journal_snapshot_refs (
    snapshot_id UUID NOT NULL REFERENCES journal_snapshots(id),
    position_no BIGINT NOT NULL,
    owner UUID NOT NULL,
    page_no BIGINT NOT NULL,
    PRIMARY KEY (snapshot_id,position_no),
    UNIQUE (snapshot_id,owner,page_no),
    FOREIGN KEY (owner,page_no) REFERENCES journal_snapshot_pages(owner,page_no)
);
CREATE INDEX journal_snapshot_ref_owner ON journal_snapshot_refs(owner,page_no);

CREATE TABLE journal_snapshot_members (
    owner UUID NOT NULL,
    family BIGINT NOT NULL,
    bucket_id UUID NOT NULL,
    generation BIGINT NOT NULL,
    seq BIGINT NOT NULL,
    object_key TEXT COLLATE "C" NOT NULL,
    deleted BOOLEAN NOT NULL,
    encoded_bytes BIGINT NOT NULL,
    PRIMARY KEY (owner,family,bucket_id,seq),
    FOREIGN KEY (owner,family) REFERENCES journal_snapshot_pages(owner,page_no)
);
CREATE INDEX journal_snapshot_member_key ON journal_snapshot_members(bucket_id,object_key,seq);

CREATE TABLE journal_snapshot_current (
    storage_id UUID NOT NULL,
    bucket_id UUID NOT NULL,
    object_key TEXT COLLATE "C" NOT NULL,
    generation BIGINT NOT NULL,
    seq BIGINT NOT NULL,
    deleted BOOLEAN NOT NULL,
    PRIMARY KEY (storage_id,bucket_id,object_key)
);

CREATE TABLE journal_snapshot_usage (
    storage_id UUID NOT NULL,
    bucket_id UUID NOT NULL,
    page_bytes BIGINT NOT NULL,
    invalid_bytes BIGINT NOT NULL,
    PRIMARY KEY (storage_id,bucket_id)
);

CREATE TABLE journal_snapshot_work (
    snapshot_id UUID NOT NULL REFERENCES journal_snapshots(id),
    bucket_id UUID NOT NULL,
    object_key TEXT COLLATE "C" NOT NULL,
    generation BIGINT NOT NULL,
    seq BIGINT NOT NULL,
    deleted BOOLEAN NOT NULL,
    output_row BOOLEAN NOT NULL DEFAULT TRUE,
    PRIMARY KEY (snapshot_id,bucket_id,object_key)
);

CREATE TABLE journal_snapshot_inputs (
    bucket_id UUID NOT NULL,
    seq BIGINT NOT NULL,
    page INT NOT NULL,
    encrypted BYTEA NOT NULL,
    PRIMARY KEY (bucket_id,seq,page)
);

CREATE TABLE journal_snapshot_holds (
    bucket_id UUID NOT NULL,
    seq BIGINT NOT NULL,
    storage_id UUID NOT NULL,
    unit TEXT NOT NULL,
    address UUID NOT NULL,
    block_key UUID,
    PRIMARY KEY (bucket_id,seq,storage_id,unit,address)
);
CREATE INDEX journal_snapshot_hold_address ON journal_snapshot_holds(address,unit);

CREATE TABLE journal_snapshot_gc (
    storage_id UUID NOT NULL,
    object_id UUID NOT NULL,
    page_no BIGINT NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN ('segment','page','manifest','checkpoint')),
    file_path TEXT NOT NULL,
    etag TEXT NOT NULL,
    remote_version TEXT,
    byte_count BIGINT NOT NULL,
    reclaim_at TIMESTAMPTZ NOT NULL,
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
