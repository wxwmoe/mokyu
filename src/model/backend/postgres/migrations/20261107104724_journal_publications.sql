ALTER TABLE journal_buckets ADD COLUMN prepared_generation BIGINT NOT NULL DEFAULT 0 CHECK (prepared_generation>=0);
ALTER TABLE journal_buckets ADD COLUMN prepare_error TEXT;
ALTER TABLE journal_buckets ADD COLUMN flush_seq BIGINT NOT NULL DEFAULT 0 CHECK (flush_seq>=0);

CREATE TABLE journal_stores (
    storage_id UUID NOT NULL PRIMARY KEY,
    policy JSONB NOT NULL,
    pending_policy JSONB,
    revision BIGINT NOT NULL DEFAULT 1 CHECK (revision > 0),
    descriptor JSONB,
    marker_installed BOOLEAN NOT NULL DEFAULT FALSE,
    emitted_seq BIGINT NOT NULL DEFAULT 0 CHECK (emitted_seq >= 0),
    seal_digest BYTEA,
    allocated_seq BIGINT NOT NULL DEFAULT 0 CHECK (allocated_seq >= 0),
    allocated_digest BYTEA,
    open_file UUID,
    last_bucket UUID,
    lease_token UUID,
    lease_until TIMESTAMPTZ,
    flush_requested BOOLEAN NOT NULL DEFAULT FALSE,
    confirmed_at TIMESTAMPTZ,
    last_error TEXT,
    FOREIGN KEY (storage_id) REFERENCES storages(id)
);

CREATE TABLE journal_files (
    encrypt_frames BOOLEAN NOT NULL DEFAULT FALSE,
    id UUID NOT NULL PRIMARY KEY,
    storage_id UUID NOT NULL,
    epoch UUID NOT NULL,
    first_seq BIGINT NOT NULL CHECK (first_seq > 0),
    last_seq BIGINT NOT NULL,
    previous_digest BYTEA NOT NULL,
    digest BYTEA NOT NULL,
    byte_count BIGINT NOT NULL DEFAULT 0 CHECK (byte_count >= 0),
    oldest_at TIMESTAMPTZ NOT NULL,
    part_cut BIGINT NOT NULL DEFAULT 0 CHECK (part_cut>=0),
    part_since_us BIGINT,
    upload_opened_at TIMESTAMPTZ,
    mode TEXT NOT NULL CHECK (mode IN ('put','multipart')),
    state TEXT NOT NULL CHECK (state IN ('open','sealed','published','gc','gone')),
    opened_at TIMESTAMPTZ NOT NULL DEFAULT (clock_timestamp()),
    upload_id TEXT,
    etag TEXT,
    object_version TEXT,
    confirmed_at TIMESTAMPTZ,
    FOREIGN KEY (storage_id) REFERENCES journal_stores(storage_id),
    UNIQUE (storage_id,epoch,first_seq)
);
CREATE INDEX journal_files_state ON journal_files(storage_id,state,first_seq);

CREATE TABLE journal_file_frames (
    file_id UUID NOT NULL,
    byte_offset BIGINT NOT NULL,
    encoded BYTEA NOT NULL,
    PRIMARY KEY (file_id,byte_offset),
    FOREIGN KEY (file_id) REFERENCES journal_files(id)
);

CREATE TABLE journal_file_entries (
    file_id UUID NOT NULL,
    bucket_id UUID NOT NULL,
    seq BIGINT NOT NULL,
    byte_offset BIGINT NOT NULL CHECK (byte_offset >= 0),
    encoded_bytes BIGINT NOT NULL CHECK (encoded_bytes > 0),
    page INTEGER NOT NULL,
    final_page BOOLEAN NOT NULL,
    part_number INTEGER,
    PRIMARY KEY (file_id,bucket_id,seq,page,byte_offset),
    FOREIGN KEY (file_id) REFERENCES journal_files(id),
    FOREIGN KEY (bucket_id,seq) REFERENCES journal_entries(bucket_id,seq)
);
CREATE INDEX journal_file_entry ON journal_file_entries(bucket_id,seq);

CREATE TABLE journal_parts (
    file_id UUID NOT NULL,
    number INTEGER NOT NULL CHECK (number BETWEEN 1 AND 10000),
    byte_offset BIGINT NOT NULL CHECK (byte_offset >= 0),
    byte_count BIGINT NOT NULL CHECK (byte_count > 0),
    digest BYTEA NOT NULL,
    etag TEXT,
    PRIMARY KEY (file_id,number),
    FOREIGN KEY (file_id) REFERENCES journal_files(id)
);

CREATE TABLE journal_dictionary (
    storage_id UUID NOT NULL,
    epoch UUID NOT NULL,
    identity_hash BYTEA NOT NULL,
    content_hash BYTEA NOT NULL,
    first_file UUID NOT NULL,
    PRIMARY KEY (storage_id,epoch,identity_hash),
    FOREIGN KEY (first_file) REFERENCES journal_files(id)
);

CREATE TABLE journal_drafts (
    bucket_id UUID NOT NULL,
    seq BIGINT NOT NULL,
    page INTEGER NOT NULL,
    end_offset BIGINT NOT NULL,
    range_count BIGINT NOT NULL,
    range_digest BYTEA NOT NULL,
    source_revision BIGINT,
    final_page BOOLEAN NOT NULL,
    ready_at TIMESTAMPTZ NOT NULL DEFAULT (clock_timestamp()),
    assigned BOOLEAN NOT NULL DEFAULT FALSE,
    encrypted BYTEA NOT NULL,
    PRIMARY KEY (bucket_id,seq,page),
    FOREIGN KEY (bucket_id,seq) REFERENCES journal_entries(bucket_id,seq)
);

CREATE INDEX journal_draft_queue ON journal_drafts(assigned,bucket_id,seq,page);
CREATE INDEX journal_complete_draft ON journal_drafts(bucket_id,seq,final_page);

CREATE TABLE journal_holds (
    bucket_id UUID NOT NULL,
    seq BIGINT NOT NULL,
    storage_id UUID NOT NULL,
    unit TEXT NOT NULL CHECK (unit IN ('chunk','pack','raw','fragment')),
    address UUID NOT NULL,
    block_key UUID,
    PRIMARY KEY (bucket_id,seq,storage_id,unit,address),
    FOREIGN KEY (bucket_id,seq) REFERENCES journal_entries(bucket_id,seq),
    FOREIGN KEY (storage_id) REFERENCES storages(id),
    FOREIGN KEY (block_key) REFERENCES managed_keys(id)
);
CREATE INDEX journal_held_address ON journal_holds(storage_id,unit,address);
CREATE INDEX journal_held_identity ON journal_holds(address,unit);
CREATE INDEX journal_held_key ON journal_holds(block_key);

UPDATE mokyu_meta SET schema_version = 20261107104724;
