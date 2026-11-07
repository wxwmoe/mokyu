ALTER TABLE journal_buckets ADD COLUMN prepared_generation INTEGER NOT NULL DEFAULT 0 CHECK (prepared_generation>=0);
ALTER TABLE journal_buckets ADD COLUMN prepare_error TEXT;
ALTER TABLE journal_buckets ADD COLUMN flush_seq INTEGER NOT NULL DEFAULT 0 CHECK (flush_seq>=0);

CREATE TABLE journal_stores (
    storage_id BLOB NOT NULL PRIMARY KEY,
    policy TEXT NOT NULL,
    pending_policy TEXT,
    revision INTEGER NOT NULL DEFAULT 1 CHECK (revision > 0),
    descriptor TEXT,
    marker_installed INTEGER NOT NULL DEFAULT FALSE,
    emitted_seq INTEGER NOT NULL DEFAULT 0 CHECK (emitted_seq >= 0),
    seal_digest BLOB,
    allocated_seq INTEGER NOT NULL DEFAULT 0 CHECK (allocated_seq >= 0),
    allocated_digest BLOB,
    open_file BLOB,
    last_bucket BLOB,
    lease_token BLOB,
    lease_until INTEGER,
    flush_requested INTEGER NOT NULL DEFAULT FALSE,
    confirmed_at INTEGER,
    last_error TEXT,
    FOREIGN KEY (storage_id) REFERENCES storages(id)
) STRICT, WITHOUT ROWID;

CREATE TABLE journal_files (
    encrypt_frames INTEGER NOT NULL DEFAULT 0 CHECK (encrypt_frames IN (0,1)),
    id BLOB NOT NULL PRIMARY KEY,
    storage_id BLOB NOT NULL,
    epoch BLOB NOT NULL,
    first_seq INTEGER NOT NULL CHECK (first_seq > 0),
    last_seq INTEGER NOT NULL,
    previous_digest BLOB NOT NULL,
    digest BLOB NOT NULL,
    byte_count INTEGER NOT NULL DEFAULT 0 CHECK (byte_count >= 0),
    oldest_at INTEGER NOT NULL,
    part_cut INTEGER NOT NULL DEFAULT 0 CHECK (part_cut>=0),
    part_since_us INTEGER,
    upload_opened_at INTEGER,
    mode TEXT NOT NULL CHECK (mode IN ('put','multipart')),
    state TEXT NOT NULL CHECK (state IN ('open','sealed','published','gc','gone')),
    opened_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec')*1000000 AS INTEGER)),
    upload_id TEXT,
    etag TEXT,
    object_version TEXT,
    confirmed_at INTEGER,
    FOREIGN KEY (storage_id) REFERENCES journal_stores(storage_id),
    UNIQUE (storage_id,epoch,first_seq)
) STRICT, WITHOUT ROWID;
CREATE INDEX journal_files_state ON journal_files(storage_id,state,first_seq);

CREATE TABLE journal_file_frames (
    file_id BLOB NOT NULL,
    byte_offset INTEGER NOT NULL,
    encoded BLOB NOT NULL,
    PRIMARY KEY (file_id,byte_offset),
    FOREIGN KEY (file_id) REFERENCES journal_files(id)
) STRICT, WITHOUT ROWID;

CREATE TABLE journal_file_entries (
    file_id BLOB NOT NULL,
    bucket_id BLOB NOT NULL,
    seq INTEGER NOT NULL,
    byte_offset INTEGER NOT NULL CHECK (byte_offset >= 0),
    encoded_bytes INTEGER NOT NULL CHECK (encoded_bytes > 0),
    page INTEGER NOT NULL,
    final_page INTEGER NOT NULL,
    part_number INTEGER,
    PRIMARY KEY (file_id,bucket_id,seq,page,byte_offset),
    FOREIGN KEY (file_id) REFERENCES journal_files(id),
    FOREIGN KEY (bucket_id,seq) REFERENCES journal_entries(bucket_id,seq)
) STRICT, WITHOUT ROWID;
CREATE INDEX journal_file_entry ON journal_file_entries(bucket_id,seq);

CREATE TABLE journal_parts (
    file_id BLOB NOT NULL,
    number INTEGER NOT NULL CHECK (number BETWEEN 1 AND 10000),
    byte_offset INTEGER NOT NULL CHECK (byte_offset >= 0),
    byte_count INTEGER NOT NULL CHECK (byte_count > 0),
    digest BLOB NOT NULL,
    etag TEXT,
    PRIMARY KEY (file_id,number),
    FOREIGN KEY (file_id) REFERENCES journal_files(id)
) STRICT, WITHOUT ROWID;

CREATE TABLE journal_dictionary (
    storage_id BLOB NOT NULL,
    epoch BLOB NOT NULL,
    identity_hash BLOB NOT NULL,
    content_hash BLOB NOT NULL,
    first_file BLOB NOT NULL,
    PRIMARY KEY (storage_id,epoch,identity_hash),
    FOREIGN KEY (first_file) REFERENCES journal_files(id)
) STRICT, WITHOUT ROWID;

CREATE TABLE journal_drafts (
    bucket_id BLOB NOT NULL,
    seq INTEGER NOT NULL,
    page INTEGER NOT NULL,
    end_offset INTEGER NOT NULL,
    range_count INTEGER NOT NULL,
    range_digest BLOB NOT NULL,
    source_revision INTEGER,
    final_page INTEGER NOT NULL,
    ready_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec')*1000000 AS INTEGER)),
    assigned INTEGER NOT NULL DEFAULT FALSE,
    encrypted BLOB NOT NULL,
    PRIMARY KEY (bucket_id,seq,page),
    FOREIGN KEY (bucket_id,seq) REFERENCES journal_entries(bucket_id,seq)
) STRICT, WITHOUT ROWID;

CREATE INDEX journal_draft_queue ON journal_drafts(assigned,bucket_id,seq,page);
CREATE INDEX journal_complete_draft ON journal_drafts(bucket_id,seq,final_page);

CREATE TABLE journal_holds (
    bucket_id BLOB NOT NULL,
    seq INTEGER NOT NULL,
    storage_id BLOB NOT NULL,
    unit TEXT NOT NULL CHECK (unit IN ('chunk','pack','raw','fragment')),
    address BLOB NOT NULL,
    block_key BLOB,
    PRIMARY KEY (bucket_id,seq,storage_id,unit,address),
    FOREIGN KEY (bucket_id,seq) REFERENCES journal_entries(bucket_id,seq),
    FOREIGN KEY (storage_id) REFERENCES storages(id),
    FOREIGN KEY (block_key) REFERENCES managed_keys(id)
) STRICT, WITHOUT ROWID;
CREATE INDEX journal_held_address ON journal_holds(storage_id,unit,address);
CREATE INDEX journal_held_identity ON journal_holds(address,unit);
CREATE INDEX journal_held_key ON journal_holds(block_key);

UPDATE mokyu_meta SET schema_version = 20261107104724;
