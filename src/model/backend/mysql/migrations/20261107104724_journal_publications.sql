ALTER TABLE journal_buckets ADD COLUMN prepared_generation BIGINT NOT NULL DEFAULT 0 CHECK (prepared_generation>=0);
ALTER TABLE journal_buckets ADD COLUMN prepare_error VARCHAR(1024);
ALTER TABLE journal_buckets ADD COLUMN flush_seq BIGINT NOT NULL DEFAULT 0 CHECK (flush_seq>=0);

CREATE TABLE journal_stores (
    storage_id BINARY(16) NOT NULL PRIMARY KEY,
    policy JSON NOT NULL,
    pending_policy JSON,
    revision BIGINT NOT NULL DEFAULT 1 CHECK (revision > 0),
    descriptor JSON,
    marker_installed BOOLEAN NOT NULL DEFAULT FALSE,
    emitted_seq BIGINT NOT NULL DEFAULT 0 CHECK (emitted_seq >= 0),
    seal_digest BINARY(32),
    allocated_seq BIGINT NOT NULL DEFAULT 0 CHECK (allocated_seq >= 0),
    allocated_digest BINARY(32),
    open_file BINARY(16),
    last_bucket BINARY(16),
    lease_token BINARY(16),
    lease_until DATETIME(6),
    flush_requested BOOLEAN NOT NULL DEFAULT FALSE,
    confirmed_at DATETIME(6),
    last_error VARCHAR(1024),
    FOREIGN KEY (storage_id) REFERENCES storages(id)
);

CREATE TABLE journal_files (
    encrypt_frames BOOLEAN NOT NULL DEFAULT FALSE,
    id BINARY(16) NOT NULL PRIMARY KEY,
    storage_id BINARY(16) NOT NULL,
    epoch BINARY(16) NOT NULL,
    first_seq BIGINT NOT NULL CHECK (first_seq > 0),
    last_seq BIGINT NOT NULL,
    previous_digest BINARY(32) NOT NULL,
    digest BINARY(32) NOT NULL,
    byte_count BIGINT NOT NULL DEFAULT 0 CHECK (byte_count >= 0),
    oldest_at DATETIME(6) NOT NULL,
    part_cut BIGINT NOT NULL DEFAULT 0 CHECK (part_cut>=0),
    part_since_us BIGINT,
    upload_opened_at DATETIME(6),
    mode VARCHAR(16) NOT NULL CHECK (mode IN ('put','multipart')),
    state VARCHAR(16) NOT NULL CHECK (state IN ('open','sealed','published','gc','gone')),
    opened_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    upload_id VARCHAR(1024),
    etag VARCHAR(255),
    object_version VARCHAR(1024),
    confirmed_at DATETIME(6),
    FOREIGN KEY (storage_id) REFERENCES journal_stores(storage_id),
    UNIQUE (storage_id,epoch,first_seq)
);
CREATE INDEX journal_files_state ON journal_files(storage_id,state,first_seq);

CREATE TABLE journal_file_frames (
    file_id BINARY(16) NOT NULL,
    byte_offset BIGINT NOT NULL,
    encoded LONGBLOB NOT NULL,
    PRIMARY KEY (file_id,byte_offset),
    FOREIGN KEY (file_id) REFERENCES journal_files(id)
);

CREATE TABLE journal_file_entries (
    file_id BINARY(16) NOT NULL,
    bucket_id BINARY(16) NOT NULL,
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
    file_id BINARY(16) NOT NULL,
    number INTEGER NOT NULL CHECK (number BETWEEN 1 AND 10000),
    byte_offset BIGINT NOT NULL CHECK (byte_offset >= 0),
    byte_count BIGINT NOT NULL CHECK (byte_count > 0),
    digest BINARY(32) NOT NULL,
    etag VARCHAR(255),
    PRIMARY KEY (file_id,number),
    FOREIGN KEY (file_id) REFERENCES journal_files(id)
);

CREATE TABLE journal_dictionary (
    storage_id BINARY(16) NOT NULL,
    epoch BINARY(16) NOT NULL,
    identity_hash BINARY(32) NOT NULL,
    content_hash BINARY(32) NOT NULL,
    first_file BINARY(16) NOT NULL,
    PRIMARY KEY (storage_id,epoch,identity_hash),
    FOREIGN KEY (first_file) REFERENCES journal_files(id)
);

CREATE TABLE journal_drafts (
    bucket_id BINARY(16) NOT NULL,
    seq BIGINT NOT NULL,
    page INTEGER NOT NULL,
    end_offset BIGINT NOT NULL,
    range_count BIGINT NOT NULL,
    range_digest BINARY(32) NOT NULL,
    source_revision BIGINT,
    final_page BOOLEAN NOT NULL,
    ready_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    assigned BOOLEAN NOT NULL DEFAULT FALSE,
    encrypted LONGBLOB NOT NULL,
    PRIMARY KEY (bucket_id,seq,page),
    FOREIGN KEY (bucket_id,seq) REFERENCES journal_entries(bucket_id,seq)
);

CREATE INDEX journal_draft_queue ON journal_drafts(assigned,bucket_id,seq,page);
CREATE INDEX journal_complete_draft ON journal_drafts(bucket_id,seq,final_page);

CREATE TABLE journal_holds (
    bucket_id BINARY(16) NOT NULL,
    seq BIGINT NOT NULL,
    storage_id BINARY(16) NOT NULL,
    unit VARCHAR(16) NOT NULL CHECK (unit IN ('chunk','pack','raw','fragment')),
    address BINARY(16) NOT NULL,
    block_key BINARY(16),
    PRIMARY KEY (bucket_id,seq,storage_id,unit,address),
    FOREIGN KEY (bucket_id,seq) REFERENCES journal_entries(bucket_id,seq),
    FOREIGN KEY (storage_id) REFERENCES storages(id),
    FOREIGN KEY (block_key) REFERENCES managed_keys(id)
);
CREATE INDEX journal_held_address ON journal_holds(storage_id,unit,address);
CREATE INDEX journal_held_identity ON journal_holds(address,unit);
CREATE INDEX journal_held_key ON journal_holds(block_key);

UPDATE mokyu_meta SET schema_version = 20261107104724;
