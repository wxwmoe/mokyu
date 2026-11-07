ALTER TABLE managed_keys ADD COLUMN recovery_read_only BOOLEAN NOT NULL DEFAULT FALSE;

CREATE TABLE journal_recoveries (
    id BINARY(16) NOT NULL PRIMARY KEY,
    state VARCHAR(128) NOT NULL CHECK (state IN ('locked','preview','queued','verifying','importing','claiming','completed','failed','cancelled')),
    source LONGBLOB NOT NULL,
    selection JSON,
    progress JSON NOT NULL,
    error_detail VARCHAR(1024),
    revision BIGINT NOT NULL DEFAULT 1 CHECK (revision>0),
    source_storage BINARY(16) NOT NULL,
    target_storage BINARY(16),
    lease_token BINARY(16),
    lease_until DATETIME(6),
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
);
CREATE INDEX journal_recoveries_work ON journal_recoveries(state,created_at);
CREATE INDEX journal_recoveries_source ON journal_recoveries(source_storage,state);

CREATE TABLE journal_restore_buckets (
    job BINARY(16) NOT NULL,
    source_bucket BINARY(16) NOT NULL,
    target_bucket BINARY(16) NOT NULL UNIQUE,
    target_name VARCHAR(128) NOT NULL,
    generation BIGINT NOT NULL,
    watermark BIGINT NOT NULL,
    state VARCHAR(128) NOT NULL CHECK (state IN ('planned','building','published')),
    next_key VARBINARY(1024),
    restored_objects BIGINT NOT NULL DEFAULT 0,
    restored_bytes BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (job,source_bucket),
    FOREIGN KEY (job) REFERENCES journal_recoveries(id)
);

CREATE TABLE journal_restore_materials (
    job BINARY(16) NOT NULL,
    identity_hash BINARY(32) NOT NULL,
    unit VARCHAR(16) NOT NULL,
    address BINARY(16) NOT NULL,
    native_id BIGINT NOT NULL,
    content_hash BINARY(32) NOT NULL,
    PRIMARY KEY (job,identity_hash),
    FOREIGN KEY (job) REFERENCES journal_recoveries(id)
);

CREATE INDEX journal_restore_addresses ON journal_restore_materials(address);

CREATE TABLE journal_restore_items (
    job BINARY(16) NOT NULL,
    source_bucket BINARY(16) NOT NULL,
    object_key VARBINARY(1024) NOT NULL,
    size BIGINT NOT NULL,
    etag VARCHAR(128) NOT NULL,
    metadata JSON NOT NULL,
    checksums JSON NOT NULL,
    payload VARCHAR(128) NOT NULL,
    modified_us BIGINT NOT NULL,
    content_type TEXT NOT NULL,
    category VARCHAR(128) NOT NULL,
    PRIMARY KEY (job,source_bucket,object_key),
    FOREIGN KEY (job,source_bucket) REFERENCES journal_restore_buckets(job,source_bucket)
);

CREATE TABLE journal_restore_ranges (
    job BINARY(16) NOT NULL,
    source_bucket BINARY(16) NOT NULL,
    object_key VARBINARY(1024) NOT NULL,
    offset_bytes BIGINT NOT NULL,
    length BIGINT NOT NULL,
    source_offset BIGINT NOT NULL,
    kind VARCHAR(128) NOT NULL CHECK (kind IN ('raw','chunk','fragment')),
    native_id BIGINT NOT NULL,
    PRIMARY KEY (job,source_bucket,object_key,offset_bytes),
    FOREIGN KEY (job,source_bucket,object_key) REFERENCES journal_restore_items(job,source_bucket,object_key) ON DELETE CASCADE
);

CREATE TABLE journal_restore_scopes (
    storage_id BINARY(16) NOT NULL,
    job BINARY(16) NOT NULL,
    protect_unknown BOOLEAN NOT NULL DEFAULT TRUE,
    PRIMARY KEY (storage_id,job),
    FOREIGN KEY (job) REFERENCES journal_recoveries(id)
);

CREATE TABLE journal_restore_orphans (
    job BINARY(16) NOT NULL,
    address BINARY(16) NOT NULL,
    file_path VARCHAR(1024) NOT NULL,
    byte_count BIGINT NOT NULL CHECK (byte_count>=0),
    PRIMARY KEY (job,address),
    FOREIGN KEY (job) REFERENCES journal_recoveries(id)
);

CREATE VIEW journal_protected_addresses AS
SELECT address,unit FROM journal_holds
UNION ALL
SELECT m.address,m.unit FROM journal_restore_materials m JOIN journal_recoveries j ON j.id=m.job
WHERE j.state NOT IN ('completed','cancelled');

UPDATE mokyu_meta SET schema_version = 20261107122341;
