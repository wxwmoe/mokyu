ALTER TABLE managed_keys ADD COLUMN recovery_read_only BOOLEAN NOT NULL DEFAULT FALSE;

CREATE TABLE journal_recoveries (
    id UUID NOT NULL PRIMARY KEY,
    state TEXT NOT NULL CHECK (state IN ('locked','preview','queued','verifying','importing','claiming','completed','failed','cancelled')),
    source BYTEA NOT NULL,
    selection JSONB,
    progress JSONB NOT NULL,
    error_detail TEXT,
    revision BIGINT NOT NULL DEFAULT 1 CHECK (revision>0),
    source_storage UUID NOT NULL,
    target_storage UUID,
    lease_token UUID,
    lease_until TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT (clock_timestamp()),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT (clock_timestamp())
);
CREATE INDEX journal_recoveries_work ON journal_recoveries(state,created_at);
CREATE INDEX journal_recoveries_source ON journal_recoveries(source_storage,state);

CREATE TABLE journal_restore_buckets (
    job UUID NOT NULL,
    source_bucket UUID NOT NULL,
    target_bucket UUID NOT NULL UNIQUE,
    target_name TEXT NOT NULL,
    generation BIGINT NOT NULL,
    watermark BIGINT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('planned','building','published')),
    next_key TEXT,
    restored_objects BIGINT NOT NULL DEFAULT 0,
    restored_bytes BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (job,source_bucket),
    FOREIGN KEY (job) REFERENCES journal_recoveries(id)
);

CREATE TABLE journal_restore_materials (
    job UUID NOT NULL,
    identity_hash BYTEA NOT NULL,
    unit TEXT NOT NULL,
    address UUID NOT NULL,
    native_id UUID NOT NULL,
    content_hash BYTEA NOT NULL,
    PRIMARY KEY (job,identity_hash),
    FOREIGN KEY (job) REFERENCES journal_recoveries(id)
);

CREATE INDEX journal_restore_addresses ON journal_restore_materials(address);

CREATE TABLE journal_restore_items (
    job UUID NOT NULL,
    source_bucket UUID NOT NULL,
    object_key TEXT NOT NULL,
    size BIGINT NOT NULL,
    etag TEXT NOT NULL,
    metadata JSONB NOT NULL,
    checksums JSONB NOT NULL,
    payload TEXT NOT NULL,
    modified_us BIGINT NOT NULL,
    content_type TEXT NOT NULL,
    category TEXT NOT NULL,
    PRIMARY KEY (job,source_bucket,object_key),
    FOREIGN KEY (job,source_bucket) REFERENCES journal_restore_buckets(job,source_bucket)
);

CREATE TABLE journal_restore_ranges (
    job UUID NOT NULL,
    source_bucket UUID NOT NULL,
    object_key TEXT COLLATE "C" NOT NULL,
    offset_bytes BIGINT NOT NULL,
    length BIGINT NOT NULL,
    source_offset BIGINT NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN ('raw','chunk','fragment')),
    native_id UUID NOT NULL,
    PRIMARY KEY (job,source_bucket,object_key,offset_bytes),
    FOREIGN KEY (job,source_bucket,object_key) REFERENCES journal_restore_items(job,source_bucket,object_key) ON DELETE CASCADE
);

CREATE TABLE journal_restore_scopes (
    storage_id UUID NOT NULL,
    job UUID NOT NULL,
    protect_unknown BOOLEAN NOT NULL DEFAULT TRUE,
    PRIMARY KEY (storage_id,job),
    FOREIGN KEY (job) REFERENCES journal_recoveries(id)
);

CREATE TABLE journal_restore_orphans (
    job UUID NOT NULL,
    address UUID NOT NULL,
    file_path TEXT NOT NULL,
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
