CREATE TABLE tier_locations (
    unit SMALLINT NOT NULL,
    physical_id uuid NOT NULL,
    home_storage uuid NOT NULL,
    storage_id uuid NOT NULL,
    address uuid NOT NULL UNIQUE,
    generation BIGINT NOT NULL,
    material jsonb NOT NULL,
    block_key_id uuid,
    stored_bytes BIGINT NOT NULL,
    activated_at BIGINT NOT NULL,
    pressure_seconds BIGINT NOT NULL,
    pressure_since BIGINT NOT NULL DEFAULT 0,
    origin_reads BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (unit,physical_id),
    CONSTRAINT tier_location_unit CHECK (unit BETWEEN 0 AND 2),
    CONSTRAINT tier_location_generation CHECK (generation > 0),
    CONSTRAINT tier_location_size CHECK (stored_bytes >= 0),
    CONSTRAINT tier_location_pressure CHECK (pressure_seconds > 0 AND origin_reads >= 0),
    FOREIGN KEY (home_storage) REFERENCES storages(id),
    FOREIGN KEY (storage_id) REFERENCES storages(id),
    FOREIGN KEY (block_key_id) REFERENCES managed_keys(id)
);
CREATE INDEX tier_locations_storage ON tier_locations(storage_id,unit,physical_id);

CREATE TABLE tier_moves (
    id uuid NOT NULL PRIMARY KEY,
    bucket_id uuid NOT NULL,
    unit SMALLINT NOT NULL,
    physical_id uuid NOT NULL,
    policy_revision BIGINT NOT NULL,
    source_generation BIGINT NOT NULL,
    source_storage uuid NOT NULL,
    source_address uuid NOT NULL,
    source_material jsonb NOT NULL,
    source_stamp bytea NOT NULL,
    source_key uuid,
    source_bytes BIGINT NOT NULL,
    target_storage uuid NOT NULL,
    target_address uuid NOT NULL UNIQUE,
    target_material jsonb,
    target_key uuid,
    target_bytes BIGINT,
    receipt bytea,
    checkpoint jsonb,
    phase text NOT NULL,
    queued_at BIGINT NOT NULL,
    retire_after BIGINT NOT NULL DEFAULT 0,
    UNIQUE (unit,physical_id),
    CONSTRAINT tier_move_unit CHECK (unit BETWEEN 0 AND 2),
    CONSTRAINT tier_move_generation CHECK (source_generation >= 0 AND policy_revision > 0),
    CONSTRAINT tier_move_size CHECK (source_bytes >= 0 AND (target_bytes IS NULL OR target_bytes >= 0)),
    CONSTRAINT tier_move_phase CHECK (phase IN ('copying','verified','retiring','discarding')),
    CONSTRAINT tier_move_distinct CHECK (source_storage <> target_storage AND source_address <> target_address),
    FOREIGN KEY (id) REFERENCES tasks(id),
    FOREIGN KEY (bucket_id) REFERENCES buckets(id),
    FOREIGN KEY (source_storage) REFERENCES storages(id),
    FOREIGN KEY (target_storage) REFERENCES storages(id),
    FOREIGN KEY (source_key) REFERENCES managed_keys(id),
    FOREIGN KEY (target_key) REFERENCES managed_keys(id)
);
CREATE INDEX tier_moves_owner ON tier_moves(bucket_id,phase,id);
CREATE INDEX tier_moves_source ON tier_moves(source_address,phase);

CREATE TABLE tier_scans (
    bucket_id uuid NOT NULL PRIMARY KEY,
    revision BIGINT NOT NULL,
    unit SMALLINT NOT NULL,
    after_id uuid NOT NULL,
    next_scan BIGINT NOT NULL,
    CONSTRAINT tier_scan_unit CHECK (unit BETWEEN 0 AND 2),
    CONSTRAINT tier_scan_revision CHECK (revision > 0),
    FOREIGN KEY (bucket_id) REFERENCES buckets(id) ON DELETE CASCADE
);
CREATE INDEX tier_scans_due ON tier_scans(next_scan,bucket_id);

UPDATE mokyu_meta SET schema_version=101 WHERE singleton;
