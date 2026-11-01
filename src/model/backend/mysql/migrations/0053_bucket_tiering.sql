CREATE TABLE bucket_tiering (
    bucket_id BINARY(16) NOT NULL PRIMARY KEY,
    hot_storage BINARY(16) NOT NULL,
    cold_storage BINARY(16) NOT NULL,
    cold_domain BINARY(32) NOT NULL,
    cold_space TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    policy JSON NOT NULL,
    revision BIGINT NOT NULL CHECK (revision > 0),
    state VARCHAR(12) NOT NULL CHECK (state IN ('active','paused','draining')),
    observed_since DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    FOREIGN KEY (bucket_id) REFERENCES buckets(id),
    FOREIGN KEY (hot_storage) REFERENCES storages(id),
    FOREIGN KEY (cold_storage) REFERENCES storages(id),
    CHECK (hot_storage <> cold_storage)
);
CREATE INDEX bucket_tiering_cold ON bucket_tiering(cold_storage,bucket_id);
CREATE INDEX bucket_tiering_hot ON bucket_tiering(hot_storage,bucket_id);

CREATE TABLE physical_demand (
    unit SMALLINT NOT NULL,
    physical_id BINARY(16) NOT NULL,
    last_read BIGINT NOT NULL,
    read_count BIGINT NOT NULL,
    served_bytes BIGINT NOT NULL,
    origin_reads BIGINT NOT NULL,
    PRIMARY KEY (unit,physical_id),
    CONSTRAINT physical_demand_unit CHECK (unit BETWEEN 0 AND 3)
);

CREATE INDEX physical_demand_recent ON physical_demand(last_read,unit,physical_id);

CREATE TABLE physical_demand_windows (
    unit SMALLINT NOT NULL,
    physical_id BINARY(16) NOT NULL,
    hour_start BIGINT NOT NULL,
    read_count BIGINT NOT NULL,
    served_bytes BIGINT NOT NULL,
    origin_reads BIGINT NOT NULL,
    PRIMARY KEY (unit,physical_id,hour_start),
    CONSTRAINT physical_demand_window_unit CHECK (unit BETWEEN 0 AND 3)
);
CREATE INDEX physical_demand_expiry ON physical_demand_windows(hour_start,unit,physical_id);

UPDATE mokyu_meta SET schema_version=53 WHERE id=1;
