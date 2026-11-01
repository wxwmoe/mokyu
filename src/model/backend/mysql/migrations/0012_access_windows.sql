CREATE TABLE IF NOT EXISTS access_state (
    id TINYINT UNSIGNED NOT NULL PRIMARY KEY,
    coverage_since DATETIME(6),
    flushed_at DATETIME(6),
    CONSTRAINT access_state_singleton CHECK (id=1)
) ENGINE=InnoDB;

INSERT IGNORE INTO access_state(id,coverage_since,flushed_at)
VALUES(1,CURRENT_TIMESTAMP(6),CURRENT_TIMESTAMP(6));

CREATE TABLE IF NOT EXISTS chunk_access_windows (
    chunk_id BIGINT NOT NULL,
    window_start DATETIME(6) NOT NULL,
    read_count BIGINT NOT NULL DEFAULT 0,
    range_read_count BIGINT NOT NULL DEFAULT 0,
    read_bytes BIGINT NOT NULL DEFAULT 0,
    range_origin_reads BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (chunk_id,window_start),
    INDEX chunk_access_expiry (window_start),
    CONSTRAINT chunk_access_source FOREIGN KEY (chunk_id) REFERENCES chunks(id) ON DELETE CASCADE
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS pack_access_windows (
    pack_id BIGINT NOT NULL,
    window_start DATETIME(6) NOT NULL,
    downloads BIGINT NOT NULL DEFAULT 0,
    downloaded_bytes BIGINT NOT NULL DEFAULT 0,
    partial_downloads BIGINT NOT NULL DEFAULT 0,
    useful_bytes BIGINT NOT NULL DEFAULT 0,
    partial_bytes BIGINT NOT NULL DEFAULT 0,
    updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (pack_id,window_start),
    INDEX pack_access_expiry (window_start),
    CONSTRAINT pack_access_source FOREIGN KEY (pack_id) REFERENCES packs(id) ON DELETE CASCADE
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS pack_member_access_windows (
    pack_id BIGINT NOT NULL,
    window_start DATETIME(6) NOT NULL,
    chunk_id BIGINT NOT NULL,
    downloads BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (pack_id,window_start,chunk_id),
    INDEX pack_member_access_expiry (window_start),
    CONSTRAINT pack_member_access_pack FOREIGN KEY (pack_id) REFERENCES packs(id) ON DELETE CASCADE,
    CONSTRAINT pack_member_access_chunk FOREIGN KEY (chunk_id) REFERENCES chunks(id) ON DELETE CASCADE
) ENGINE=InnoDB;

UPDATE mokyu_meta SET schema_version=12 WHERE id=1;
