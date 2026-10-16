CREATE TABLE IF NOT EXISTS objects (
    bucket_id BINARY(16) NOT NULL,
    object_key VARBINARY(1024) NOT NULL,
    stream_id BINARY(16),
    write_token BINARY(16) NOT NULL,
    catalog_size BIGINT,
    catalog_modified DATETIME(6),
    catalog_type TEXT,
    catalog_category VARCHAR(16),
    catalog_public BOOLEAN,
    PRIMARY KEY (bucket_id,object_key),
    CONSTRAINT objects_bucket FOREIGN KEY (bucket_id) REFERENCES buckets(id),
    CONSTRAINT objects_stream FOREIGN KEY (stream_id) REFERENCES streams(id),
    CONSTRAINT objects_key CHECK (OCTET_LENGTH(object_key) BETWEEN 1 AND 1024),
    INDEX objects_catalog_size (bucket_id,catalog_size,object_key),
    INDEX objects_catalog_modified (bucket_id,catalog_modified,object_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 /*!80000 COLLATE=utf8mb4_0900_bin */ /*M!100603 COLLATE=utf8mb4_nopad_bin */;

CREATE TABLE IF NOT EXISTS fragments (
    id BINARY(16) NOT NULL PRIMARY KEY,
    owner_stream BINARY(16),
    sealed BOOLEAN NOT NULL DEFAULT FALSE,
    size INT NOT NULL,
    hash BINARY(32) NOT NULL,
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    CONSTRAINT fragments_stream FOREIGN KEY (owner_stream) REFERENCES streams(id) ON DELETE SET NULL,
    CONSTRAINT fragments_size CHECK (size BETWEEN 1 AND 4194304),
    INDEX fragments_cleanup (created_at,id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS extents (
    stream_id BINARY(16) NOT NULL,
    offset_bytes BIGINT NOT NULL,
    length INT NOT NULL,
    chunk_id BIGINT,
    fragment_id BINARY(16),
    source_offset INT NOT NULL DEFAULT 0,
    PRIMARY KEY (stream_id,offset_bytes),
    CONSTRAINT extents_stream FOREIGN KEY (stream_id) REFERENCES streams(id) ON DELETE CASCADE,
    CONSTRAINT extents_chunk FOREIGN KEY (chunk_id) REFERENCES chunks(id),
    CONSTRAINT extents_fragment FOREIGN KEY (fragment_id) REFERENCES fragments(id),
    CONSTRAINT extents_source CHECK ((chunk_id IS NULL)<>(fragment_id IS NULL)),
    CONSTRAINT extents_length CHECK (length BETWEEN 1 AND 4194304),
    CONSTRAINT extents_offset CHECK (offset_bytes>=0),
    CONSTRAINT extents_source_offset CHECK (source_offset>=0 AND source_offset+length<=4194304)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS uploads (
    id BINARY(16) NOT NULL PRIMARY KEY,
    bucket_id BINARY(16) NOT NULL,
    object_key VARBINARY(1024) NOT NULL,
    access_key VARCHAR(128) NOT NULL,
    state VARCHAR(10) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT 'active',
    metadata JSON NOT NULL DEFAULT (JSON_OBJECT()),
    public_read BOOLEAN NOT NULL DEFAULT FALSE,
    checksum_algorithm VARCHAR(16) CHARACTER SET ascii COLLATE ascii_bin,
    checksum_type VARCHAR(16) CHARACTER SET ascii COLLATE ascii_bin,
    manifest_hash VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin,
    result JSON,
    output_stream BINARY(16),
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    touched_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    durable_at DATETIME(6),
    CONSTRAINT uploads_bucket FOREIGN KEY (bucket_id) REFERENCES buckets(id),
    CONSTRAINT uploads_output FOREIGN KEY (output_stream) REFERENCES streams(id),
    CONSTRAINT uploads_key CHECK (OCTET_LENGTH(object_key) BETWEEN 1 AND 1024),
    CONSTRAINT uploads_state CHECK (state IN ('active','completing','completed','aborted')),
    INDEX uploads_listing (bucket_id,object_key,id),
    INDEX uploads_cleanup (state,touched_at,id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 /*!80000 COLLATE=utf8mb4_0900_bin */ /*M!100603 COLLATE=utf8mb4_nopad_bin */;

CREATE TABLE IF NOT EXISTS cache_pins (
    chunk_id BIGINT NOT NULL,
    pin_type VARCHAR(6) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    owner_id BINARY(16) NOT NULL,
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (chunk_id,pin_type,owner_id),
    CONSTRAINT cache_pins_chunk FOREIGN KEY (chunk_id) REFERENCES chunks(id),
    CONSTRAINT cache_pins_type CHECK (pin_type IN ('upload','pack')),
    INDEX cache_pins_owner (owner_id,chunk_id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS pack_members (
    pack_id BIGINT NOT NULL,
    ordinal INT NOT NULL,
    chunk_id BIGINT NOT NULL,
    offset_bytes BIGINT NOT NULL,
    PRIMARY KEY (pack_id,ordinal),
    UNIQUE INDEX pack_members_chunk (pack_id,chunk_id),
    CONSTRAINT members_pack FOREIGN KEY (pack_id) REFERENCES packs(id) ON DELETE CASCADE,
    CONSTRAINT members_chunk FOREIGN KEY (chunk_id) REFERENCES chunks(id),
    CONSTRAINT members_ordinal CHECK (ordinal BETWEEN 0 AND 4095),
    CONSTRAINT members_offset CHECK (offset_bytes>=0)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS integrity_packs (
    task_id BINARY(16) NOT NULL,
    pack_id BIGINT NOT NULL,
    error_code VARCHAR(64),
    PRIMARY KEY (task_id,pack_id),
    CONSTRAINT integrity_packs_task FOREIGN KEY (task_id) REFERENCES tasks(id) ON DELETE CASCADE,
    CONSTRAINT integrity_packs_pack FOREIGN KEY (pack_id) REFERENCES packs(id) ON DELETE CASCADE
) ENGINE=InnoDB;

UPDATE mokyu_meta SET schema_version=6 WHERE id=1;
