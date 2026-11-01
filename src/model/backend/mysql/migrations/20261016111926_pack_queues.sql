CREATE TABLE IF NOT EXISTS pack_inputs (
    task_id BINARY(16) NOT NULL,
    chunk_id BIGINT NOT NULL,
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (task_id,chunk_id),
    UNIQUE INDEX pack_inputs_chunk (chunk_id),
    CONSTRAINT pack_inputs_task FOREIGN KEY (task_id) REFERENCES tasks(id) ON DELETE CASCADE,
    CONSTRAINT pack_inputs_source FOREIGN KEY (chunk_id) REFERENCES chunks(id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS pack_maintenance (
    stream_id BINARY(16) NOT NULL PRIMARY KEY,
    progress_cursor BIGINT NOT NULL DEFAULT 0,
    generation BIGINT NOT NULL DEFAULT 1,
    next_check_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    reason VARCHAR(6) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT 'pack',
    CONSTRAINT pack_maintenance_stream FOREIGN KEY (stream_id) REFERENCES streams(id) ON DELETE CASCADE,
    CONSTRAINT pack_maintenance_reason CHECK (reason IN ('pack','repack')),
    CONSTRAINT pack_maintenance_progress CHECK (progress_cursor>=0 AND generation>0),
    INDEX pack_maintenance_due (reason,next_check_at,stream_id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS pack_changes (
    pack_id BIGINT NOT NULL PRIMARY KEY,
    generation BIGINT NOT NULL DEFAULT 1,
    next_check_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    reason VARCHAR(7) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT 'reuse',
    CONSTRAINT pack_changes_pack FOREIGN KEY (pack_id) REFERENCES packs(id) ON DELETE CASCADE,
    CONSTRAINT pack_changes_reason CHECK (reason IN ('reuse','reclaim')),
    CONSTRAINT pack_changes_generation CHECK (generation>0),
    INDEX pack_changes_due (reason,next_check_at,pack_id)
) ENGINE=InnoDB;

UPDATE mokyu_meta SET schema_version=11 WHERE id=1;
