CREATE TABLE pack_read_shapes (
    stream_id bigint NOT NULL,
    start_bytes BIGINT NOT NULL,
    end_bytes BIGINT NOT NULL,
    requests BIGINT NOT NULL,
    observed_hour BIGINT NOT NULL,
    saved_gets BIGINT NOT NULL,
    PRIMARY KEY (stream_id, start_bytes, end_bytes, observed_hour),
    FOREIGN KEY (stream_id) REFERENCES streams(id) ON DELETE CASCADE,
    CHECK (start_bytes >= 0 AND end_bytes > start_bytes AND requests > 0 AND saved_gets >= 0)
) ENGINE=InnoDB;
CREATE INDEX pack_shapes_expiry ON pack_read_shapes(observed_hour);

CREATE TABLE pack_repack_queue (
    task_id binary(16) NOT NULL,
    stream_id bigint NOT NULL,
    saved_gets BIGINT NOT NULL,
    PRIMARY KEY (task_id,stream_id),
    FOREIGN KEY (task_id) REFERENCES tasks(id) ON DELETE CASCADE
) ENGINE=InnoDB;
CREATE INDEX pack_repack_priority ON pack_repack_queue(task_id,saved_gets DESC,stream_id);

DELETE FROM pack_changes WHERE reason='reuse';
DELETE FROM pack_maintenance WHERE reason='repack';
DELETE FROM maintenance_controls WHERE policy IN ('reuse','range');

UPDATE mokyu_meta SET schema_version = 20261108051001;
