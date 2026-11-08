CREATE TABLE pack_read_shapes (
    stream_id INTEGER NOT NULL REFERENCES streams(id) ON DELETE CASCADE,
    start_bytes INTEGER NOT NULL,
    end_bytes INTEGER NOT NULL,
    requests INTEGER NOT NULL,
    observed_hour INTEGER NOT NULL,
    saved_gets INTEGER NOT NULL,
    PRIMARY KEY (stream_id, start_bytes, end_bytes, observed_hour),
    CHECK (start_bytes >= 0 AND end_bytes > start_bytes AND requests > 0 AND saved_gets >= 0)
) STRICT, WITHOUT ROWID;
CREATE INDEX pack_shapes_expiry ON pack_read_shapes(observed_hour);

CREATE TABLE pack_repack_queue (
    task_id BLOB NOT NULL,
    stream_id INTEGER NOT NULL,
    saved_gets INTEGER NOT NULL,
    PRIMARY KEY (task_id,stream_id),
    FOREIGN KEY (task_id) REFERENCES tasks(id) ON DELETE CASCADE
) STRICT, WITHOUT ROWID;
CREATE INDEX pack_repack_priority ON pack_repack_queue(task_id,saved_gets DESC,stream_id);

DELETE FROM pack_changes WHERE reason='reuse';
DELETE FROM pack_maintenance WHERE reason='repack';
DELETE FROM maintenance_controls WHERE policy IN ('reuse','range');

UPDATE mokyu_meta SET schema_version = 20261108051001;
