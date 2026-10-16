CREATE TABLE pack_inputs (
    task_id BLOB CHECK (task_id IS NULL OR length(task_id)=16) NOT NULL,
    chunk_id INTEGER NOT NULL,
    created_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec')*1000000 AS INTEGER)),
    PRIMARY KEY (task_id,chunk_id),
    CONSTRAINT pack_inputs_task FOREIGN KEY (task_id) REFERENCES tasks(id) ON DELETE CASCADE,
    CONSTRAINT pack_inputs_source FOREIGN KEY (chunk_id) REFERENCES chunks(id)
) STRICT, WITHOUT ROWID;

CREATE UNIQUE INDEX pack_inputs_chunk ON pack_inputs(chunk_id);

CREATE TABLE pack_maintenance (
    stream_id BLOB CHECK (stream_id IS NULL OR length(stream_id)=16) NOT NULL PRIMARY KEY,
    progress_cursor INTEGER NOT NULL DEFAULT 0,
    generation INTEGER NOT NULL DEFAULT 1,
    next_check_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec')*1000000 AS INTEGER)),
    updated_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec')*1000000 AS INTEGER)),
    reason TEXT NOT NULL DEFAULT 'pack',
    CONSTRAINT pack_maintenance_stream FOREIGN KEY (stream_id) REFERENCES streams(id) ON DELETE CASCADE,
    CONSTRAINT pack_maintenance_reason CHECK (reason IN ('pack','repack')),
    CONSTRAINT pack_maintenance_progress CHECK (progress_cursor>=0 AND generation>0)
) STRICT, WITHOUT ROWID;

CREATE INDEX pack_maintenance_due ON pack_maintenance(reason,next_check_at,stream_id);

CREATE TABLE pack_changes (
    pack_id INTEGER NOT NULL PRIMARY KEY,
    generation INTEGER NOT NULL DEFAULT 1,
    next_check_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec')*1000000 AS INTEGER)),
    reason TEXT NOT NULL DEFAULT 'reuse',
    CONSTRAINT pack_changes_pack FOREIGN KEY (pack_id) REFERENCES packs(id) ON DELETE CASCADE,
    CONSTRAINT pack_changes_reason CHECK (reason IN ('reuse','reclaim')),
    CONSTRAINT pack_changes_generation CHECK (generation>0)
) STRICT, WITHOUT ROWID;

CREATE INDEX pack_changes_due ON pack_changes(reason,next_check_at,pack_id);

UPDATE mokyu_meta SET schema_version=12 WHERE id=1;
