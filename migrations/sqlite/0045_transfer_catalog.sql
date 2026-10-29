CREATE TABLE transfer_runs (
    task_id BLOB PRIMARY KEY REFERENCES tasks(id) ON DELETE CASCADE,
    created_by BLOB,
    options TEXT NOT NULL,
    authority TEXT NOT NULL,
    storage_id BLOB NOT NULL,
    profile_revision BIGINT NOT NULL,
    generation BIGINT NOT NULL DEFAULT 1 CHECK (generation>0),
    phase TEXT NOT NULL DEFAULT 'scan',
    cursor_key TEXT,
    progress TEXT NOT NULL,
    CHECK (phase IN ('scan','plan','verify','publish','cancel','complete','cancelled'))
);
CREATE INDEX transfer_runs_creator ON transfer_runs(created_by,task_id);

CREATE TABLE transfer_items (
    id INTEGER PRIMARY KEY,
    task_id BLOB NOT NULL REFERENCES transfer_runs(task_id) ON DELETE CASCADE,
    source_key TEXT NOT NULL,
    target_key TEXT NOT NULL,
    source_version INTEGER,
    source_pin INTEGER REFERENCES streams(id),
    target_version INTEGER,
    candidate INTEGER REFERENCES streams(id),
    output_version INTEGER,
    snapshot TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'planning',
    position_bytes BIGINT NOT NULL DEFAULT 0 CHECK (position_bytes>=0),
    error TEXT,
    CHECK (state IN ('planning','planned','copying','completed','skipped','conflict','cancelled'))
);
CREATE INDEX transfer_items_work ON transfer_items(task_id,state,id);
CREATE INDEX transfer_items_source_pin ON transfer_items(source_pin);
CREATE INDEX transfer_items_candidate ON transfer_items(candidate);

CREATE TABLE transfer_sources (
    task_id BLOB NOT NULL REFERENCES transfer_runs(task_id) ON DELETE CASCADE,
    source_type TEXT NOT NULL CHECK (source_type IN ('raw','chunk','pack')),
    source_id INTEGER NOT NULL,
    generation BIGINT NOT NULL DEFAULT 0 CHECK (generation>=0),
    snapshot TEXT,
    PRIMARY KEY (task_id,source_type,source_id)
);
CREATE INDEX transfer_sources_protection ON transfer_sources(source_type,source_id,task_id);

UPDATE mokyu_meta SET schema_version=45 WHERE id=1;
