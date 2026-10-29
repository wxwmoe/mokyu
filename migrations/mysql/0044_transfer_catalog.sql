CREATE TABLE transfer_runs (
    task_id BINARY(16) PRIMARY KEY,
    created_by BINARY(16),
    options JSON NOT NULL,
    authority JSON NOT NULL,
    storage_id BINARY(16) NOT NULL,
    profile_revision BIGINT NOT NULL,
    generation BIGINT NOT NULL DEFAULT 1 CHECK (generation>0),
    phase VARCHAR(24) NOT NULL DEFAULT 'scan',
    cursor_key VARCHAR(1024) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin,
    progress JSON NOT NULL,
    CHECK (phase IN ('scan','plan','verify','publish','cancel','complete','cancelled')),
    FOREIGN KEY (task_id) REFERENCES tasks(id) ON DELETE CASCADE
);
CREATE INDEX transfer_runs_creator ON transfer_runs(created_by,task_id);

CREATE TABLE transfer_items (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    task_id BINARY(16) NOT NULL,
    source_key VARCHAR(1024) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    target_key VARCHAR(1024) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    source_version BIGINT,
    source_pin BIGINT,
    target_version BIGINT,
    candidate BIGINT,
    output_version BIGINT,
    snapshot JSON NOT NULL,
    state VARCHAR(24) NOT NULL DEFAULT 'planning',
    position_bytes BIGINT NOT NULL DEFAULT 0 CHECK (position_bytes>=0),
    error VARCHAR(24),
    CHECK (state IN ('planning','planned','copying','completed','skipped','conflict','cancelled')),
    FOREIGN KEY (task_id) REFERENCES transfer_runs(task_id) ON DELETE CASCADE,
    FOREIGN KEY (source_pin) REFERENCES streams(id),
    FOREIGN KEY (candidate) REFERENCES streams(id)
);
CREATE INDEX transfer_items_work ON transfer_items(task_id,state,id);
CREATE INDEX transfer_items_source_pin ON transfer_items(source_pin);
CREATE INDEX transfer_items_candidate ON transfer_items(candidate);

CREATE TABLE transfer_sources (
    task_id BINARY(16) NOT NULL,
    source_type VARCHAR(24) NOT NULL CHECK (source_type IN ('raw','chunk','pack')),
    source_id BIGINT NOT NULL,
    generation BIGINT NOT NULL DEFAULT 0 CHECK (generation>=0),
    snapshot JSON,
    PRIMARY KEY (task_id,source_type,source_id),
    FOREIGN KEY (task_id) REFERENCES transfer_runs(task_id) ON DELETE CASCADE
);
CREATE INDEX transfer_sources_protection ON transfer_sources(source_type,source_id,task_id);

UPDATE mokyu_meta SET schema_version=44 WHERE id=1;
