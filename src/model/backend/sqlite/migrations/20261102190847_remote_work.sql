ALTER TABLE mokyu_meta ADD COLUMN remote_work INTEGER NOT NULL DEFAULT 0 CHECK(remote_work IN (0,1));
CREATE TABLE node_work (
    node_id BLOB NOT NULL REFERENCES nodes(id),
    operation_id BLOB NOT NULL CHECK(length(operation_id)=16),
    boot_id BLOB NOT NULL CHECK(length(boot_id)=16),
    fence BLOB NOT NULL CHECK(length(fence)=16),
    request_hash BLOB NOT NULL CHECK(length(request_hash)=32),
    task_id BLOB REFERENCES tasks(id) ON DELETE SET NULL,
    token BLOB NOT NULL CHECK(length(token)=16),
    plan TEXT CHECK(plan IS NULL OR json_valid(plan)),
    materials_encrypted BLOB,
    result_hash BLOB CHECK(result_hash IS NULL OR length(result_hash)=32),
    expires_at INTEGER NOT NULL,
    PRIMARY KEY (node_id,operation_id)
) STRICT, WITHOUT ROWID;
CREATE INDEX node_work_expiry ON node_work(expires_at);
CREATE INDEX node_work_task ON node_work(task_id) WHERE task_id IS NOT NULL;

UPDATE mokyu_meta SET schema_version = 20261102190847;
