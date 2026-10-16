CREATE TABLE maintenance_previews (
    id BLOB CHECK (id IS NULL OR length(id)=16) NOT NULL PRIMARY KEY,
    actor_id BLOB CHECK (actor_id IS NULL OR length(actor_id)=16) NOT NULL,
    token_id BLOB CHECK (token_id IS NULL OR length(token_id)=16),
    action TEXT NOT NULL,
    parameters TEXT CHECK (json_valid(parameters)) NOT NULL,
    fingerprint TEXT NOT NULL,
    expires_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec')*1000000 AS INTEGER)+600000000),
    task_id BLOB CHECK (task_id IS NULL OR length(task_id)=16),
    CONSTRAINT maintenance_previews_actor FOREIGN KEY (actor_id) REFERENCES web_users(id) ON DELETE CASCADE,
    CONSTRAINT maintenance_previews_token FOREIGN KEY (token_id) REFERENCES api_tokens(id) ON DELETE CASCADE,
    CONSTRAINT maintenance_previews_task FOREIGN KEY (task_id) REFERENCES tasks(id) ON DELETE SET NULL,
    CONSTRAINT maintenance_previews_action CHECK (action IN ('unpack','sweep'))
) STRICT, WITHOUT ROWID;

CREATE INDEX maintenance_previews_owner ON maintenance_previews(actor_id,token_id,action,task_id);
CREATE INDEX maintenance_previews_expiry ON maintenance_previews(expires_at);
CREATE INDEX maintenance_previews_token_id_fk ON maintenance_previews(token_id);
CREATE INDEX maintenance_previews_task_id_fk ON maintenance_previews(task_id);

UPDATE mokyu_meta SET schema_version=15 WHERE id=1;
