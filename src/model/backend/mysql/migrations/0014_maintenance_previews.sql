CREATE TABLE IF NOT EXISTS maintenance_previews (
    id BINARY(16) NOT NULL PRIMARY KEY,
    actor_id BINARY(16) NOT NULL,
    token_id BINARY(16),
    action VARCHAR(6) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    parameters JSON NOT NULL,
    fingerprint CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    expires_at DATETIME(6) NOT NULL DEFAULT (CURRENT_TIMESTAMP(6)+INTERVAL 10 MINUTE),
    task_id BINARY(16),
    CONSTRAINT maintenance_previews_actor FOREIGN KEY (actor_id) REFERENCES web_users(id) ON DELETE CASCADE,
    CONSTRAINT maintenance_previews_token FOREIGN KEY (token_id) REFERENCES api_tokens(id) ON DELETE CASCADE,
    CONSTRAINT maintenance_previews_task FOREIGN KEY (task_id) REFERENCES tasks(id) ON DELETE SET NULL,
    CONSTRAINT maintenance_previews_action CHECK (action IN ('unpack','sweep')),
    INDEX maintenance_previews_owner (actor_id,token_id,action,task_id),
    INDEX maintenance_previews_expiry (expires_at)
) ENGINE=InnoDB;

UPDATE mokyu_meta SET schema_version=14 WHERE id=1;
