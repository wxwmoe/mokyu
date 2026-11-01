CREATE TABLE IF NOT EXISTS mokyu_meta (
    id TINYINT UNSIGNED NOT NULL PRIMARY KEY,
    schema_version INT NOT NULL,
    deployment_id BINARY(16) NOT NULL,
    deployment_key_fingerprint BINARY(32),
    gc_paused BOOLEAN NOT NULL DEFAULT FALSE,
    maintenance BOOLEAN NOT NULL DEFAULT FALSE,
    project_management BOOLEAN NOT NULL DEFAULT FALSE,
    pack_creation_paused BOOLEAN NOT NULL DEFAULT FALSE,
    CONSTRAINT mokyu_meta_singleton CHECK (id=1)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS runtime_settings (
    section VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL PRIMARY KEY,
    value JSON NOT NULL,
    revision BIGINT NOT NULL DEFAULT 1,
    updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    is_default BOOLEAN NOT NULL DEFAULT TRUE,
    CONSTRAINT runtime_settings_revision CHECK (revision>0)
) ENGINE=InnoDB;

INSERT IGNORE INTO mokyu_meta(id,schema_version,deployment_id)
VALUES(1,1,UNHEX(REPLACE(UUID(),'-','')));
