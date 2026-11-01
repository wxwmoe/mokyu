CREATE TABLE IF NOT EXISTS web_users (
    id BINARY(16) NOT NULL PRIMARY KEY,
    username VARCHAR(64) NOT NULL UNIQUE,
    password_hash VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    display_name VARCHAR(240) NOT NULL DEFAULT '',
    locale VARCHAR(5) CHARACTER SET ascii COLLATE ascii_bin,
    theme VARCHAR(5) CHARACTER SET ascii COLLATE ascii_bin,
    avatar_email VARCHAR(320) NOT NULL DEFAULT '',
    avatar_enabled BOOLEAN NOT NULL DEFAULT FALSE,
    auth_revision BIGINT NOT NULL DEFAULT 0,
    role VARCHAR(6) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT 'member',
    authorization_revision BIGINT NOT NULL DEFAULT 0,
    must_change_password BOOLEAN NOT NULL DEFAULT FALSE,
    CONSTRAINT web_users_username CHECK (OCTET_LENGTH(username) BETWEEN 1 AND 64),
    CONSTRAINT web_users_display_name CHECK (OCTET_LENGTH(display_name)<=240),
    CONSTRAINT web_users_avatar_email CHECK (OCTET_LENGTH(avatar_email)<=320),
    CONSTRAINT web_users_locale CHECK (locale IN ('en','zh-CN','ja')),
    CONSTRAINT web_users_theme CHECK (theme IN ('auto','light','dark')),
    CONSTRAINT web_users_role CHECK (role IN ('admin','member'))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 /*!80000 COLLATE=utf8mb4_0900_bin */ /*M!100603 COLLATE=utf8mb4_nopad_bin */;

CREATE TABLE IF NOT EXISTS sessions (
    id BINARY(16) NOT NULL PRIMARY KEY,
    token_hash BINARY(32) NOT NULL UNIQUE,
    user_id BINARY(16) NOT NULL,
    csrf_hash BINARY(32) NOT NULL,
    expires_at DATETIME(6) NOT NULL,
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    last_seen_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    reauthenticated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    auth_revision BIGINT NOT NULL DEFAULT 0,
    user_agent VARCHAR(512) NOT NULL DEFAULT '',
    CONSTRAINT sessions_user_agent CHECK (OCTET_LENGTH(user_agent)<=512),
    CONSTRAINT sessions_user FOREIGN KEY (user_id) REFERENCES web_users(id) ON DELETE CASCADE,
    INDEX sessions_expiry (expires_at),
    INDEX sessions_user_created (user_id,created_at DESC,id DESC)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 /*!80000 COLLATE=utf8mb4_0900_bin */ /*M!100603 COLLATE=utf8mb4_nopad_bin */;

CREATE TABLE IF NOT EXISTS session_refresh_tokens (
    id BINARY(16) NOT NULL PRIMARY KEY,
    session_id BINARY(16) NOT NULL,
    family_id BINARY(16) NOT NULL,
    token_hash BINARY(32) NOT NULL UNIQUE,
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    last_used_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    expires_at DATETIME(6) NOT NULL,
    revoked_at DATETIME(6),
    replaced_by BINARY(16),
    active_session BINARY(16) GENERATED ALWAYS AS (IF(revoked_at IS NULL,session_id,NULL)) VIRTUAL,
    CONSTRAINT refresh_session FOREIGN KEY (session_id) REFERENCES sessions(id) ON DELETE CASCADE,
    CONSTRAINT refresh_successor FOREIGN KEY (replaced_by) REFERENCES session_refresh_tokens(id) ON DELETE SET NULL,
    UNIQUE INDEX refresh_active (active_session),
    INDEX refresh_expiry (revoked_at,expires_at)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS setup_tokens (
    id TINYINT UNSIGNED NOT NULL PRIMARY KEY,
    token_hash BINARY(32) NOT NULL,
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    expires_at DATETIME(6) NOT NULL,
    CONSTRAINT setup_singleton CHECK (id=1)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS api_tokens (
    id BINARY(16) NOT NULL PRIMARY KEY,
    user_id BINARY(16) NOT NULL,
    label VARCHAR(128) NOT NULL,
    token_hash BINARY(32) NOT NULL UNIQUE,
    prefix VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    system_access BOOLEAN NOT NULL DEFAULT FALSE,
    auth_revision BIGINT NOT NULL,
    authorization_revision BIGINT NOT NULL DEFAULT 0,
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    expires_at DATETIME(6),
    last_used_at DATETIME(6),
    revoked_at DATETIME(6),
    CONSTRAINT api_tokens_label CHECK (OCTET_LENGTH(label) BETWEEN 1 AND 128),
    CONSTRAINT api_tokens_user FOREIGN KEY (user_id) REFERENCES web_users(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 /*!80000 COLLATE=utf8mb4_0900_bin */ /*M!100603 COLLATE=utf8mb4_nopad_bin */;

CREATE TABLE IF NOT EXISTS audit_events (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    finished_at DATETIME(6),
    actor_id BINARY(16),
    actor_label VARCHAR(64) NOT NULL,
    token_id BINARY(16),
    source VARCHAR(5) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    action VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    target TEXT NOT NULL DEFAULT (''),
    project_id BINARY(16),
    bucket_id BINARY(16),
    outcome VARCHAR(9) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT 'unknown',
    request_id VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin,
    http_status INT,
    detail JSON NOT NULL DEFAULT (JSON_OBJECT()),
    CONSTRAINT audit_source CHECK (source IN ('web','token','cli')),
    CONSTRAINT audit_outcome CHECK (outcome IN ('unknown','succeeded','failed','partial')),
    INDEX audit_actor (actor_id,id),
    INDEX audit_security (actor_id,action,outcome,created_at),
    INDEX audit_created (created_at,id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 /*!80000 COLLATE=utf8mb4_0900_bin */ /*M!100603 COLLATE=utf8mb4_nopad_bin */;

UPDATE mokyu_meta SET schema_version=2 WHERE id=1;
