CREATE TABLE IF NOT EXISTS projects (
    id BINARY(16) NOT NULL PRIMARY KEY,
    name VARCHAR(128) NOT NULL UNIQUE,
    description VARCHAR(2000) NOT NULL DEFAULT '',
    builtin BOOLEAN NOT NULL DEFAULT FALSE,
    allow_bucket_create BOOLEAN NOT NULL DEFAULT FALSE,
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    CONSTRAINT projects_name CHECK (OCTET_LENGTH(name) BETWEEN 1 AND 128),
    CONSTRAINT projects_description CHECK (OCTET_LENGTH(description)<=2000)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 /*!80000 COLLATE=utf8mb4_0900_bin */ /*M!100603 COLLATE=utf8mb4_nopad_bin */;

INSERT INTO projects(id,name,builtin) VALUES(0x00000000000000000000000000000001,'Default',TRUE)
ON DUPLICATE KEY UPDATE id=projects.id;

CREATE TABLE IF NOT EXISTS buckets (
    id BINARY(16) NOT NULL PRIMARY KEY,
    name VARCHAR(63) NOT NULL UNIQUE,
    state VARCHAR(7) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT 'active',
    cors JSON NOT NULL DEFAULT (JSON_ARRAY()),
    website_enabled BOOLEAN NOT NULL DEFAULT FALSE,
    index_document TEXT NOT NULL DEFAULT ('index.html'),
    error_document TEXT NOT NULL DEFAULT ('404.html'),
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    project_id BINARY(16) NOT NULL,
    settings_revision BIGINT NOT NULL DEFAULT 0,
    uploads_paused BOOLEAN NOT NULL DEFAULT FALSE,
    public_base_url TEXT NOT NULL DEFAULT (''),
    storage_id BINARY(16) NOT NULL,
    UNIQUE INDEX buckets_id_project (id,project_id),
    CONSTRAINT buckets_state CHECK (state IN ('active','purging')),
    CONSTRAINT buckets_project FOREIGN KEY (project_id) REFERENCES projects(id),
    CONSTRAINT buckets_storage FOREIGN KEY (storage_id) REFERENCES storages(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 /*!80000 COLLATE=utf8mb4_0900_bin */ /*M!100603 COLLATE=utf8mb4_nopad_bin */;

CREATE TABLE IF NOT EXISTS project_members (
    user_id BINARY(16) NOT NULL,
    project_id BINARY(16) NOT NULL,
    role VARCHAR(10) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    scope VARCHAR(8) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    PRIMARY KEY (user_id,project_id),
    CONSTRAINT project_members_user FOREIGN KEY (user_id) REFERENCES web_users(id) ON DELETE CASCADE,
    CONSTRAINT project_members_project FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE CASCADE,
    CONSTRAINT project_members_role CHECK (role IN ('reader','writer','maintainer')),
    CONSTRAINT project_members_scope CHECK (scope IN ('all','selected'))
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS member_bucket_grants (
    user_id BINARY(16) NOT NULL,
    project_id BINARY(16) NOT NULL,
    bucket_id BINARY(16) NOT NULL,
    actions SMALLINT UNSIGNED NOT NULL,
    PRIMARY KEY (user_id,bucket_id),
    CONSTRAINT member_grants_actions CHECK (actions<=127),
    CONSTRAINT member_grants_member FOREIGN KEY (user_id,project_id) REFERENCES project_members(user_id,project_id) ON DELETE CASCADE,
    CONSTRAINT member_grants_bucket FOREIGN KEY (bucket_id,project_id) REFERENCES buckets(id,project_id) ON DELETE CASCADE
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS s3_credentials (
    access_key_id VARCHAR(128) NOT NULL PRIMARY KEY,
    secret_encrypted BLOB NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    project_id BINARY(16) NOT NULL,
    authorization_revision BIGINT NOT NULL DEFAULT 0,
    label VARCHAR(128) NOT NULL DEFAULT '',
    expires_at DATETIME(6),
    last_used_at DATETIME(6),
    created_by BINARY(16),
    CONSTRAINT s3_credentials_label CHECK (OCTET_LENGTH(label)<=128),
    CONSTRAINT s3_credentials_project FOREIGN KEY (project_id) REFERENCES projects(id),
    CONSTRAINT s3_credentials_creator FOREIGN KEY (created_by) REFERENCES web_users(id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 /*!80000 COLLATE=utf8mb4_0900_bin */ /*M!100603 COLLATE=utf8mb4_nopad_bin */;

CREATE TABLE IF NOT EXISTS s3_credential_grants (
    access_key VARCHAR(128) CHARACTER SET utf8mb4 /*!80000 COLLATE utf8mb4_0900_bin */ /*M!100603 COLLATE utf8mb4_nopad_bin */ NOT NULL,
    bucket_id BINARY(16) NOT NULL,
    actions SMALLINT UNSIGNED NOT NULL,
    PRIMARY KEY (access_key,bucket_id),
    CONSTRAINT credential_grants_actions CHECK (actions<=127),
    CONSTRAINT credential_grants_credential FOREIGN KEY (access_key) REFERENCES s3_credentials(access_key_id) ON DELETE CASCADE,
    CONSTRAINT credential_grants_bucket FOREIGN KEY (bucket_id) REFERENCES buckets(id) ON DELETE CASCADE
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS token_bucket_grants (
    token_id BINARY(16) NOT NULL,
    bucket_id BINARY(16) NOT NULL,
    actions SMALLINT UNSIGNED NOT NULL,
    PRIMARY KEY (token_id,bucket_id),
    CONSTRAINT token_grants_actions CHECK (actions<=127),
    CONSTRAINT token_grants_token FOREIGN KEY (token_id) REFERENCES api_tokens(id) ON DELETE CASCADE,
    CONSTRAINT token_grants_bucket FOREIGN KEY (bucket_id) REFERENCES buckets(id) ON DELETE CASCADE
) ENGINE=InnoDB;

CREATE OR REPLACE VIEW user_bucket_access AS
SELECT m.user_id,b.id AS bucket_id,
    (CASE m.role WHEN 'reader' THEN 67 WHEN 'writer' THEN 95 WHEN 'maintainer' THEN 127 ELSE 0 END)
    & IF(m.scope='all',127,COALESCE(g.actions,0)) AS actions
FROM project_members m JOIN buckets b ON b.project_id=m.project_id
LEFT JOIN member_bucket_grants g ON g.user_id=m.user_id AND g.bucket_id=b.id;

CREATE OR REPLACE VIEW token_bucket_access AS
SELECT t.id AS token_id,g.bucket_id,
    g.actions & IF(u.role='admin',127,COALESCE(a.actions,0)) AS actions
FROM api_tokens t JOIN web_users u ON u.id=t.user_id
JOIN token_bucket_grants g ON g.token_id=t.id
LEFT JOIN user_bucket_access a ON a.user_id=t.user_id AND a.bucket_id=g.bucket_id;

UPDATE mokyu_meta SET schema_version=4 WHERE id=1;
