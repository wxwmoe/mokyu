CREATE TABLE external_buckets (
    id BINARY(16) NOT NULL PRIMARY KEY,
    project_id BINARY(16) NOT NULL,
    name VARCHAR(128) NOT NULL,
    connection JSON NOT NULL,
    revision BIGINT NOT NULL DEFAULT 1,
    access_encrypted BLOB NOT NULL,
    secret_encrypted BLOB NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    UNIQUE KEY external_buckets_name (project_id,name),
    UNIQUE KEY external_buckets_project_id (id,project_id),
    CONSTRAINT external_buckets_project FOREIGN KEY (project_id) REFERENCES projects(id),
    CONSTRAINT external_buckets_name_size CHECK (OCTET_LENGTH(name) BETWEEN 1 AND 128),
    CONSTRAINT external_buckets_connection CHECK (JSON_TYPE(connection)='OBJECT'),
    CONSTRAINT external_buckets_revision CHECK (revision>0)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 /*!80000 COLLATE=utf8mb4_0900_bin */ /*M!100603 COLLATE=utf8mb4_nopad_bin */;

CREATE TABLE member_external_grants (
    user_id BINARY(16) NOT NULL,
    project_id BINARY(16) NOT NULL,
    bucket_id BINARY(16) NOT NULL,
    actions SMALLINT UNSIGNED NOT NULL,
    PRIMARY KEY (user_id,bucket_id),
    CONSTRAINT member_external_actions CHECK (actions<=15),
    CONSTRAINT member_external_member FOREIGN KEY (user_id,project_id) REFERENCES project_members(user_id,project_id) ON DELETE CASCADE,
    CONSTRAINT member_external_bucket FOREIGN KEY (bucket_id,project_id) REFERENCES external_buckets(id,project_id) ON DELETE CASCADE
) ENGINE=InnoDB;

CREATE TABLE token_external_grants (
    token_id BINARY(16) NOT NULL,
    bucket_id BINARY(16) NOT NULL,
    actions SMALLINT UNSIGNED NOT NULL,
    PRIMARY KEY (token_id,bucket_id),
    CONSTRAINT token_external_actions CHECK (actions<=15),
    CONSTRAINT token_external_token FOREIGN KEY (token_id) REFERENCES api_tokens(id) ON DELETE CASCADE,
    CONSTRAINT token_external_bucket FOREIGN KEY (bucket_id) REFERENCES external_buckets(id) ON DELETE CASCADE
) ENGINE=InnoDB;

CREATE TABLE external_tests (
    id BINARY(16) NOT NULL PRIMARY KEY,
    external_id BINARY(16),
    revision BIGINT NOT NULL,
    connection_encrypted MEDIUMBLOB,
    node_id BINARY(16),
    attempts INT NOT NULL DEFAULT 0,
    report JSON NOT NULL DEFAULT (JSON_OBJECT()),
    cleanup_required BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    KEY external_tests_bucket (external_id,created_at DESC,id DESC),
    CONSTRAINT external_tests_task FOREIGN KEY (id) REFERENCES tasks(id) ON DELETE CASCADE,
    CONSTRAINT external_tests_bucket_fk FOREIGN KEY (external_id) REFERENCES external_buckets(id) ON DELETE SET NULL,
    CONSTRAINT external_tests_revision CHECK (revision>0),
    CONSTRAINT external_tests_attempts CHECK (attempts>=0),
    CONSTRAINT external_tests_report CHECK (JSON_TYPE(report)='OBJECT')
) ENGINE=InnoDB;

CREATE VIEW user_external_access AS
SELECT m.user_id,b.id AS bucket_id,
    (CASE m.role WHEN 'reader' THEN 3 WHEN 'writer' THEN 15 WHEN 'maintainer' THEN 15 ELSE 0 END)
    & IF(m.scope='all',15,COALESCE(g.actions,0)) AS actions
FROM project_members m JOIN external_buckets b ON b.project_id=m.project_id
LEFT JOIN member_external_grants g ON g.user_id=m.user_id AND g.bucket_id=b.id;

CREATE VIEW token_external_access AS
SELECT t.id AS token_id,g.bucket_id,
    g.actions & IF(u.role='admin',15,COALESCE(a.actions,0)) AS actions
FROM api_tokens t JOIN web_users u ON u.id=t.user_id
JOIN token_external_grants g ON g.token_id=t.id
LEFT JOIN user_external_access a ON a.user_id=t.user_id AND a.bucket_id=g.bucket_id;

UPDATE mokyu_meta SET schema_version=43 WHERE id=1;
