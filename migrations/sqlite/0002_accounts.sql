CREATE TABLE web_users (
    id BLOB NOT NULL PRIMARY KEY CHECK(length(id)=16),
    username TEXT NOT NULL UNIQUE CHECK(octet_length(username) BETWEEN 1 AND 64),
    password_hash TEXT NOT NULL,
    enabled INTEGER NOT NULL DEFAULT 1 CHECK(enabled IN (0,1)),
    created_at INTEGER NOT NULL DEFAULT(CAST(unixepoch('subsec')*1000000 AS INTEGER)),
    display_name TEXT NOT NULL DEFAULT '' CHECK(octet_length(display_name)<=240),
    locale TEXT CHECK(locale IN ('en','zh-CN','ja')),
    theme TEXT CHECK(theme IN ('auto','light','dark')),
    avatar_email TEXT NOT NULL DEFAULT '' CHECK(octet_length(avatar_email)<=320),
    avatar_enabled INTEGER NOT NULL DEFAULT 0 CHECK(avatar_enabled IN (0,1)),
    auth_revision INTEGER NOT NULL DEFAULT 0,
    role TEXT NOT NULL DEFAULT 'member' CHECK(role IN ('admin','member')),
    authorization_revision INTEGER NOT NULL DEFAULT 0,
    must_change_password INTEGER NOT NULL DEFAULT 0 CHECK(must_change_password IN (0,1))
) STRICT, WITHOUT ROWID;

CREATE TABLE sessions (
    id BLOB NOT NULL PRIMARY KEY CHECK(length(id)=16),
    token_hash BLOB NOT NULL UNIQUE CHECK(length(token_hash)=32),
    user_id BLOB NOT NULL REFERENCES web_users(id) ON DELETE CASCADE,
    csrf_hash BLOB NOT NULL CHECK(length(csrf_hash)=32),
    expires_at INTEGER NOT NULL,
    created_at INTEGER NOT NULL DEFAULT(CAST(unixepoch('subsec')*1000000 AS INTEGER)),
    last_seen_at INTEGER NOT NULL DEFAULT(CAST(unixepoch('subsec')*1000000 AS INTEGER)),
    reauthenticated_at INTEGER NOT NULL DEFAULT(CAST(unixepoch('subsec')*1000000 AS INTEGER)),
    auth_revision INTEGER NOT NULL DEFAULT 0,
    user_agent TEXT NOT NULL DEFAULT '' CHECK(octet_length(user_agent)<=512)
) STRICT, WITHOUT ROWID;
CREATE INDEX sessions_expiry ON sessions(expires_at);
CREATE INDEX sessions_user_created ON sessions(user_id,created_at DESC,id DESC);

CREATE TABLE session_refresh_tokens (
    id BLOB NOT NULL PRIMARY KEY CHECK(length(id)=16),
    session_id BLOB NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
    family_id BLOB NOT NULL CHECK(length(family_id)=16),
    token_hash BLOB NOT NULL UNIQUE CHECK(length(token_hash)=32),
    created_at INTEGER NOT NULL DEFAULT(CAST(unixepoch('subsec')*1000000 AS INTEGER)),
    last_used_at INTEGER NOT NULL DEFAULT(CAST(unixepoch('subsec')*1000000 AS INTEGER)),
    expires_at INTEGER NOT NULL,
    revoked_at INTEGER,
    replaced_by BLOB REFERENCES session_refresh_tokens(id) ON DELETE SET NULL
) STRICT, WITHOUT ROWID;
CREATE UNIQUE INDEX refresh_active ON session_refresh_tokens(session_id) WHERE revoked_at IS NULL;
CREATE INDEX refresh_expiry ON session_refresh_tokens(revoked_at,expires_at);
CREATE INDEX refresh_session ON session_refresh_tokens(session_id);
CREATE INDEX refresh_successor ON session_refresh_tokens(replaced_by) WHERE replaced_by IS NOT NULL;

CREATE TABLE setup_tokens (
    id INTEGER PRIMARY KEY CHECK(id=1),
    token_hash BLOB NOT NULL CHECK(length(token_hash)=32),
    created_at INTEGER NOT NULL DEFAULT(CAST(unixepoch('subsec')*1000000 AS INTEGER)),
    expires_at INTEGER NOT NULL
) STRICT;

CREATE TABLE api_tokens (
    id BLOB NOT NULL PRIMARY KEY CHECK(length(id)=16),
    user_id BLOB NOT NULL REFERENCES web_users(id) ON DELETE CASCADE,
    label TEXT NOT NULL CHECK(octet_length(label) BETWEEN 1 AND 128),
    token_hash BLOB NOT NULL UNIQUE CHECK(length(token_hash)=32),
    prefix TEXT NOT NULL,
    system_access INTEGER NOT NULL DEFAULT 0 CHECK(system_access IN (0,1)),
    auth_revision INTEGER NOT NULL,
    authorization_revision INTEGER NOT NULL DEFAULT 0,
    created_at INTEGER NOT NULL DEFAULT(CAST(unixepoch('subsec')*1000000 AS INTEGER)),
    expires_at INTEGER,
    last_used_at INTEGER,
    revoked_at INTEGER
) STRICT, WITHOUT ROWID;
CREATE INDEX api_tokens_user ON api_tokens(user_id);

CREATE TABLE audit_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at INTEGER NOT NULL DEFAULT(CAST(unixepoch('subsec')*1000000 AS INTEGER)),
    finished_at INTEGER,
    actor_id BLOB,
    actor_label TEXT NOT NULL,
    token_id BLOB,
    source TEXT NOT NULL CHECK(source IN ('web','token','cli')),
    action TEXT NOT NULL,
    target TEXT NOT NULL DEFAULT '',
    project_id BLOB,
    bucket_id BLOB,
    outcome TEXT NOT NULL DEFAULT 'unknown' CHECK(outcome IN ('unknown','succeeded','failed','partial')),
    request_id TEXT,
    http_status INTEGER,
    detail TEXT NOT NULL DEFAULT '{}' CHECK(json_valid(detail))
) STRICT;
CREATE INDEX audit_actor ON audit_events(actor_id,id);
CREATE INDEX audit_security ON audit_events(actor_id,action,outcome,created_at);
CREATE INDEX audit_created ON audit_events(created_at,id);

UPDATE mokyu_meta SET schema_version=2 WHERE id=1;
