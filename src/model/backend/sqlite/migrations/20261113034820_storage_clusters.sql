CREATE TABLE storage_cluster_lock (id INTEGER PRIMARY KEY CHECK(id=1)) STRICT;
INSERT INTO storage_cluster_lock VALUES(1);
CREATE TABLE storage_clusters (
    id BLOB PRIMARY KEY CHECK(length(id)=16),
    name TEXT NOT NULL UNIQUE CHECK(length(CAST(name AS BLOB)) BETWEEN 1 AND 128),
    revision INTEGER NOT NULL CHECK(revision>0),
    policy TEXT NOT NULL CHECK(json_valid(policy)),
    manifest TEXT NOT NULL CHECK(json_valid(manifest))
) STRICT, WITHOUT ROWID;
CREATE TABLE storage_cluster_history (
    cluster_id BLOB NOT NULL REFERENCES storage_clusters(id),
    revision INTEGER NOT NULL CHECK(revision>0),
    manifest TEXT NOT NULL CHECK(json_valid(manifest)),
    digest TEXT NOT NULL CHECK(length(digest)=64),
    PRIMARY KEY(cluster_id,revision)
) STRICT, WITHOUT ROWID;
CREATE TABLE storage_devices (
    media_id BLOB PRIMARY KEY CHECK(length(media_id)=16),
    node_id BLOB NOT NULL UNIQUE REFERENCES nodes(id),
    cluster_id BLOB REFERENCES storage_clusters(id),
    observed_at INTEGER NOT NULL,
    announcement TEXT NOT NULL CHECK(json_valid(announcement))
) STRICT, WITHOUT ROWID;

PRAGMA legacy_alter_table=ON;

CREATE TABLE nodes_new (
    id BLOB NOT NULL PRIMARY KEY CHECK(length(id)=16),
    name TEXT NOT NULL UNIQUE,
    role TEXT NOT NULL CHECK(role IN ('control','data','worker','node','storage')),
    enabled INTEGER NOT NULL DEFAULT 1 CHECK(enabled IN (0,1)),
    credential_generation INTEGER NOT NULL CHECK(credential_generation>0),
    credential_encrypted BLOB,
    run_state TEXT CHECK(run_state IS NULL OR json_valid(run_state)),
    report TEXT CHECK(report IS NULL OR json_valid(report)),
    created_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec')*1000000 AS INTEGER)),
    last_seen_at INTEGER,
    reported_at INTEGER
) STRICT, WITHOUT ROWID;
INSERT INTO nodes_new SELECT * FROM nodes;
DROP TABLE nodes;
ALTER TABLE nodes_new RENAME TO nodes;

CREATE TABLE notification_consumers_new (
    node_id BLOB NOT NULL PRIMARY KEY,
    generation BLOB NOT NULL,
    role TEXT NOT NULL CHECK (role IN ('control','data','worker','node','storage')),
    expires_at INTEGER NOT NULL,
    FOREIGN KEY (node_id) REFERENCES nodes(id) ON DELETE CASCADE
) STRICT, WITHOUT ROWID;
INSERT INTO notification_consumers_new SELECT * FROM notification_consumers;
DROP TABLE notification_consumers;
ALTER TABLE notification_consumers_new RENAME TO notification_consumers;

CREATE TABLE storages_new (
    id BLOB CHECK (id IS NULL OR length(id)=16) NOT NULL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    type TEXT NOT NULL,
    cluster_id BLOB REFERENCES storage_clusters(id) CHECK (cluster_id IS NULL OR length(cluster_id)=16),
    path TEXT,
    endpoint_url TEXT,
    remote_bucket_name TEXT,
    region TEXT,
    prefix TEXT NOT NULL DEFAULT (''),
    access_key_encrypted BLOB,
    secret_key_encrypted BLOB,
    identity_marker TEXT CHECK (json_valid(identity_marker)) NOT NULL,
    enabled INTEGER CHECK (enabled IN (0,1)) NOT NULL DEFAULT TRUE,
    block_key_id BLOB CHECK (block_key_id IS NULL OR length(block_key_id)=16),
    created_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec')*1000000 AS INTEGER)),
    updated_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec')*1000000 AS INTEGER)),
    CONSTRAINT storages_name CHECK (length(CAST(name AS BLOB)) BETWEEN 1 AND 128),
    CONSTRAINT storages_type CHECK (type IN ('local','s3','mokyu')),
    CONSTRAINT storages_connection CHECK (
        (type='local' AND cluster_id IS NULL AND path IS NOT NULL AND endpoint_url IS NULL AND remote_bucket_name IS NULL AND region IS NULL AND access_key_encrypted IS NULL AND secret_key_encrypted IS NULL)
        OR (type='s3' AND cluster_id IS NULL AND path IS NULL AND endpoint_url IS NOT NULL AND remote_bucket_name IS NOT NULL AND region IS NOT NULL AND access_key_encrypted IS NOT NULL AND secret_key_encrypted IS NOT NULL)
        OR (type='mokyu' AND cluster_id IS NOT NULL AND path IS NULL AND endpoint_url IS NULL AND remote_bucket_name IS NULL AND region IS NULL AND access_key_encrypted IS NULL AND secret_key_encrypted IS NULL AND prefix='')
    ),
    CONSTRAINT storages_cluster_connection CHECK ((type='mokyu')=(cluster_id IS NOT NULL)),
    CONSTRAINT storages_key FOREIGN KEY (block_key_id) REFERENCES managed_keys(id)
) STRICT, WITHOUT ROWID;
INSERT INTO storages_new(id,name,type,path,endpoint_url,remote_bucket_name,region,prefix,access_key_encrypted,secret_key_encrypted,identity_marker,enabled,block_key_id,created_at,updated_at) SELECT id,name,type,path,endpoint_url,remote_bucket_name,region,prefix,access_key_encrypted,secret_key_encrypted,identity_marker,enabled,block_key_id,created_at,updated_at FROM storages;
DROP TABLE storages;
ALTER TABLE storages_new RENAME TO storages;

CREATE UNIQUE INDEX nodes_active_control ON nodes(role) WHERE role='control';

CREATE INDEX storages_block_key ON storages(block_key_id);

CREATE TRIGGER storages_notice_delete AFTER DELETE ON storages
BEGIN
    INSERT INTO notice_changes VALUES('storage',OLD.id,X'');
END;

CREATE TRIGGER storages_notice_update AFTER UPDATE ON storages
BEGIN
    INSERT INTO notice_changes VALUES('storage',NEW.id,X'');
END;
CREATE INDEX storages_cluster ON storages(cluster_id);
PRAGMA legacy_alter_table=OFF;
UPDATE mokyu_meta SET schema_version=20261113034820 WHERE id=1;
