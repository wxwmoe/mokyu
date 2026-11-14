CREATE TABLE storage_backups (
    id BLOB NOT NULL PRIMARY KEY,
    operation BLOB NOT NULL,
    ordinal INTEGER NOT NULL,
    verify_since INTEGER NOT NULL DEFAULT 0,
    storage_group TEXT,
    created_at INTEGER NOT NULL,
    verified_at INTEGER,
    CONSTRAINT storage_backup_payload FOREIGN KEY(operation) REFERENCES storage_payloads(operation),
    CONSTRAINT storage_backup_ordinal UNIQUE(operation,ordinal),
    CONSTRAINT storage_backup_domain UNIQUE(operation,storage_group),
    CONSTRAINT storage_backup_bounds CHECK(ordinal>=0 AND ordinal<256)
) STRICT, WITHOUT ROWID;

CREATE INDEX storage_backup_debt ON storage_backups(verified_at,created_at,id);

CREATE TABLE storage_backup_files (
    backup_id BLOB NOT NULL,
    sequence INTEGER NOT NULL,
    shard INTEGER NOT NULL,
    media_id BLOB NOT NULL,
    receipt TEXT NOT NULL CHECK(json_valid(receipt)),
    verified_at INTEGER,
    PRIMARY KEY(backup_id,sequence,shard),
    CONSTRAINT storage_backup_file_slot FOREIGN KEY(backup_id) REFERENCES storage_backups(id)
) STRICT, WITHOUT ROWID;

CREATE INDEX storage_backup_recheck ON storage_backup_files(backup_id,verified_at,sequence,shard);

CREATE INDEX storage_backup_location ON storage_backup_files(media_id,backup_id);

CREATE TABLE storage_backup_scans (
    cluster_id BLOB NOT NULL PRIMARY KEY,
    after_id BLOB NOT NULL,
    verify_since INTEGER NOT NULL DEFAULT 0,
    CONSTRAINT storage_backup_scan_cluster FOREIGN KEY(cluster_id) REFERENCES storage_clusters(id) ON DELETE CASCADE
) STRICT, WITHOUT ROWID;

CREATE INDEX storage_backup_scan_payload ON storage_payloads(cluster_id,state,operation);

CREATE TABLE storage_recovery_objects (
    catalog BLOB NOT NULL,
    path_hash BLOB NOT NULL,
    path TEXT NOT NULL,
    revision INTEGER NOT NULL,
    published INTEGER NOT NULL,
    intent TEXT NOT NULL,
    PRIMARY KEY(catalog,path_hash),
    CONSTRAINT storage_recovery_object_catalog FOREIGN KEY(catalog) REFERENCES storage_recovery_catalogs(id) ON DELETE CASCADE
) STRICT, WITHOUT ROWID;

CREATE TABLE storage_recovery_shards (
    catalog BLOB NOT NULL,
    operation BLOB NOT NULL,
    sequence INTEGER NOT NULL,
    copy_index INTEGER NOT NULL,
    shard_index INTEGER NOT NULL,
    receipt TEXT NOT NULL,
    PRIMARY KEY(catalog,operation,sequence,shard_index,copy_index),
    CONSTRAINT storage_recovery_shard_catalog FOREIGN KEY(catalog) REFERENCES storage_recovery_catalogs(id) ON DELETE CASCADE
) STRICT, WITHOUT ROWID;
UPDATE mokyu_meta SET schema_version=20261114135600 WHERE id=1;
