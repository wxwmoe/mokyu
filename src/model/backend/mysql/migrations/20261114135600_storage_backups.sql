CREATE TABLE storage_backups (
    id BINARY(16) NOT NULL PRIMARY KEY,
    operation BINARY(16) NOT NULL,
    ordinal SMALLINT NOT NULL,
    verify_since BIGINT NOT NULL DEFAULT 0,
    storage_group VARCHAR(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin,
    created_at BIGINT NOT NULL,
    verified_at BIGINT,
    CONSTRAINT storage_backup_payload FOREIGN KEY(operation) REFERENCES storage_payloads(operation),
    CONSTRAINT storage_backup_ordinal UNIQUE(operation,ordinal),
    CONSTRAINT storage_backup_domain UNIQUE(operation,storage_group),
    CONSTRAINT storage_backup_bounds CHECK(ordinal>=0 AND ordinal<256)
);

CREATE INDEX storage_backup_debt ON storage_backups(verified_at,created_at,id);

CREATE TABLE storage_backup_files (
    backup_id BINARY(16) NOT NULL,
    sequence BIGINT NOT NULL,
    shard SMALLINT NOT NULL,
    media_id BINARY(16) NOT NULL,
    receipt JSON NOT NULL,
    verified_at BIGINT,
    PRIMARY KEY(backup_id,sequence,shard),
    CONSTRAINT storage_backup_file_slot FOREIGN KEY(backup_id) REFERENCES storage_backups(id)
);

CREATE INDEX storage_backup_recheck ON storage_backup_files(backup_id,verified_at,sequence,shard);

CREATE INDEX storage_backup_location ON storage_backup_files(media_id,backup_id);

CREATE TABLE storage_backup_scans (
    cluster_id BINARY(16) NOT NULL PRIMARY KEY,
    after_id BINARY(16) NOT NULL,
    verify_since BIGINT NOT NULL DEFAULT 0,
    CONSTRAINT storage_backup_scan_cluster FOREIGN KEY(cluster_id) REFERENCES storage_clusters(id) ON DELETE CASCADE
);

CREATE INDEX storage_backup_scan_payload ON storage_payloads(cluster_id,state,operation);

CREATE TABLE storage_recovery_objects (
    catalog BINARY(16) NOT NULL,
    path_hash BINARY(32) NOT NULL,
    path TEXT NOT NULL,
    revision BIGINT NOT NULL,
    published BOOLEAN NOT NULL,
    intent JSON NOT NULL,
    PRIMARY KEY(catalog,path_hash),
    CONSTRAINT storage_recovery_object_catalog FOREIGN KEY(catalog) REFERENCES storage_recovery_catalogs(id) ON DELETE CASCADE
);

CREATE TABLE storage_recovery_shards (
    catalog BINARY(16) NOT NULL,
    operation BINARY(16) NOT NULL,
    sequence BIGINT NOT NULL,
    copy_index SMALLINT NOT NULL,
    shard_index SMALLINT NOT NULL,
    receipt JSON NOT NULL,
    PRIMARY KEY(catalog,operation,sequence,shard_index,copy_index),
    CONSTRAINT storage_recovery_shard_catalog FOREIGN KEY(catalog) REFERENCES storage_recovery_catalogs(id) ON DELETE CASCADE
);
UPDATE mokyu_meta SET schema_version=20261114135600 WHERE id=1;
