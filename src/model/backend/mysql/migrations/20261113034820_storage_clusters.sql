SET @check_drop = IF(LOCATE('MariaDB',VERSION())>0,' DROP CONSTRAINT ',' DROP CHECK ');
SET @role_check = (SELECT tc.CONSTRAINT_NAME FROM information_schema.TABLE_CONSTRAINTS tc JOIN information_schema.CHECK_CONSTRAINTS cc ON cc.CONSTRAINT_SCHEMA=tc.CONSTRAINT_SCHEMA AND cc.CONSTRAINT_NAME=tc.CONSTRAINT_NAME WHERE tc.TABLE_SCHEMA=DATABASE() AND tc.TABLE_NAME='nodes' AND tc.CONSTRAINT_TYPE='CHECK' AND cc.CHECK_CLAUSE LIKE '%role%' LIMIT 1);
SET @ddl = IF(LOCATE('MariaDB',VERSION())>0,
    'ALTER TABLE nodes MODIFY COLUMN role VARCHAR(8) CHARACTER SET ascii COLLATE ascii_bin NOT NULL',
    CONCAT('ALTER TABLE nodes DROP CHECK `',REPLACE(@role_check,'`','``'),'`'));
PREPARE storage_ddl FROM @ddl;
EXECUTE storage_ddl;
DEALLOCATE PREPARE storage_ddl;
ALTER TABLE nodes ADD CONSTRAINT nodes_role CHECK(role IN ('control','data','worker','node','storage'));
SET @role_check = (SELECT tc.CONSTRAINT_NAME FROM information_schema.TABLE_CONSTRAINTS tc JOIN information_schema.CHECK_CONSTRAINTS cc ON cc.CONSTRAINT_SCHEMA=tc.CONSTRAINT_SCHEMA AND cc.CONSTRAINT_NAME=tc.CONSTRAINT_NAME WHERE tc.TABLE_SCHEMA=DATABASE() AND tc.TABLE_NAME='notification_consumers' AND tc.CONSTRAINT_TYPE='CHECK' AND cc.CHECK_CLAUSE LIKE '%role%' LIMIT 1);
SET @ddl = IF(LOCATE('MariaDB',VERSION())>0,
    'ALTER TABLE notification_consumers MODIFY COLUMN role VARCHAR(8) NOT NULL',
    CONCAT('ALTER TABLE notification_consumers DROP CHECK `',REPLACE(@role_check,'`','``'),'`'));
PREPARE storage_ddl FROM @ddl;
EXECUTE storage_ddl;
DEALLOCATE PREPARE storage_ddl;
ALTER TABLE notification_consumers ADD CONSTRAINT notification_consumers_role CHECK(role IN ('control','data','worker','node','storage'));

CREATE TABLE storage_cluster_lock (id integer PRIMARY KEY, CONSTRAINT storage_cluster_lock_singleton CHECK(id=1));
INSERT INTO storage_cluster_lock VALUES(1);
CREATE TABLE storage_clusters (
    id binary(16) PRIMARY KEY,
    name varchar(128) COLLATE utf8mb4_bin NOT NULL UNIQUE,
    revision bigint NOT NULL,
    policy json NOT NULL,
    manifest json NOT NULL,
    CONSTRAINT storage_clusters_revision CHECK(revision>0)
) ENGINE=InnoDB;
CREATE TABLE storage_cluster_history (
    cluster_id binary(16) NOT NULL,
    revision bigint NOT NULL,
    manifest json NOT NULL,
    digest char(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    PRIMARY KEY(cluster_id,revision),
    FOREIGN KEY(cluster_id) REFERENCES storage_clusters(id),
    CONSTRAINT storage_cluster_history_revision CHECK(revision>0)
) ENGINE=InnoDB;
CREATE TABLE storage_devices (
    media_id binary(16) PRIMARY KEY,
    node_id binary(16) NOT NULL UNIQUE,
    cluster_id binary(16),
    observed_at bigint NOT NULL,
    announcement json NOT NULL,
    FOREIGN KEY(node_id) REFERENCES nodes(id),
    FOREIGN KEY(cluster_id) REFERENCES storage_clusters(id)
) ENGINE=InnoDB;
ALTER TABLE storages ADD COLUMN cluster_id binary(16), ADD CONSTRAINT storages_cluster FOREIGN KEY(cluster_id) REFERENCES storage_clusters(id);
SET @ddl = CONCAT('ALTER TABLE storages',@check_drop,'storages_type,',@check_drop,'storages_connection');
PREPARE storage_ddl FROM @ddl;
EXECUTE storage_ddl;
DEALLOCATE PREPARE storage_ddl;
ALTER TABLE storages ADD CONSTRAINT storages_type CHECK(type IN ('local','s3','mokyu'));
ALTER TABLE storages ADD CONSTRAINT storages_connection CHECK(
    (type='local' AND cluster_id IS NULL AND path IS NOT NULL AND endpoint_url IS NULL AND remote_bucket_name IS NULL AND region IS NULL AND access_key_encrypted IS NULL AND secret_key_encrypted IS NULL)
    OR (type='s3' AND cluster_id IS NULL AND path IS NULL AND endpoint_url IS NOT NULL AND remote_bucket_name IS NOT NULL AND region IS NOT NULL AND access_key_encrypted IS NOT NULL AND secret_key_encrypted IS NOT NULL)
    OR (type='mokyu' AND cluster_id IS NOT NULL AND path IS NULL AND endpoint_url IS NULL AND remote_bucket_name IS NULL AND region IS NULL AND access_key_encrypted IS NULL AND secret_key_encrypted IS NULL AND prefix='')
);
UPDATE mokyu_meta SET schema_version=20261113034820 WHERE id=1;
