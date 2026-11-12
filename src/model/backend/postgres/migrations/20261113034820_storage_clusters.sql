ALTER TABLE nodes DROP CONSTRAINT nodes_role_check;
ALTER TABLE nodes ADD CONSTRAINT nodes_role_check CHECK(role IN ('control','data','worker','node','storage'));
ALTER TABLE notification_consumers DROP CONSTRAINT notification_consumers_role_check;
ALTER TABLE notification_consumers ADD CONSTRAINT notification_consumers_role_check CHECK(role IN ('control','data','worker','node','storage'));

CREATE TABLE storage_cluster_lock (id integer PRIMARY KEY CHECK(id=1));
INSERT INTO storage_cluster_lock VALUES(1);
CREATE TABLE storage_clusters (
    id uuid PRIMARY KEY,
    name text NOT NULL UNIQUE CHECK(octet_length(name) BETWEEN 1 AND 128),
    revision bigint NOT NULL CHECK(revision>0),
    policy jsonb NOT NULL,
    manifest jsonb NOT NULL
);
CREATE TABLE storage_cluster_history (
    cluster_id uuid NOT NULL REFERENCES storage_clusters(id),
    revision bigint NOT NULL CHECK(revision>0),
    manifest jsonb NOT NULL,
    digest text NOT NULL CHECK(length(digest)=64),
    PRIMARY KEY(cluster_id,revision)
);
CREATE TABLE storage_devices (
    media_id uuid PRIMARY KEY,
    node_id uuid NOT NULL UNIQUE REFERENCES nodes(id),
    cluster_id uuid REFERENCES storage_clusters(id),
    observed_at bigint NOT NULL,
    announcement jsonb NOT NULL
);

ALTER TABLE storages ADD COLUMN cluster_id uuid REFERENCES storage_clusters(id);
ALTER TABLE storages DROP CONSTRAINT storage_targets_type_check;
ALTER TABLE storages DROP CONSTRAINT storage_targets_check;
ALTER TABLE storages ADD CONSTRAINT storage_targets_type_check CHECK(type IN ('local','s3','mokyu'));
ALTER TABLE storages ADD CONSTRAINT storage_targets_check CHECK(
    (type='local' AND cluster_id IS NULL AND path IS NOT NULL AND endpoint_url IS NULL AND remote_bucket_name IS NULL AND region IS NULL AND access_key_encrypted IS NULL AND secret_key_encrypted IS NULL)
    OR (type='s3' AND cluster_id IS NULL AND path IS NULL AND endpoint_url IS NOT NULL AND remote_bucket_name IS NOT NULL AND region IS NOT NULL AND access_key_encrypted IS NOT NULL AND secret_key_encrypted IS NOT NULL)
    OR (type='mokyu' AND cluster_id IS NOT NULL AND path IS NULL AND endpoint_url IS NULL AND remote_bucket_name IS NULL AND region IS NULL AND access_key_encrypted IS NULL AND secret_key_encrypted IS NULL AND prefix='')
);
CREATE INDEX storages_cluster ON storages(cluster_id);
UPDATE mokyu_meta SET schema_version=20261113034820 WHERE singleton;
