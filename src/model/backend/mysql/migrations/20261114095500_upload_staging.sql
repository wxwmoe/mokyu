CREATE TABLE upload_staging (
    upload_id BINARY(16) NOT NULL PRIMARY KEY,
    storage_id BINARY(16),
    policy_revision BIGINT NOT NULL CHECK(policy_revision>0),
    tail_upload VARCHAR(24) CHARACTER SET ascii COLLATE ascii_bin NOT NULL CHECK(tail_upload IN ('immediate','on_demand_timeout','on_demand')),
    tail_wait_seconds BIGINT NOT NULL CHECK(tail_wait_seconds>0),
    FOREIGN KEY (upload_id) REFERENCES uploads(id) ON DELETE CASCADE,
    FOREIGN KEY (storage_id) REFERENCES storages(id),
    INDEX upload_staging_storage(storage_id)
) ENGINE=InnoDB;

INSERT INTO upload_staging(upload_id,storage_id,policy_revision,tail_upload,tail_wait_seconds)
SELECT u.id,CASE WHEN m.deployment_mode='single' THEN NULL ELSE coalesce(UNHEX(REPLACE(JSON_UNQUOTE(JSON_EXTRACT(r.value,'$.staging_storage_id')),'-','')),b.storage_id) END,r.revision,'immediate',600
FROM uploads u JOIN buckets b ON b.id=u.bucket_id CROSS JOIN mokyu_meta m JOIN runtime_settings r ON r.section='uploads'
WHERE u.state IN ('active','completing');

UPDATE mokyu_meta SET schema_version=20261114095500 WHERE id=1;
