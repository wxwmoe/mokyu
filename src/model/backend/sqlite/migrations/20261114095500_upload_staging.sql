CREATE TABLE upload_staging (
    upload_id BLOB NOT NULL PRIMARY KEY REFERENCES uploads(id) ON DELETE CASCADE,
    storage_id BLOB REFERENCES storages(id),
    policy_revision INTEGER NOT NULL CHECK(policy_revision>0),
    tail_upload TEXT NOT NULL CHECK(tail_upload IN ('immediate','on_demand_timeout','on_demand')),
    tail_wait_seconds INTEGER NOT NULL CHECK(tail_wait_seconds>0)
) STRICT, WITHOUT ROWID;
CREATE INDEX upload_staging_storage ON upload_staging(storage_id);

INSERT INTO upload_staging(upload_id,storage_id,policy_revision,tail_upload,tail_wait_seconds)
SELECT u.id,CASE WHEN m.deployment_mode='single' THEN NULL ELSE coalesce(unhex(replace(json_extract(r.value,'$.staging_storage_id'),'-','')),b.storage_id) END,r.revision,'immediate',600
FROM uploads u JOIN buckets b ON b.id=u.bucket_id CROSS JOIN mokyu_meta m JOIN runtime_settings r ON r.section='uploads'
WHERE u.state IN ('active','completing');

UPDATE mokyu_meta SET schema_version=20261114095500 WHERE id=1;
