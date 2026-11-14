ALTER TABLE storage_payloads ADD COLUMN collect_after BIGINT NOT NULL DEFAULT 0 CHECK(collect_after>=0);
CREATE INDEX storage_payload_collection ON storage_payloads(namespace_id,state,collect_after,operation);
UPDATE mokyu_meta SET schema_version=20261114054900 WHERE singleton;
