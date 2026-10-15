CREATE INDEX packs_preparing_owner ON packs(owner_task)
WHERE owner_task IS NOT NULL AND state='preparing';

CREATE INDEX chunk_encodings_uploading_owner ON chunk_encodings(owner_task)
WHERE owner_task IS NOT NULL AND state='uploading';

UPDATE mokyu_meta SET schema_version=36;
