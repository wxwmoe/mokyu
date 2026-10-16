CREATE TABLE bucket_domains (
    host TEXT NOT NULL PRIMARY KEY,
    bucket_id BLOB CHECK (bucket_id IS NULL OR length(bucket_id)=16) NOT NULL,
    CONSTRAINT bucket_domains_bucket FOREIGN KEY (bucket_id) REFERENCES buckets(id) ON DELETE CASCADE
) STRICT, WITHOUT ROWID;

CREATE INDEX bucket_domains_bucket_id_fk ON bucket_domains(bucket_id);

UPDATE mokyu_meta SET schema_version=9 WHERE id=1;
