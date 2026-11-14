ALTER TABLE storage_recovery_catalogs ADD COLUMN publication JSON;
ALTER TABLE storage_recovery_catalogs ADD COLUMN indexed_revision BIGINT NOT NULL DEFAULT 0;

CREATE TABLE storage_recovery_files (
    catalog BINARY(16) NOT NULL,
    operation BINARY(16) NOT NULL,
    path_hash BINARY(32) NOT NULL,
    path VARCHAR(1024) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    publication_revision BIGINT NOT NULL,
    through_sequence BIGINT NOT NULL,
    retired BOOLEAN NOT NULL DEFAULT FALSE,
    part JSON NOT NULL,
    decoded JSON,
    CONSTRAINT storage_recovery_file_id PRIMARY KEY(catalog,operation),
    CONSTRAINT storage_recovery_file_catalog FOREIGN KEY(catalog) REFERENCES storage_recovery_catalogs(id) ON DELETE CASCADE,
    CONSTRAINT storage_recovery_file_revision CHECK(publication_revision>0),
    CONSTRAINT storage_recovery_file_sequence CHECK(through_sequence>=0),
    CONSTRAINT storage_recovery_file_hash CHECK(octet_length(path_hash)=32)
);

CREATE INDEX storage_recovery_file_path ON storage_recovery_files(catalog,path_hash,publication_revision);
UPDATE mokyu_meta SET schema_version=20261114030312 WHERE id=1;
