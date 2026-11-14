ALTER TABLE storage_recovery_catalogs ADD COLUMN publication TEXT CHECK(publication IS NULL OR json_valid(publication));
ALTER TABLE storage_recovery_catalogs ADD COLUMN indexed_revision INTEGER NOT NULL DEFAULT 0;

CREATE TABLE storage_recovery_files (
    catalog BLOB NOT NULL,
    operation BLOB NOT NULL,
    path_hash BLOB NOT NULL,
    path TEXT NOT NULL,
    publication_revision INTEGER NOT NULL,
    through_sequence INTEGER NOT NULL,
    retired INTEGER NOT NULL DEFAULT 0,
    part TEXT NOT NULL CHECK(json_valid(part)),
    decoded TEXT CHECK(decoded IS NULL OR json_valid(decoded)),
    CONSTRAINT storage_recovery_file_id PRIMARY KEY(catalog,operation),
    CONSTRAINT storage_recovery_file_catalog FOREIGN KEY(catalog) REFERENCES storage_recovery_catalogs(id) ON DELETE CASCADE,
    CONSTRAINT storage_recovery_file_revision CHECK(publication_revision>0),
    CONSTRAINT storage_recovery_file_sequence CHECK(through_sequence>=0),
    CONSTRAINT storage_recovery_file_hash CHECK(length(path_hash)=32),
    CONSTRAINT storage_recovery_file_retired CHECK(retired IN (0,1))
) STRICT, WITHOUT ROWID;

CREATE INDEX storage_recovery_file_path ON storage_recovery_files(catalog,path_hash,publication_revision);
UPDATE mokyu_meta SET schema_version=20261114030312 WHERE id=1;
