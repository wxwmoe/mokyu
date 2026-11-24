CREATE TABLE storage_recovery_catalogs (
    id BLOB NOT NULL PRIMARY KEY,
    namespace_id BLOB NOT NULL,
    anchor TEXT NOT NULL CHECK(json_valid(anchor))
, publication TEXT CHECK(publication IS NULL OR json_valid(publication)), indexed_revision INTEGER NOT NULL DEFAULT 0) STRICT, WITHOUT ROWID;

CREATE TABLE storage_recovery_cursors (
    catalog BLOB NOT NULL,
    media BLOB NOT NULL,
    scope TEXT NOT NULL,
    revision INTEGER NOT NULL,
    cursor_value TEXT NOT NULL CHECK(json_valid(cursor_value)),
    batch_hash BLOB NOT NULL,
    CONSTRAINT storage_recovery_cursor_id PRIMARY KEY(catalog,media,scope),
    CONSTRAINT storage_recovery_cursor_catalog FOREIGN KEY(catalog) REFERENCES storage_recovery_catalogs(id) ON DELETE CASCADE,
    CONSTRAINT storage_recovery_cursor_revision CHECK(revision>0),
    CONSTRAINT storage_recovery_cursor_hash CHECK(length(batch_hash)=32)
) STRICT, WITHOUT ROWID;

CREATE TABLE storage_recovery_entries (
    catalog BLOB NOT NULL,
    category INTEGER NOT NULL,
    owner_id BLOB NOT NULL,
    id BLOB NOT NULL,
    position INTEGER NOT NULL,
    content_hash BLOB NOT NULL,
    encoded_bytes INTEGER NOT NULL,
    content TEXT NOT NULL CHECK(json_valid(content)),
    CONSTRAINT storage_recovery_entry_id PRIMARY KEY(catalog,category,id,position),
    CONSTRAINT storage_recovery_entry_catalog FOREIGN KEY(catalog) REFERENCES storage_recovery_catalogs(id) ON DELETE CASCADE,
    CONSTRAINT storage_recovery_entry_category CHECK(category BETWEEN 0 AND 6),
    CONSTRAINT storage_recovery_entry_position CHECK(position>=0),
    CONSTRAINT storage_recovery_entry_hash CHECK(length(content_hash)=32),
    CONSTRAINT storage_recovery_entry_size CHECK(encoded_bytes BETWEEN 1 AND 8388608)
) STRICT, WITHOUT ROWID;

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

CREATE TABLE storage_recovery_objects (
    catalog BLOB NOT NULL,
    path_hash BLOB NOT NULL,
    path TEXT NOT NULL,
    revision INTEGER NOT NULL,
    published INTEGER NOT NULL,
    intent TEXT NOT NULL,
    PRIMARY KEY(catalog,path_hash),
    CONSTRAINT storage_recovery_object_catalog FOREIGN KEY(catalog) REFERENCES storage_recovery_catalogs(id) ON DELETE CASCADE
) STRICT, WITHOUT ROWID;

CREATE TABLE storage_recovery_shards (
    catalog BLOB NOT NULL,
    operation BLOB NOT NULL,
    sequence INTEGER NOT NULL,
    copy_index INTEGER NOT NULL,
    shard_index INTEGER NOT NULL,
    receipt TEXT NOT NULL,
    PRIMARY KEY(catalog,operation,sequence,shard_index,copy_index),
    CONSTRAINT storage_recovery_shard_catalog FOREIGN KEY(catalog) REFERENCES storage_recovery_catalogs(id) ON DELETE CASCADE
) STRICT, WITHOUT ROWID;

CREATE INDEX storage_recovery_entry_owner ON storage_recovery_entries(catalog,category,owner_id,id,position);

CREATE INDEX storage_recovery_file_path ON storage_recovery_files(catalog,path_hash,publication_revision);

CREATE TABLE storage_clusters (id BLOB PRIMARY KEY, manifest TEXT NOT NULL) STRICT;
CREATE TABLE storage_cluster_history (cluster_id BLOB NOT NULL, revision INTEGER NOT NULL, manifest TEXT NOT NULL, PRIMARY KEY(cluster_id,revision)) STRICT, WITHOUT ROWID;
