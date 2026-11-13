CREATE TABLE storage_recovery_catalogs (
    id BLOB NOT NULL PRIMARY KEY,
    namespace_id BLOB NOT NULL,
    anchor TEXT NOT NULL CHECK(json_valid(anchor))
) STRICT, WITHOUT ROWID;

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

CREATE INDEX storage_recovery_entry_owner ON storage_recovery_entries(catalog,category,owner_id,id,position);
UPDATE mokyu_meta SET schema_version=20261113065937 WHERE id=1;
