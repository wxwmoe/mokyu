CREATE TABLE storage_recovery_catalogs (
    id BINARY(16) NOT NULL PRIMARY KEY,
    namespace_id BINARY(16) NOT NULL,
    anchor JSON NOT NULL
);

CREATE TABLE storage_recovery_cursors (
    catalog BINARY(16) NOT NULL,
    media BINARY(16) NOT NULL,
    scope VARCHAR(16) NOT NULL,
    revision BIGINT NOT NULL,
    cursor_value JSON NOT NULL,
    batch_hash BINARY(32) NOT NULL,
    CONSTRAINT storage_recovery_cursor_id PRIMARY KEY(catalog,media,scope),
    CONSTRAINT storage_recovery_cursor_catalog FOREIGN KEY(catalog) REFERENCES storage_recovery_catalogs(id) ON DELETE CASCADE,
    CONSTRAINT storage_recovery_cursor_revision CHECK(revision>0),
    CONSTRAINT storage_recovery_cursor_hash CHECK(octet_length(batch_hash)=32)
);

CREATE TABLE storage_recovery_entries (
    catalog BINARY(16) NOT NULL,
    category SMALLINT NOT NULL,
    owner_id BINARY(16) NOT NULL,
    id BINARY(16) NOT NULL,
    position BIGINT NOT NULL,
    content_hash BINARY(32) NOT NULL,
    encoded_bytes INTEGER NOT NULL,
    content JSON NOT NULL,
    CONSTRAINT storage_recovery_entry_id PRIMARY KEY(catalog,category,id,position),
    CONSTRAINT storage_recovery_entry_catalog FOREIGN KEY(catalog) REFERENCES storage_recovery_catalogs(id) ON DELETE CASCADE,
    CONSTRAINT storage_recovery_entry_category CHECK(category BETWEEN 0 AND 6),
    CONSTRAINT storage_recovery_entry_position CHECK(position>=0),
    CONSTRAINT storage_recovery_entry_hash CHECK(octet_length(content_hash)=32),
    CONSTRAINT storage_recovery_entry_size CHECK(encoded_bytes BETWEEN 1 AND 8388608)
);

CREATE INDEX storage_recovery_entry_owner ON storage_recovery_entries(catalog,category,owner_id,id,position);
UPDATE mokyu_meta SET schema_version=20261113065937 WHERE id=1;
