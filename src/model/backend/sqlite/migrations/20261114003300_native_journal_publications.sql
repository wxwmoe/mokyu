CREATE TABLE storage_journal_publications (
    id BLOB NOT NULL PRIMARY KEY,
    namespace_id BLOB NOT NULL,
    cluster_id BLOB NOT NULL,
    revision INTEGER NOT NULL CHECK(revision>0),
    manifest_revision INTEGER NOT NULL CHECK(manifest_revision>0),
    proof TEXT NOT NULL CHECK(json_valid(proof)),
    settled INTEGER NOT NULL DEFAULT FALSE,
    CONSTRAINT storage_journal_publication_revision UNIQUE(namespace_id,revision)
) STRICT, WITHOUT ROWID;

CREATE TABLE storage_journal_receipts (
    namespace_id BLOB NOT NULL,
    media_id BLOB NOT NULL,
    revision INTEGER NOT NULL CHECK(revision>=0),
    PRIMARY KEY(namespace_id,media_id)
) STRICT, WITHOUT ROWID;

UPDATE mokyu_meta SET schema_version=20261114003300 WHERE id=1;
