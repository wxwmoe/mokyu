CREATE TABLE storage_journal_sources (
    operation BLOB NOT NULL PRIMARY KEY,
    namespace_id BLOB NOT NULL,
    path TEXT NOT NULL,
    revision INTEGER NOT NULL DEFAULT 1 CHECK(revision>0),
    allocated_revision INTEGER NOT NULL DEFAULT 0 CHECK(allocated_revision>=0),
    cursor_id BLOB NOT NULL,
    file_id BLOB,
    CONSTRAINT storage_journal_source_payload FOREIGN KEY(operation) REFERENCES storage_payloads(operation),
    CONSTRAINT storage_journal_source_file FOREIGN KEY(file_id) REFERENCES journal_files(id) ON DELETE SET NULL
) STRICT, WITHOUT ROWID;

CREATE INDEX storage_journal_pending ON storage_journal_sources(namespace_id,allocated_revision,revision,operation);
UPDATE mokyu_meta SET schema_version=20261114002400 WHERE id=1;
