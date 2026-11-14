CREATE TABLE storage_journal_sources (
    operation BINARY(16) NOT NULL PRIMARY KEY,
    namespace_id BINARY(16) NOT NULL,
    path VARCHAR(1024) COLLATE utf8mb4_bin NOT NULL,
    revision BIGINT NOT NULL DEFAULT 1 CHECK(revision>0),
    allocated_revision BIGINT NOT NULL DEFAULT 0 CHECK(allocated_revision>=0),
    cursor_id BINARY(16) NOT NULL,
    file_id BINARY(16),
    CONSTRAINT storage_journal_source_payload FOREIGN KEY(operation) REFERENCES storage_payloads(operation),
    CONSTRAINT storage_journal_source_file FOREIGN KEY(file_id) REFERENCES journal_files(id) ON DELETE SET NULL
);

CREATE INDEX storage_journal_pending ON storage_journal_sources(namespace_id,allocated_revision,revision,operation);
UPDATE mokyu_meta SET schema_version=20261114002400 WHERE id=1;
