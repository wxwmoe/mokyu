CREATE TABLE storage_repairs (
    id BLOB NOT NULL PRIMARY KEY,
    operation BLOB NOT NULL,
    sequence INTEGER NOT NULL,
    original_receipt TEXT NOT NULL CHECK(json_valid(original_receipt)),
    candidate_id BLOB,
    candidate TEXT CHECK(candidate IS NULL OR json_valid(candidate)),
    state TEXT NOT NULL DEFAULT 'queued',
    original_removed INTEGER NOT NULL DEFAULT FALSE,
    candidate_removed INTEGER NOT NULL DEFAULT FALSE,
    CONSTRAINT storage_repair_payload FOREIGN KEY(operation) REFERENCES storage_payloads(operation),
    CONSTRAINT storage_repair_state CHECK(state IN ('queued','healthy','writing','installed'))
) STRICT, WITHOUT ROWID;

CREATE INDEX storage_repair_operation ON storage_repairs(operation,state,id);
UPDATE mokyu_meta SET schema_version=20261114060500 WHERE id=1;
