CREATE TABLE storage_repairs (
    id UUID NOT NULL PRIMARY KEY,
    operation UUID NOT NULL,
    sequence BIGINT NOT NULL,
    original_receipt JSONB NOT NULL,
    candidate_id UUID,
    candidate JSONB,
    state VARCHAR(16) NOT NULL DEFAULT 'queued',
    original_removed BOOLEAN NOT NULL DEFAULT FALSE,
    candidate_removed BOOLEAN NOT NULL DEFAULT FALSE,
    CONSTRAINT storage_repair_payload FOREIGN KEY(operation) REFERENCES storage_payloads(operation),
    CONSTRAINT storage_repair_state CHECK(state IN ('queued','healthy','writing','installed'))
);

CREATE INDEX storage_repair_operation ON storage_repairs(operation,state,id);
UPDATE mokyu_meta SET schema_version=20261114060500 WHERE singleton;
