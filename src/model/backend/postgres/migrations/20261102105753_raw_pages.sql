ALTER TABLE raw_sources ADD COLUMN page_count BIGINT CHECK (page_count >= 0);

CREATE TABLE raw_page_groups (
    source_id UUID NOT NULL REFERENCES raw_sources(id) ON DELETE CASCADE,
    group_no BIGINT NOT NULL CHECK (group_no >= 0),
    hashes BYTEA NOT NULL CHECK (length(hashes) BETWEEN 32 AND 8192) CHECK (length(hashes) % 32 = 0),
    PRIMARY KEY (source_id, group_no)
);

UPDATE mokyu_meta SET schema_version = 20261102105753;
