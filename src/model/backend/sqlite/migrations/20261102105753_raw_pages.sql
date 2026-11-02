ALTER TABLE raw_sources ADD COLUMN page_count INTEGER CHECK (page_count >= 0);

CREATE TABLE raw_page_groups (
    source_id INTEGER NOT NULL REFERENCES raw_sources(id) ON DELETE CASCADE,
    group_no INTEGER NOT NULL CHECK (group_no >= 0),
    hashes BLOB NOT NULL CHECK (length(hashes) BETWEEN 32 AND 8192 AND length(hashes) % 32 = 0),
    PRIMARY KEY (source_id, group_no)
) WITHOUT ROWID;

UPDATE mokyu_meta SET schema_version = 20261102105753;
