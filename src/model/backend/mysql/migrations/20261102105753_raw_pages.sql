ALTER TABLE raw_sources ADD COLUMN page_count BIGINT CHECK (page_count >= 0);

CREATE TABLE raw_page_groups (
    source_id BIGINT NOT NULL,
    group_no BIGINT NOT NULL CHECK (group_no >= 0),
    hashes VARBINARY(8192) NOT NULL CHECK (OCTET_LENGTH(hashes) BETWEEN 32 AND 8192 AND MOD(OCTET_LENGTH(hashes), 32) = 0),
    PRIMARY KEY (source_id, group_no),
    CONSTRAINT raw_page_groups_source_fk FOREIGN KEY (source_id) REFERENCES raw_sources(id) ON DELETE CASCADE
) ENGINE=InnoDB;

UPDATE mokyu_meta SET schema_version = 20261102105753;
