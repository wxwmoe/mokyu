ALTER TABLE transfer_runs ADD COLUMN source_storage_id BINARY(16);
UPDATE transfer_runs SET source_storage_id=storage_id;
ALTER TABLE transfer_runs MODIFY source_storage_id BINARY(16) NOT NULL;

ALTER TABLE transfer_sources
    ADD COLUMN id BIGINT NOT NULL AUTO_INCREMENT UNIQUE,
    ADD COLUMN input JSON,
    ADD COLUMN output JSON,
    ADD COLUMN candidate BIGINT,
    ADD COLUMN state VARCHAR(24) NOT NULL DEFAULT 'selected'
        CHECK (state IN ('selected','scanning','copying','verifying','ready')),
    ADD COLUMN scanned_bytes BIGINT NOT NULL DEFAULT 0 CHECK (scanned_bytes>=0),
    ADD COLUMN written_bytes BIGINT NOT NULL DEFAULT 0 CHECK (written_bytes>=0),
    ADD COLUMN checksum JSON,
    ADD COLUMN upload_id TEXT,
    ADD COLUMN source_encoding BIGINT,
    ADD COLUMN source_pack BIGINT,
    ADD COLUMN source_raw BIGINT,
    ADD FOREIGN KEY (candidate) REFERENCES streams(id),
    ADD FOREIGN KEY (source_encoding) REFERENCES chunk_encodings(encoding_id),
    ADD FOREIGN KEY (source_pack) REFERENCES packs(id),
    ADD FOREIGN KEY (source_raw) REFERENCES raw_sources(id);
CREATE INDEX transfer_sources_candidate ON transfer_sources(candidate);
CREATE INDEX transfer_sources_encoding ON transfer_sources(source_encoding);
CREATE INDEX transfer_sources_pack ON transfer_sources(source_pack);
CREATE INDEX transfer_sources_raw ON transfer_sources(source_raw);
CREATE INDEX transfer_sources_work ON transfer_sources(task_id,state,id);

CREATE TABLE transfer_members (
    unit_id BIGINT NOT NULL,
    task_id BINARY(16) NOT NULL,
    source_chunk BIGINT NOT NULL,
    source_offset BIGINT NOT NULL CHECK (source_offset>=0),
    length INT NOT NULL CHECK (length>0),
    PRIMARY KEY (task_id,source_chunk),
    UNIQUE (unit_id,source_offset),
    FOREIGN KEY (unit_id) REFERENCES transfer_sources(id) ON DELETE CASCADE,
    FOREIGN KEY (task_id) REFERENCES transfer_runs(task_id) ON DELETE CASCADE,
    FOREIGN KEY (source_chunk) REFERENCES chunks(id)
);
CREATE INDEX transfer_members_chunk ON transfer_members(source_chunk);

CREATE TABLE transfer_spans (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    unit_id BIGINT NOT NULL,
    source_offset BIGINT NOT NULL CHECK (source_offset>=0),
    length INT NOT NULL CHECK (length>0),
    hash BINARY(32) NOT NULL,
    UNIQUE (unit_id,source_offset),
    FOREIGN KEY (unit_id) REFERENCES transfer_sources(id) ON DELETE CASCADE
);

CREATE TABLE transfer_parts (
    unit_id BIGINT NOT NULL,
    number INT NOT NULL CHECK (number BETWEEN 1 AND 10000),
    source_offset BIGINT NOT NULL CHECK (source_offset>=0),
    length BIGINT NOT NULL CHECK (length>0),
    digest BINARY(16) NOT NULL,
    etag TEXT,
    PRIMARY KEY (unit_id,number),
    FOREIGN KEY (unit_id) REFERENCES transfer_sources(id) ON DELETE CASCADE
);

UPDATE mokyu_meta SET schema_version=46 WHERE id=1;
