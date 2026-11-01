ALTER TABLE transfer_runs ADD COLUMN source_storage_id BLOB NOT NULL DEFAULT X'00000000000000000000000000000000';
UPDATE transfer_runs SET source_storage_id=storage_id;

CREATE TABLE transfer_sources_new (
    id INTEGER PRIMARY KEY,
    task_id BLOB NOT NULL REFERENCES transfer_runs(task_id) ON DELETE CASCADE,
    source_type TEXT NOT NULL CHECK (source_type IN ('raw','chunk','pack')),
    source_id INTEGER NOT NULL,
    generation BIGINT NOT NULL DEFAULT 0 CHECK (generation>=0),
    snapshot TEXT,
    input TEXT,
    output TEXT,
    candidate INTEGER REFERENCES streams(id),
    state TEXT NOT NULL DEFAULT 'selected'
        CHECK (state IN ('selected','scanning','copying','verifying','ready')),
    scanned_bytes BIGINT NOT NULL DEFAULT 0 CHECK (scanned_bytes>=0),
    written_bytes BIGINT NOT NULL DEFAULT 0 CHECK (written_bytes>=0),
    checksum TEXT,
    upload_id TEXT,
    source_encoding INTEGER REFERENCES chunk_encodings(encoding_id),
    source_pack INTEGER REFERENCES packs(id),
    source_raw INTEGER REFERENCES raw_sources(id),
    UNIQUE (task_id,source_type,source_id)
);
INSERT INTO transfer_sources_new(task_id,source_type,source_id,generation,snapshot)
SELECT task_id,source_type,source_id,generation,snapshot FROM transfer_sources;
DROP TABLE transfer_sources;
ALTER TABLE transfer_sources_new RENAME TO transfer_sources;
CREATE INDEX transfer_sources_protection ON transfer_sources(source_type,source_id,task_id);
CREATE INDEX transfer_sources_candidate ON transfer_sources(candidate);
CREATE INDEX transfer_sources_encoding ON transfer_sources(source_encoding);
CREATE INDEX transfer_sources_pack ON transfer_sources(source_pack);
CREATE INDEX transfer_sources_raw ON transfer_sources(source_raw);
CREATE INDEX transfer_sources_work ON transfer_sources(task_id,state,id);

CREATE TABLE transfer_members (
    unit_id INTEGER NOT NULL REFERENCES transfer_sources(id) ON DELETE CASCADE,
    task_id BLOB NOT NULL REFERENCES transfer_runs(task_id) ON DELETE CASCADE,
    source_chunk INTEGER NOT NULL REFERENCES chunks(id),
    source_offset BIGINT NOT NULL CHECK (source_offset>=0),
    length INT NOT NULL CHECK (length>0),
    PRIMARY KEY (task_id,source_chunk),
    UNIQUE (unit_id,source_offset)
);
CREATE INDEX transfer_members_chunk ON transfer_members(source_chunk);

CREATE TABLE transfer_spans (
    id INTEGER PRIMARY KEY,
    unit_id INTEGER NOT NULL REFERENCES transfer_sources(id) ON DELETE CASCADE,
    source_offset BIGINT NOT NULL CHECK (source_offset>=0),
    length INT NOT NULL CHECK (length>0),
    hash BLOB NOT NULL CHECK (length(hash)=32),
    UNIQUE (unit_id,source_offset)
);

CREATE TABLE transfer_parts (
    unit_id INTEGER NOT NULL REFERENCES transfer_sources(id) ON DELETE CASCADE,
    number INT NOT NULL CHECK (number BETWEEN 1 AND 10000),
    source_offset BIGINT NOT NULL CHECK (source_offset>=0),
    length BIGINT NOT NULL CHECK (length>0),
    digest BLOB NOT NULL CHECK (length(digest)=16),
    etag TEXT,
    PRIMARY KEY (unit_id,number)
);

UPDATE mokyu_meta SET schema_version=47 WHERE id=1;
