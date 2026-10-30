ALTER TABLE transfer_runs ADD COLUMN source_storage_id uuid;
UPDATE transfer_runs SET source_storage_id=storage_id;
ALTER TABLE transfer_runs ALTER COLUMN source_storage_id SET NOT NULL;

ALTER TABLE transfer_sources
    ADD COLUMN id uuid NOT NULL DEFAULT uuidv7() UNIQUE,
    ADD COLUMN input jsonb,
    ADD COLUMN output jsonb,
    ADD COLUMN candidate uuid REFERENCES streams(id),
    ADD COLUMN state TEXT NOT NULL DEFAULT 'selected'
        CHECK (state IN ('selected','scanning','copying','verifying','ready')),
    ADD COLUMN scanned_bytes BIGINT NOT NULL DEFAULT 0 CHECK (scanned_bytes>=0),
    ADD COLUMN written_bytes BIGINT NOT NULL DEFAULT 0 CHECK (written_bytes>=0),
    ADD COLUMN checksum jsonb,
    ADD COLUMN upload_id TEXT,
    ADD COLUMN source_encoding uuid REFERENCES chunk_encodings(encoding_id),
    ADD COLUMN source_pack uuid REFERENCES packs(id),
    ADD COLUMN source_raw uuid REFERENCES raw_sources(id);
CREATE INDEX transfer_sources_candidate ON transfer_sources(candidate);
CREATE INDEX transfer_sources_encoding ON transfer_sources(source_encoding);
CREATE INDEX transfer_sources_pack ON transfer_sources(source_pack);
CREATE INDEX transfer_sources_raw ON transfer_sources(source_raw);
CREATE INDEX transfer_sources_work ON transfer_sources(task_id,state,id);

CREATE TABLE transfer_members (
    unit_id uuid NOT NULL REFERENCES transfer_sources(id) ON DELETE CASCADE,
    task_id uuid NOT NULL REFERENCES transfer_runs(task_id) ON DELETE CASCADE,
    source_chunk uuid NOT NULL REFERENCES chunks(id),
    source_offset BIGINT NOT NULL CHECK (source_offset>=0),
    length INT NOT NULL CHECK (length>0),
    PRIMARY KEY (task_id,source_chunk),
    UNIQUE (unit_id,source_offset)
);
CREATE INDEX transfer_members_chunk ON transfer_members(source_chunk);

CREATE TABLE transfer_spans (
    id uuid PRIMARY KEY DEFAULT uuidv7(),
    unit_id uuid NOT NULL REFERENCES transfer_sources(id) ON DELETE CASCADE,
    source_offset BIGINT NOT NULL CHECK (source_offset>=0),
    length INT NOT NULL CHECK (length>0),
    hash bytea NOT NULL CHECK (octet_length(hash)=32),
    UNIQUE (unit_id,source_offset)
);

CREATE TABLE transfer_parts (
    unit_id uuid NOT NULL REFERENCES transfer_sources(id) ON DELETE CASCADE,
    number INT NOT NULL CHECK (number BETWEEN 1 AND 10000),
    source_offset BIGINT NOT NULL CHECK (source_offset>=0),
    length BIGINT NOT NULL CHECK (length>0),
    digest bytea NOT NULL CHECK (octet_length(digest)=16),
    etag TEXT,
    PRIMARY KEY (unit_id,number)
);

UPDATE mokyu_meta SET schema_version=93 WHERE singleton;
