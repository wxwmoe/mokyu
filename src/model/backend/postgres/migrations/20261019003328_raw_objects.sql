ALTER TABLE storage_profiles ADD COLUMN cdc_enabled boolean NOT NULL DEFAULT true;
ALTER TABLE storage_profiles ADD COLUMN deduplication_enabled boolean NOT NULL DEFAULT false;
ALTER TABLE streams ADD COLUMN payload_type text NOT NULL DEFAULT 'chunks' CHECK (payload_type IN ('chunks','raw'));
ALTER TABLE streams ADD COLUMN payload_revision bigint NOT NULL DEFAULT 1 CHECK (payload_revision>0);

CREATE TABLE raw_sources (
    id uuid PRIMARY KEY,
    storage_id uuid NOT NULL REFERENCES storages(id),
    owner_stream uuid REFERENCES streams(id) ON DELETE SET NULL,
    medium text NOT NULL DEFAULT 'local' CHECK (medium IN ('local','memory','shared_s3')),
    durability text NOT NULL DEFAULT 'durable' CHECK (durability IN ('durable','buffered','volatile')),
    boot_id uuid,
    state text NOT NULL DEFAULT 'writing' CHECK (state IN ('writing','ready','unavailable','deleting')),
    size bigint NOT NULL DEFAULT 0 CHECK (size>=0),
    digest bytea CHECK (digest IS NULL OR octet_length(digest)=16),
    content_hash bytea CHECK (content_hash IS NULL OR octet_length(content_hash)=32),
    upload_id text,
    revision bigint NOT NULL DEFAULT 1 CHECK (revision>0),
    profile_revision bigint NOT NULL CHECK (profile_revision>0),
    quarantined_at timestamptz,
    quarantine_code text,
    created_at timestamptz NOT NULL DEFAULT now(),
    unreferenced_at timestamptz NOT NULL DEFAULT now(),
    CHECK (state<>'ready' OR digest IS NOT NULL),
    CHECK ((medium='memory')=(boot_id IS NOT NULL))
);
CREATE INDEX raw_sources_cleanup ON raw_sources(storage_id,state,unreferenced_at,id);
CREATE INDEX raw_sources_owner ON raw_sources(owner_stream) WHERE owner_stream IS NOT NULL;
CREATE UNIQUE INDEX raw_sources_content ON raw_sources(storage_id,content_hash,size);
CREATE TABLE raw_ranges (
    stream_id uuid NOT NULL REFERENCES streams(id) ON DELETE CASCADE,
    offset_bytes bigint NOT NULL CHECK (offset_bytes>=0),
    length bigint NOT NULL CHECK (length>=0),
    source_id uuid NOT NULL REFERENCES raw_sources(id),
    source_offset bigint NOT NULL DEFAULT 0 CHECK (source_offset>=0),
    PRIMARY KEY (stream_id,offset_bytes)
);
CREATE INDEX raw_ranges_source ON raw_ranges(source_id);
CREATE TABLE multipart_files (
    id uuid PRIMARY KEY,
    upload_id uuid REFERENCES uploads(id) ON DELETE SET NULL,
    storage_id uuid NOT NULL REFERENCES storages(id),
    source_id uuid UNIQUE REFERENCES raw_sources(id),
    reclaim_pending boolean NOT NULL DEFAULT false,
    state text NOT NULL DEFAULT 'open' CHECK (state IN ('open','sealed','deleting'))
);
CREATE INDEX multipart_files_upload ON multipart_files(upload_id,state);
CREATE INDEX multipart_files_reclaim ON multipart_files(storage_id,id) WHERE reclaim_pending;
ALTER TABLE fragments ADD COLUMN file_id uuid REFERENCES multipart_files(id);
ALTER TABLE fragments ADD COLUMN file_offset bigint CHECK ((file_id IS NULL)=(file_offset IS NULL) AND (file_offset IS NULL OR file_offset>=0));
CREATE INDEX fragments_file ON fragments(file_id,file_offset) WHERE file_id IS NOT NULL;

UPDATE mokyu_meta SET schema_version=47 WHERE singleton;
