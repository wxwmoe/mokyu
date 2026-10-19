ALTER TABLE storage_profiles ADD COLUMN cdc_enabled INTEGER NOT NULL DEFAULT 1 CHECK (cdc_enabled IN (0,1));
ALTER TABLE storage_profiles ADD COLUMN deduplication_enabled INTEGER NOT NULL DEFAULT 0 CHECK (deduplication_enabled IN (0,1));
ALTER TABLE streams ADD COLUMN payload_type TEXT NOT NULL DEFAULT 'chunks' CHECK (payload_type IN ('chunks','raw'));
ALTER TABLE streams ADD COLUMN payload_revision INTEGER NOT NULL DEFAULT 1 CHECK (payload_revision>0);

CREATE TABLE raw_sources (
    id BLOB NOT NULL PRIMARY KEY CHECK (length(id)=16),
    storage_id BLOB NOT NULL REFERENCES storages(id),
    owner_stream BLOB REFERENCES streams(id) ON DELETE SET NULL,
    medium TEXT NOT NULL DEFAULT 'local' CHECK (medium IN ('local','memory','shared_s3')),
    durability TEXT NOT NULL DEFAULT 'durable' CHECK (durability IN ('durable','buffered','volatile')),
    boot_id BLOB CHECK (boot_id IS NULL OR length(boot_id)=16),
    state TEXT NOT NULL DEFAULT 'writing' CHECK (state IN ('writing','ready','unavailable','deleting')),
    size INTEGER NOT NULL DEFAULT 0 CHECK (size>=0),
    digest BLOB CHECK (digest IS NULL OR length(digest)=16),
    content_hash BLOB CHECK (content_hash IS NULL OR length(content_hash)=32),
    upload_id TEXT,
    revision INTEGER NOT NULL DEFAULT 1 CHECK (revision>0),
    profile_revision INTEGER NOT NULL CHECK (profile_revision>0),
    quarantined_at INTEGER,
    quarantine_code TEXT,
    created_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec')*1000000 AS INTEGER)),
    unreferenced_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec')*1000000 AS INTEGER)),
    CHECK (state<>'ready' OR digest IS NOT NULL),
    CHECK ((medium='memory')=(boot_id IS NOT NULL))
) STRICT, WITHOUT ROWID;
CREATE INDEX raw_sources_cleanup ON raw_sources(storage_id,state,unreferenced_at,id);
CREATE INDEX raw_sources_owner ON raw_sources(owner_stream) WHERE owner_stream IS NOT NULL;
CREATE UNIQUE INDEX raw_sources_content ON raw_sources(storage_id,content_hash,size);
CREATE TABLE raw_ranges (
    stream_id BLOB NOT NULL REFERENCES streams(id) ON DELETE CASCADE,
    offset_bytes INTEGER NOT NULL CHECK (offset_bytes>=0),
    length INTEGER NOT NULL CHECK (length>=0),
    source_id BLOB NOT NULL REFERENCES raw_sources(id),
    source_offset INTEGER NOT NULL DEFAULT 0 CHECK (source_offset>=0),
    PRIMARY KEY (stream_id,offset_bytes)
) STRICT, WITHOUT ROWID;
CREATE INDEX raw_ranges_source ON raw_ranges(source_id);
CREATE TABLE multipart_files (
    id BLOB NOT NULL PRIMARY KEY CHECK (length(id)=16),
    upload_id BLOB REFERENCES uploads(id) ON DELETE SET NULL,
    storage_id BLOB NOT NULL REFERENCES storages(id),
    source_id BLOB UNIQUE REFERENCES raw_sources(id),
    reclaim_pending INTEGER NOT NULL DEFAULT 0 CHECK (reclaim_pending IN (0,1)),
    state TEXT NOT NULL DEFAULT 'open' CHECK (state IN ('open','sealed','deleting'))
) STRICT, WITHOUT ROWID;
CREATE INDEX multipart_files_upload ON multipart_files(upload_id,state);
CREATE INDEX multipart_files_reclaim ON multipart_files(storage_id,id) WHERE reclaim_pending;
ALTER TABLE fragments ADD COLUMN file_id BLOB REFERENCES multipart_files(id);
ALTER TABLE fragments ADD COLUMN file_offset INTEGER CHECK ((file_id IS NULL)=(file_offset IS NULL) AND (file_offset IS NULL OR file_offset>=0));
CREATE INDEX fragments_file ON fragments(file_id,file_offset) WHERE file_id IS NOT NULL;

UPDATE mokyu_meta SET schema_version=22 WHERE id=1;
