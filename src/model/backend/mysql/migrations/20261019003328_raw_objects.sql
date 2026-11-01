ALTER TABLE storage_profiles ADD COLUMN cdc_enabled boolean NOT NULL DEFAULT TRUE;
ALTER TABLE storage_profiles ADD COLUMN deduplication_enabled boolean NOT NULL DEFAULT FALSE;
ALTER TABLE streams ADD COLUMN payload_type varchar(6) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT 'chunks' CHECK (payload_type IN ('chunks','raw'));
ALTER TABLE streams ADD COLUMN payload_revision bigint NOT NULL DEFAULT 1 CHECK (payload_revision>0);

CREATE TABLE raw_sources (
    id binary(16) NOT NULL PRIMARY KEY,
    storage_id binary(16) NOT NULL,
    owner_stream binary(16),
    medium varchar(10) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT 'local' CHECK (medium IN ('local','memory','shared_s3')),
    durability varchar(8) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT 'durable' CHECK (durability IN ('durable','buffered','volatile')),
    boot_id binary(16),
    state varchar(11) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT 'writing' CHECK (state IN ('writing','ready','unavailable','deleting')),
    size bigint NOT NULL DEFAULT 0 CHECK (size>=0),
    digest binary(16),
    content_hash binary(32),
    upload_id text,
    revision bigint NOT NULL DEFAULT 1 CHECK (revision>0),
    profile_revision bigint NOT NULL CHECK (profile_revision>0),
    quarantined_at datetime(6),
    quarantine_code varchar(32) CHARACTER SET ascii COLLATE ascii_bin,
    created_at datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    unreferenced_at datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    CONSTRAINT raw_sources_storage FOREIGN KEY (storage_id) REFERENCES storages(id),
    CONSTRAINT raw_sources_owner FOREIGN KEY (owner_stream) REFERENCES streams(id) ON DELETE SET NULL,
    CHECK (state<>'ready' OR digest IS NOT NULL),
    CHECK ((medium='memory')=(boot_id IS NOT NULL)),
    INDEX raw_sources_cleanup(storage_id,state,unreferenced_at,id)
) ENGINE=InnoDB;
CREATE UNIQUE INDEX raw_sources_content ON raw_sources(storage_id,content_hash,size);
CREATE TABLE raw_ranges (
    stream_id binary(16) NOT NULL,
    offset_bytes bigint NOT NULL CHECK (offset_bytes>=0),
    length bigint NOT NULL CHECK (length>=0),
    source_id binary(16) NOT NULL,
    source_offset bigint NOT NULL DEFAULT 0 CHECK (source_offset>=0),
    PRIMARY KEY (stream_id,offset_bytes),
    CONSTRAINT raw_ranges_stream FOREIGN KEY (stream_id) REFERENCES streams(id) ON DELETE CASCADE,
    CONSTRAINT raw_ranges_source FOREIGN KEY (source_id) REFERENCES raw_sources(id)
) ENGINE=InnoDB;
CREATE TABLE multipart_files (
    id binary(16) NOT NULL PRIMARY KEY,
    upload_id binary(16),
    storage_id binary(16) NOT NULL,
    source_id binary(16) UNIQUE,
    reclaim_pending boolean NOT NULL DEFAULT false,
    state varchar(8) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT 'open' CHECK (state IN ('open','sealed','deleting')),
    FOREIGN KEY (upload_id) REFERENCES uploads(id) ON DELETE SET NULL,
    FOREIGN KEY (storage_id) REFERENCES storages(id),
    FOREIGN KEY (source_id) REFERENCES raw_sources(id),
    INDEX multipart_files_upload (upload_id,state),
    INDEX multipart_files_reclaim (reclaim_pending,storage_id,id)
) ENGINE=InnoDB;
ALTER TABLE fragments ADD COLUMN file_id binary(16), ADD COLUMN file_offset bigint,
    ADD FOREIGN KEY (file_id) REFERENCES multipart_files(id),
    ADD CHECK ((file_id IS NULL)=(file_offset IS NULL) AND (file_offset IS NULL OR file_offset>=0)),
    ADD INDEX fragments_file (file_id,file_offset);

UPDATE mokyu_meta SET schema_version=21 WHERE id=1;
