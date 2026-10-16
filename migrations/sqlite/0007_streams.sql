CREATE TABLE objects (
    bucket_id BLOB CHECK (bucket_id IS NULL OR length(bucket_id)=16) NOT NULL,
    object_key BLOB NOT NULL,
    stream_id BLOB CHECK (stream_id IS NULL OR length(stream_id)=16),
    write_token BLOB CHECK (write_token IS NULL OR length(write_token)=16) NOT NULL,
    catalog_size INTEGER,
    catalog_modified INTEGER,
    catalog_type TEXT,
    catalog_category TEXT,
    catalog_public INTEGER CHECK (catalog_public IN (0,1)),
    PRIMARY KEY (bucket_id,object_key),
    CONSTRAINT objects_bucket FOREIGN KEY (bucket_id) REFERENCES buckets(id),
    CONSTRAINT objects_stream FOREIGN KEY (stream_id) REFERENCES streams(id),
    CONSTRAINT objects_key CHECK (length(CAST(object_key AS BLOB)) BETWEEN 1 AND 1024)
) STRICT, WITHOUT ROWID;

CREATE INDEX objects_catalog_size ON objects(bucket_id,catalog_size,object_key);
CREATE INDEX objects_catalog_modified ON objects(bucket_id,catalog_modified,object_key);
CREATE INDEX objects_stream_id_fk ON objects(stream_id);

CREATE TABLE fragments (
    id BLOB CHECK (id IS NULL OR length(id)=16) NOT NULL PRIMARY KEY,
    owner_stream BLOB CHECK (owner_stream IS NULL OR length(owner_stream)=16),
    sealed INTEGER CHECK (sealed IN (0,1)) NOT NULL DEFAULT FALSE,
    size INTEGER NOT NULL,
    hash BLOB CHECK (hash IS NULL OR length(hash)=32) NOT NULL,
    created_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec')*1000000 AS INTEGER)),
    CONSTRAINT fragments_stream FOREIGN KEY (owner_stream) REFERENCES streams(id) ON DELETE SET NULL,
    CONSTRAINT fragments_size CHECK (size BETWEEN 1 AND 4194304)
) STRICT, WITHOUT ROWID;

CREATE INDEX fragments_cleanup ON fragments(created_at,id);
CREATE INDEX fragments_owner_stream_fk ON fragments(owner_stream);

CREATE TABLE extents (
    stream_id BLOB CHECK (stream_id IS NULL OR length(stream_id)=16) NOT NULL,
    offset_bytes INTEGER NOT NULL,
    length INTEGER NOT NULL,
    chunk_id INTEGER,
    fragment_id BLOB CHECK (fragment_id IS NULL OR length(fragment_id)=16),
    source_offset INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (stream_id,offset_bytes),
    CONSTRAINT extents_stream FOREIGN KEY (stream_id) REFERENCES streams(id) ON DELETE CASCADE,
    CONSTRAINT extents_chunk FOREIGN KEY (chunk_id) REFERENCES chunks(id),
    CONSTRAINT extents_fragment FOREIGN KEY (fragment_id) REFERENCES fragments(id),
    CONSTRAINT extents_source CHECK ((chunk_id IS NULL)<>(fragment_id IS NULL)),
    CONSTRAINT extents_length CHECK (length BETWEEN 1 AND 4194304),
    CONSTRAINT extents_offset CHECK (offset_bytes>=0),
    CONSTRAINT extents_source_offset CHECK (source_offset>=0 AND source_offset+length<=4194304)
) STRICT, WITHOUT ROWID;

CREATE INDEX extents_chunk_id_fk ON extents(chunk_id);
CREATE INDEX extents_fragment_id_fk ON extents(fragment_id);

CREATE TABLE uploads (
    id BLOB CHECK (id IS NULL OR length(id)=16) NOT NULL PRIMARY KEY,
    bucket_id BLOB CHECK (bucket_id IS NULL OR length(bucket_id)=16) NOT NULL,
    object_key BLOB NOT NULL,
    access_key TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'active',
    metadata TEXT CHECK (json_valid(metadata)) NOT NULL DEFAULT (JSON_OBJECT()),
    public_read INTEGER CHECK (public_read IN (0,1)) NOT NULL DEFAULT FALSE,
    checksum_algorithm TEXT,
    checksum_type TEXT,
    manifest_hash TEXT,
    result TEXT CHECK (json_valid(result)),
    output_stream BLOB CHECK (output_stream IS NULL OR length(output_stream)=16),
    created_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec')*1000000 AS INTEGER)),
    touched_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec')*1000000 AS INTEGER)),
    durable_at INTEGER,
    CONSTRAINT uploads_bucket FOREIGN KEY (bucket_id) REFERENCES buckets(id),
    CONSTRAINT uploads_output FOREIGN KEY (output_stream) REFERENCES streams(id),
    CONSTRAINT uploads_key CHECK (length(CAST(object_key AS BLOB)) BETWEEN 1 AND 1024),
    CONSTRAINT uploads_state CHECK (state IN ('active','completing','completed','aborted'))
) STRICT, WITHOUT ROWID;

CREATE INDEX uploads_listing ON uploads(bucket_id,object_key,id);
CREATE INDEX uploads_cleanup ON uploads(state,touched_at,id);
CREATE INDEX uploads_output_stream_fk ON uploads(output_stream);

CREATE TABLE cache_pins (
    chunk_id INTEGER NOT NULL,
    pin_type TEXT NOT NULL,
    owner_id BLOB CHECK (owner_id IS NULL OR length(owner_id)=16) NOT NULL,
    created_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec')*1000000 AS INTEGER)),
    PRIMARY KEY (chunk_id,pin_type,owner_id),
    CONSTRAINT cache_pins_chunk FOREIGN KEY (chunk_id) REFERENCES chunks(id),
    CONSTRAINT cache_pins_type CHECK (pin_type IN ('upload','pack'))
) STRICT, WITHOUT ROWID;

CREATE INDEX cache_pins_owner ON cache_pins(owner_id,chunk_id);

CREATE TABLE pack_members (
    pack_id INTEGER NOT NULL,
    ordinal INTEGER NOT NULL,
    chunk_id INTEGER NOT NULL,
    offset_bytes INTEGER NOT NULL,
    PRIMARY KEY (pack_id,ordinal),
    CONSTRAINT members_pack FOREIGN KEY (pack_id) REFERENCES packs(id) ON DELETE CASCADE,
    CONSTRAINT members_chunk FOREIGN KEY (chunk_id) REFERENCES chunks(id),
    CONSTRAINT members_ordinal CHECK (ordinal BETWEEN 0 AND 4095),
    CONSTRAINT members_offset CHECK (offset_bytes>=0)
) STRICT, WITHOUT ROWID;

CREATE UNIQUE INDEX pack_members_chunk ON pack_members(pack_id,chunk_id);
CREATE INDEX pack_members_chunk_id_fk ON pack_members(chunk_id);

CREATE TABLE integrity_packs (
    task_id BLOB CHECK (task_id IS NULL OR length(task_id)=16) NOT NULL,
    pack_id INTEGER NOT NULL,
    error_code TEXT,
    PRIMARY KEY (task_id,pack_id),
    CONSTRAINT integrity_packs_task FOREIGN KEY (task_id) REFERENCES tasks(id) ON DELETE CASCADE,
    CONSTRAINT integrity_packs_pack FOREIGN KEY (pack_id) REFERENCES packs(id) ON DELETE CASCADE
) STRICT, WITHOUT ROWID;

CREATE INDEX integrity_packs_pack_id_fk ON integrity_packs(pack_id);

UPDATE mokyu_meta SET schema_version=7 WHERE id=1;
