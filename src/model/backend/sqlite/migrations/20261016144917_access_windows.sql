CREATE TABLE access_state (
    id INTEGER NOT NULL PRIMARY KEY,
    coverage_since INTEGER,
    flushed_at INTEGER,
    CONSTRAINT access_state_singleton CHECK (id=1)
) STRICT, WITHOUT ROWID;



INSERT OR IGNORE INTO access_state(id,coverage_since,flushed_at)
VALUES(1,CAST(unixepoch('subsec')*1000000 AS INTEGER),CAST(unixepoch('subsec')*1000000 AS INTEGER));

CREATE TABLE chunk_access_windows (
    chunk_id INTEGER NOT NULL,
    window_start INTEGER NOT NULL,
    read_count INTEGER NOT NULL DEFAULT 0,
    range_read_count INTEGER NOT NULL DEFAULT 0,
    read_bytes INTEGER NOT NULL DEFAULT 0,
    range_origin_reads INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (chunk_id,window_start),
    CONSTRAINT chunk_access_source FOREIGN KEY (chunk_id) REFERENCES chunks(id) ON DELETE CASCADE
) STRICT, WITHOUT ROWID;

CREATE INDEX chunk_access_expiry ON chunk_access_windows(window_start);

CREATE TABLE pack_access_windows (
    pack_id INTEGER NOT NULL,
    window_start INTEGER NOT NULL,
    downloads INTEGER NOT NULL DEFAULT 0,
    downloaded_bytes INTEGER NOT NULL DEFAULT 0,
    partial_downloads INTEGER NOT NULL DEFAULT 0,
    useful_bytes INTEGER NOT NULL DEFAULT 0,
    partial_bytes INTEGER NOT NULL DEFAULT 0,
    updated_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec')*1000000 AS INTEGER)),
    PRIMARY KEY (pack_id,window_start),
    CONSTRAINT pack_access_source FOREIGN KEY (pack_id) REFERENCES packs(id) ON DELETE CASCADE
) STRICT, WITHOUT ROWID;

CREATE INDEX pack_access_expiry ON pack_access_windows(window_start);

CREATE TABLE pack_member_access_windows (
    pack_id INTEGER NOT NULL,
    window_start INTEGER NOT NULL,
    chunk_id INTEGER NOT NULL,
    downloads INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (pack_id,window_start,chunk_id),
    CONSTRAINT pack_member_access_pack FOREIGN KEY (pack_id) REFERENCES packs(id) ON DELETE CASCADE,
    CONSTRAINT pack_member_access_chunk FOREIGN KEY (chunk_id) REFERENCES chunks(id) ON DELETE CASCADE
) STRICT, WITHOUT ROWID;

CREATE INDEX pack_member_access_expiry ON pack_member_access_windows(window_start);
CREATE INDEX pack_member_access_windows_chunk_id_fk ON pack_member_access_windows(chunk_id);

UPDATE mokyu_meta SET schema_version=13 WHERE id=1;
