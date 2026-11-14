PRAGMA legacy_alter_table=ON;

CREATE TABLE raw_sources_next (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    location_id BLOB NOT NULL UNIQUE CHECK(length(location_id)=16),
    storage_id BLOB NOT NULL REFERENCES storages(id),
    owner_stream INTEGER REFERENCES streams(id) ON DELETE SET NULL,
    medium TEXT NOT NULL DEFAULT 'local' CHECK (medium IN ('local','memory','shared')),
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
    unreferenced_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec')*1000000 AS INTEGER)), pending_upload INTEGER NOT NULL DEFAULT false, processing_task BLOB REFERENCES tasks(id) ON DELETE SET NULL, page_count INTEGER CHECK (page_count >= 0),
    CHECK (state<>'ready' OR digest IS NOT NULL),
    CHECK ((medium='memory')=(boot_id IS NOT NULL))
) STRICT;

INSERT INTO raw_sources_next(id,location_id,storage_id,owner_stream,medium,durability,boot_id,state,size,digest,content_hash,upload_id,revision,profile_revision,quarantined_at,quarantine_code,created_at,unreferenced_at,pending_upload,processing_task,page_count)
SELECT id,location_id,storage_id,owner_stream,CASE WHEN medium='shared_s3' THEN 'shared' ELSE medium END,durability,boot_id,state,size,digest,content_hash,upload_id,revision,profile_revision,quarantined_at,quarantine_code,created_at,unreferenced_at,pending_upload,processing_task,page_count FROM raw_sources;
UPDATE sqlite_sequence SET seq=MAX(seq,(SELECT seq FROM sqlite_sequence WHERE name='raw_sources')) WHERE name='raw_sources_next';
DROP TABLE raw_sources;
ALTER TABLE raw_sources_next RENAME TO raw_sources;

CREATE INDEX raw_sources_cleanup ON raw_sources(storage_id,state,unreferenced_at,id);
CREATE UNIQUE INDEX raw_sources_content ON raw_sources(storage_id,content_hash,size);
CREATE INDEX raw_sources_owner ON raw_sources(owner_stream) WHERE owner_stream IS NOT NULL;
CREATE INDEX raw_sources_processing ON raw_sources(processing_task);
CREATE INDEX raw_sources_sync ON raw_sources(id) WHERE durability IN ('buffered','volatile') AND state='ready' AND quarantine_code IS NULL;

CREATE TRIGGER raw_sources_notice_update AFTER UPDATE ON raw_sources
WHEN OLD.quarantine_code IS NOT NEW.quarantine_code
BEGIN
    INSERT INTO notice_changes VALUES('storage',NEW.storage_id,X'');
END;

PRAGMA legacy_alter_table=OFF;
UPDATE mokyu_meta SET schema_version=20261113183012 WHERE id=1;
