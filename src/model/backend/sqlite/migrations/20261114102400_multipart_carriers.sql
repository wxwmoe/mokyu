CREATE TABLE multipart_carriers (
    location_id BLOB NOT NULL PRIMARY KEY,
    upload_id BLOB REFERENCES uploads(id) ON DELETE SET NULL,
    stream_id INTEGER REFERENCES streams(id) ON DELETE SET NULL,
    storage_id BLOB NOT NULL REFERENCES storages(id),
    created_at INTEGER NOT NULL,
    UNIQUE(upload_id,stream_id)
) STRICT, WITHOUT ROWID;
CREATE INDEX multipart_carriers_storage ON multipart_carriers(storage_id,created_at);
CREATE INDEX multipart_carriers_stream ON multipart_carriers(stream_id);

UPDATE mokyu_meta SET schema_version=20261114102400 WHERE id=1;
