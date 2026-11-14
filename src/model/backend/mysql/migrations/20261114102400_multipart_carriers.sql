CREATE TABLE multipart_carriers (
    location_id BINARY(16) NOT NULL PRIMARY KEY,
    upload_id BINARY(16),
    stream_id BIGINT,
    storage_id BINARY(16) NOT NULL,
    created_at BIGINT NOT NULL,
    UNIQUE(upload_id,stream_id),
    FOREIGN KEY (upload_id) REFERENCES uploads(id) ON DELETE SET NULL,
    FOREIGN KEY (stream_id) REFERENCES streams(id) ON DELETE SET NULL,
    FOREIGN KEY (storage_id) REFERENCES storages(id)
) ENGINE=InnoDB;
CREATE INDEX multipart_carriers_storage ON multipart_carriers(storage_id,created_at);
CREATE INDEX multipart_carriers_stream ON multipart_carriers(stream_id);

UPDATE mokyu_meta SET schema_version=20261114102400 WHERE id=1;
