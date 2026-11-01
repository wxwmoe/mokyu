CREATE TABLE parts (
    upload_id BLOB CHECK (upload_id IS NULL OR length(upload_id)=16) NOT NULL,
    part_number INTEGER NOT NULL,
    stream_id BLOB CHECK (stream_id IS NULL OR length(stream_id)=16),
    write_token BLOB CHECK (write_token IS NULL OR length(write_token)=16) NOT NULL,
    quota_size INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (upload_id,part_number),
    CONSTRAINT parts_upload FOREIGN KEY (upload_id) REFERENCES uploads(id) ON DELETE CASCADE,
    CONSTRAINT parts_stream FOREIGN KEY (stream_id) REFERENCES streams(id),
    CONSTRAINT parts_number CHECK (part_number BETWEEN 1 AND 10000),
    CONSTRAINT parts_quota_size CHECK (quota_size>=0)
) STRICT, WITHOUT ROWID;

CREATE INDEX parts_stream_id_fk ON parts(stream_id);

CREATE TABLE web_uploads (
    upload_id BLOB CHECK (upload_id IS NULL OR length(upload_id)=16) NOT NULL PRIMARY KEY,
    user_id BLOB CHECK (user_id IS NULL OR length(user_id)=16),
    client_id BLOB CHECK (client_id IS NULL OR length(client_id)=16) NOT NULL,
    request_hash BLOB CHECK (request_hash IS NULL OR length(request_hash)=32) NOT NULL,
    file_name TEXT NOT NULL,
    expected_size INTEGER NOT NULL,
    part_size INTEGER NOT NULL,
    modified_at INTEGER,
    expected_stream_id BLOB CHECK (expected_stream_id IS NULL OR length(expected_stream_id)=16),
    CONSTRAINT web_uploads_upload FOREIGN KEY (upload_id) REFERENCES uploads(id) ON DELETE CASCADE,
    CONSTRAINT web_uploads_user FOREIGN KEY (user_id) REFERENCES web_users(id) ON DELETE SET NULL,
    CONSTRAINT web_uploads_size CHECK (expected_size BETWEEN 0 AND 53687091200000),
    CONSTRAINT web_uploads_part_size CHECK (part_size BETWEEN 5242880 AND 5368709120),
    CONSTRAINT web_uploads_name CHECK (length(CAST(file_name AS BLOB)) BETWEEN 1 AND 1024),
    CONSTRAINT web_uploads_modified CHECK (modified_at IS NULL OR modified_at>=0)
) STRICT, WITHOUT ROWID;

CREATE UNIQUE INDEX web_uploads_client ON web_uploads(user_id,client_id);

CREATE TABLE quota_reservations (
    id BLOB CHECK (id IS NULL OR length(id)=16) NOT NULL PRIMARY KEY,
    bucket_id BLOB CHECK (bucket_id IS NULL OR length(bucket_id)=16) NOT NULL,
    object_key BLOB NOT NULL,
    stream_id BLOB CHECK (stream_id IS NULL OR length(stream_id)=16) UNIQUE,
    upload_id BLOB CHECK (upload_id IS NULL OR length(upload_id)=16) UNIQUE,
    result_stream_id BLOB CHECK (result_stream_id IS NULL OR length(result_stream_id)=16) UNIQUE,
    closed INTEGER CHECK (closed IN (0,1)) NOT NULL DEFAULT FALSE,
    replaced_stream_id BLOB CHECK (replaced_stream_id IS NULL OR length(replaced_stream_id)=16),
    credit_bytes INTEGER NOT NULL DEFAULT 0,
    logical_bytes INTEGER NOT NULL DEFAULT 0,
    inflight_bytes INTEGER NOT NULL DEFAULT 0,
    CONSTRAINT reservations_bucket FOREIGN KEY (bucket_id) REFERENCES buckets(id),
    CONSTRAINT reservations_stream FOREIGN KEY (stream_id) REFERENCES streams(id),
    CONSTRAINT reservations_upload FOREIGN KEY (upload_id) REFERENCES uploads(id),
    CONSTRAINT reservations_result FOREIGN KEY (result_stream_id) REFERENCES streams(id) ON DELETE SET NULL,
    CONSTRAINT reservations_owner CHECK ((stream_id IS NULL)<>(upload_id IS NULL)),
    CONSTRAINT reservations_bytes CHECK (credit_bytes>=0 AND logical_bytes>=0 AND inflight_bytes>=0)
) STRICT, WITHOUT ROWID;

CREATE INDEX reservations_replaced ON quota_reservations(bucket_id,object_key,replaced_stream_id);

CREATE TABLE quota_writes (
    stream_id BLOB CHECK (stream_id IS NULL OR length(stream_id)=16) NOT NULL PRIMARY KEY,
    reservation_id BLOB CHECK (reservation_id IS NULL OR length(reservation_id)=16) NOT NULL,
    allocated_bytes INTEGER NOT NULL DEFAULT 0,
    contribution_bytes INTEGER NOT NULL DEFAULT 0,
    CONSTRAINT quota_writes_stream FOREIGN KEY (stream_id) REFERENCES streams(id),
    CONSTRAINT quota_writes_reservation FOREIGN KEY (reservation_id) REFERENCES quota_reservations(id) ON DELETE CASCADE,
    CONSTRAINT quota_writes_bytes CHECK (allocated_bytes>=0 AND contribution_bytes>=0)
) STRICT, WITHOUT ROWID;

CREATE INDEX quota_writes_reservation_id_fk ON quota_writes(reservation_id);

INSERT INTO quota_scopes(scope,scope_id,used_bytes,object_count,bucket_count)
SELECT 'project',p.id,
    COALESCE((SELECT SUM(s.size) FROM buckets b JOIN objects o ON o.bucket_id=b.id JOIN streams s ON s.id=o.stream_id WHERE b.project_id=p.id),0),
    (SELECT COUNT(*) FROM buckets b JOIN objects o ON o.bucket_id=b.id WHERE b.project_id=p.id AND o.stream_id IS NOT NULL),
    (SELECT COUNT(*) FROM buckets b WHERE b.project_id=p.id)
FROM projects p WHERE TRUE ON CONFLICT(scope,scope_id) DO UPDATE SET used_bytes=excluded.used_bytes,object_count=excluded.object_count,bucket_count=excluded.bucket_count;

INSERT INTO quota_scopes(scope,scope_id,used_bytes,object_count)
SELECT 'bucket',b.id,
    COALESCE((SELECT SUM(s.size) FROM objects o JOIN streams s ON s.id=o.stream_id WHERE o.bucket_id=b.id),0),
    (SELECT COUNT(*) FROM objects o WHERE o.bucket_id=b.id AND o.stream_id IS NOT NULL)
FROM buckets b WHERE TRUE ON CONFLICT(scope,scope_id) DO NOTHING;

UPDATE mokyu_meta SET schema_version=8 WHERE id=1;
