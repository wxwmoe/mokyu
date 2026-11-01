CREATE TABLE IF NOT EXISTS parts (
    upload_id BINARY(16) NOT NULL,
    part_number INT NOT NULL,
    stream_id BINARY(16),
    write_token BINARY(16) NOT NULL,
    quota_size BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (upload_id,part_number),
    CONSTRAINT parts_upload FOREIGN KEY (upload_id) REFERENCES uploads(id) ON DELETE CASCADE,
    CONSTRAINT parts_stream FOREIGN KEY (stream_id) REFERENCES streams(id),
    CONSTRAINT parts_number CHECK (part_number BETWEEN 1 AND 10000),
    CONSTRAINT parts_quota_size CHECK (quota_size>=0)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS web_uploads (
    upload_id BINARY(16) NOT NULL PRIMARY KEY,
    user_id BINARY(16),
    client_id BINARY(16) NOT NULL,
    request_hash BINARY(32) NOT NULL,
    file_name VARCHAR(1024) NOT NULL,
    expected_size BIGINT NOT NULL,
    part_size BIGINT NOT NULL,
    modified_at BIGINT,
    expected_stream_id BINARY(16),
    CONSTRAINT web_uploads_upload FOREIGN KEY (upload_id) REFERENCES uploads(id) ON DELETE CASCADE,
    CONSTRAINT web_uploads_user FOREIGN KEY (user_id) REFERENCES web_users(id) ON DELETE SET NULL,
    CONSTRAINT web_uploads_size CHECK (expected_size BETWEEN 0 AND 53687091200000),
    CONSTRAINT web_uploads_part_size CHECK (part_size BETWEEN 5242880 AND 5368709120),
    CONSTRAINT web_uploads_name CHECK (OCTET_LENGTH(file_name) BETWEEN 1 AND 1024),
    CONSTRAINT web_uploads_modified CHECK (modified_at IS NULL OR modified_at>=0),
    UNIQUE INDEX web_uploads_client (user_id,client_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 /*!80000 COLLATE=utf8mb4_0900_bin */ /*M!100603 COLLATE=utf8mb4_nopad_bin */;

CREATE TABLE IF NOT EXISTS quota_scopes (
    scope VARCHAR(7) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    scope_id BINARY(16) NOT NULL,
    used_bytes BIGINT NOT NULL DEFAULT 0,
    reserved_bytes BIGINT NOT NULL DEFAULT 0,
    inflight_bytes BIGINT NOT NULL DEFAULT 0,
    object_count BIGINT NOT NULL DEFAULT 0,
    bucket_count BIGINT NOT NULL DEFAULT 0,
    logical_bytes_limit BIGINT,
    inflight_bytes_limit BIGINT,
    bucket_count_limit BIGINT,
    PRIMARY KEY (scope,scope_id),
    CONSTRAINT quota_scope CHECK (scope IN ('project','bucket')),
    CONSTRAINT quota_bytes CHECK (used_bytes>=0 AND reserved_bytes>=0 AND inflight_bytes>=0),
    CONSTRAINT quota_counts CHECK (object_count>=0 AND bucket_count>=0),
    CONSTRAINT quota_limits CHECK ((logical_bytes_limit IS NULL OR logical_bytes_limit>=0) AND (inflight_bytes_limit IS NULL OR inflight_bytes_limit>=0) AND (bucket_count_limit IS NULL OR bucket_count_limit>=0))
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS quota_reservations (
    id BINARY(16) NOT NULL PRIMARY KEY,
    bucket_id BINARY(16) NOT NULL,
    object_key VARBINARY(1024) NOT NULL,
    stream_id BINARY(16) UNIQUE,
    upload_id BINARY(16) UNIQUE,
    result_stream_id BINARY(16) UNIQUE,
    closed BOOLEAN NOT NULL DEFAULT FALSE,
    replaced_stream_id BINARY(16),
    credit_bytes BIGINT NOT NULL DEFAULT 0,
    logical_bytes BIGINT NOT NULL DEFAULT 0,
    inflight_bytes BIGINT NOT NULL DEFAULT 0,
    CONSTRAINT reservations_bucket FOREIGN KEY (bucket_id) REFERENCES buckets(id),
    CONSTRAINT reservations_stream FOREIGN KEY (stream_id) REFERENCES streams(id),
    CONSTRAINT reservations_upload FOREIGN KEY (upload_id) REFERENCES uploads(id),
    CONSTRAINT reservations_result FOREIGN KEY (result_stream_id) REFERENCES streams(id) ON DELETE SET NULL,
    CONSTRAINT reservations_owner CHECK ((stream_id IS NULL)<>(upload_id IS NULL)),
    CONSTRAINT reservations_bytes CHECK (credit_bytes>=0 AND logical_bytes>=0 AND inflight_bytes>=0),
    INDEX reservations_replaced (bucket_id,object_key,replaced_stream_id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS quota_writes (
    stream_id BINARY(16) NOT NULL PRIMARY KEY,
    reservation_id BINARY(16) NOT NULL,
    allocated_bytes BIGINT NOT NULL DEFAULT 0,
    contribution_bytes BIGINT NOT NULL DEFAULT 0,
    CONSTRAINT quota_writes_stream FOREIGN KEY (stream_id) REFERENCES streams(id),
    CONSTRAINT quota_writes_reservation FOREIGN KEY (reservation_id) REFERENCES quota_reservations(id) ON DELETE CASCADE,
    CONSTRAINT quota_writes_bytes CHECK (allocated_bytes>=0 AND contribution_bytes>=0)
) ENGINE=InnoDB;

INSERT INTO quota_scopes(scope,scope_id,used_bytes,object_count,bucket_count)
SELECT 'project',p.id,
    COALESCE((SELECT SUM(s.size) FROM buckets b JOIN objects o ON o.bucket_id=b.id JOIN streams s ON s.id=o.stream_id WHERE b.project_id=p.id),0),
    (SELECT COUNT(*) FROM buckets b JOIN objects o ON o.bucket_id=b.id WHERE b.project_id=p.id AND o.stream_id IS NOT NULL),
    (SELECT COUNT(*) FROM buckets b WHERE b.project_id=p.id)
FROM projects p ON DUPLICATE KEY UPDATE scope_id=quota_scopes.scope_id;

INSERT INTO quota_scopes(scope,scope_id,used_bytes,object_count)
SELECT 'bucket',b.id,
    COALESCE((SELECT SUM(s.size) FROM objects o JOIN streams s ON s.id=o.stream_id WHERE o.bucket_id=b.id),0),
    (SELECT COUNT(*) FROM objects o WHERE o.bucket_id=b.id AND o.stream_id IS NOT NULL)
FROM buckets b ON DUPLICATE KEY UPDATE scope_id=quota_scopes.scope_id;

UPDATE mokyu_meta SET schema_version=7 WHERE id=1;
