CREATE TABLE IF NOT EXISTS media_operations (
    id BINARY(16) NOT NULL PRIMARY KEY,
    user_id BINARY(16) NOT NULL,
    token_id BINARY(16),
    token_scope BINARY(17) GENERATED ALWAYS AS (CONCAT(IF(token_id IS NULL,0x00,0x01),IFNULL(token_id,0x00000000000000000000000000000000))) STORED,
    client_id BINARY(16) NOT NULL,
    request_hash BINARY(32) NOT NULL,
    result JSON,
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    CONSTRAINT media_operations_user FOREIGN KEY (user_id) REFERENCES web_users(id) ON DELETE CASCADE,
    UNIQUE INDEX media_operations_client (user_id,token_scope,client_id),
    INDEX media_operations_cleanup (created_at,id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS chunk_access_stats (
    chunk_id BIGINT NOT NULL PRIMARY KEY,
    read_count BIGINT NOT NULL DEFAULT 0,
    range_read_count BIGINT NOT NULL DEFAULT 0,
    read_bytes BIGINT NOT NULL DEFAULT 0,
    last_read_at DATETIME(6) NOT NULL,
    CONSTRAINT chunk_access_stats_chunk FOREIGN KEY (chunk_id) REFERENCES chunks(id) ON DELETE CASCADE,
    CONSTRAINT chunk_access_stats_counts CHECK (read_count>=0 AND range_read_count>=0 AND read_bytes>=0)
) ENGINE=InnoDB;

UPDATE mokyu_meta SET schema_version=9 WHERE id=1;
