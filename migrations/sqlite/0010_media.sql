CREATE TABLE media_operations (
    id BLOB CHECK (id IS NULL OR length(id)=16) NOT NULL PRIMARY KEY,
    user_id BLOB CHECK (user_id IS NULL OR length(user_id)=16) NOT NULL,
    token_id BLOB CHECK (token_id IS NULL OR length(token_id)=16),
    client_id BLOB CHECK (client_id IS NULL OR length(client_id)=16) NOT NULL,
    request_hash BLOB CHECK (request_hash IS NULL OR length(request_hash)=32) NOT NULL,
    result TEXT CHECK (json_valid(result)),
    created_at INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec')*1000000 AS INTEGER)),
    CONSTRAINT media_operations_user FOREIGN KEY (user_id) REFERENCES web_users(id) ON DELETE CASCADE
) STRICT, WITHOUT ROWID;

CREATE INDEX media_operations_cleanup ON media_operations(created_at,id);
CREATE INDEX media_operations_user_id_fk ON media_operations(user_id);

CREATE TABLE chunk_access_stats (
    chunk_id INTEGER NOT NULL PRIMARY KEY,
    read_count INTEGER NOT NULL DEFAULT 0,
    range_read_count INTEGER NOT NULL DEFAULT 0,
    read_bytes INTEGER NOT NULL DEFAULT 0,
    last_read_at INTEGER NOT NULL,
    CONSTRAINT chunk_access_stats_chunk FOREIGN KEY (chunk_id) REFERENCES chunks(id) ON DELETE CASCADE,
    CONSTRAINT chunk_access_stats_counts CHECK (read_count>=0 AND range_read_count>=0 AND read_bytes>=0)
) STRICT, WITHOUT ROWID;



CREATE UNIQUE INDEX media_operations_user_client ON media_operations(user_id,client_id) WHERE token_id IS NULL;
CREATE UNIQUE INDEX media_operations_token_client ON media_operations(user_id,token_id,client_id) WHERE token_id IS NOT NULL;

UPDATE mokyu_meta SET schema_version=10 WHERE id=1;
