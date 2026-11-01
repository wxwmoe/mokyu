
SET @mokyu_raw_upload_ddl = IF(EXISTS(SELECT 1 FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='raw_sources' AND COLUMN_NAME='pending_upload'), 'SELECT 1', 'ALTER TABLE raw_sources ADD COLUMN pending_upload BOOLEAN NOT NULL DEFAULT false');
PREPARE mokyu_raw_upload FROM @mokyu_raw_upload_ddl;
EXECUTE mokyu_raw_upload;
DEALLOCATE PREPARE mokyu_raw_upload;
CREATE TABLE IF NOT EXISTS raw_uploads (
    source_id BIGINT PRIMARY KEY,
    token BINARY(16),
    deduplicate BOOLEAN NOT NULL,
    background BOOLEAN NOT NULL,
    content_hash BINARY(32),
    next_attempt_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    INDEX raw_uploads_due(next_attempt_at,source_id),
    CONSTRAINT raw_uploads_source FOREIGN KEY(source_id) REFERENCES raw_sources(id) ON DELETE CASCADE
);
UPDATE mokyu_meta SET schema_version=28 WHERE id=1;
