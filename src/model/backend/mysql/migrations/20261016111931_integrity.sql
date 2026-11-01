CREATE TABLE IF NOT EXISTS integrity_issues (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    task_id BINARY(16) NOT NULL,
    subject VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    code VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    chunk_id BIGINT,
    location_id BINARY(16),
    stream_id BINARY(16),
    bucket_id BINARY(16),
    object_key VARBINARY(1024),
    detail JSON NOT NULL DEFAULT (JSON_OBJECT()),
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    CONSTRAINT integrity_issues_task FOREIGN KEY (task_id) REFERENCES tasks(id) ON DELETE CASCADE,
    UNIQUE INDEX integrity_issues_subject (task_id,subject,code),
    INDEX integrity_issues_page (task_id,id),
    INDEX integrity_issues_code (task_id,code,id)
) ENGINE=InnoDB;

UPDATE mokyu_meta SET schema_version=13 WHERE id=1;
