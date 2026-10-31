-- Serialize namespace checks separately from object publication gates.
CREATE TABLE redundancy_catalog (
    singleton TINYINT PRIMARY KEY DEFAULT 1,
    CONSTRAINT redundancy_catalog_singleton CHECK (singleton=1)
);
INSERT INTO redundancy_catalog(singleton) VALUES(1);

CREATE TABLE redundancy_links (
    id BINARY(16) PRIMARY KEY,
    source_bucket BINARY(16) NOT NULL,
    target_bucket BINARY(16),
    external_bucket BINARY(16),
    target_prefix TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    source_domain BINARY(32) NOT NULL,
    source_space TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    target_domain BINARY(32) NOT NULL,
    target_space TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    settings JSON NOT NULL,
    revision BIGINT NOT NULL DEFAULT 1 CHECK (revision>0),
    scan_credit INTEGER NOT NULL DEFAULT 0 CHECK (scan_credit BETWEEN 0 AND 64),
    state VARCHAR(16) NOT NULL DEFAULT 'preparing' CHECK (state IN ('preparing','active','reconciling','catchup','idle','pausing','paused','removing')),
    reconcile_prefix TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    reconciled_at DATETIME(6),
    next_reconcile_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    FOREIGN KEY (id) REFERENCES transfer_runs(task_id),
    FOREIGN KEY (source_bucket) REFERENCES buckets(id),
    FOREIGN KEY (target_bucket) REFERENCES buckets(id),
    FOREIGN KEY (external_bucket) REFERENCES external_buckets(id),
    CHECK ((target_bucket IS NULL)<>(external_bucket IS NULL)),
    CHECK (target_bucket IS NULL OR source_bucket<>target_bucket),
    CHECK (OCTET_LENGTH(target_prefix)<=1024 AND OCTET_LENGTH(reconcile_prefix)<=1024)
);
CREATE INDEX redundancy_links_source ON redundancy_links(source_bucket,id);
CREATE UNIQUE INDEX redundancy_links_destination ON redundancy_links(source_bucket,target_domain);
CREATE INDEX redundancy_links_source_domain ON redundancy_links(source_domain);
CREATE INDEX redundancy_links_target_domain ON redundancy_links(target_domain);
CREATE INDEX redundancy_links_target ON redundancy_links(target_bucket);
CREATE INDEX redundancy_links_external ON redundancy_links(external_bucket);
CREATE INDEX redundancy_links_due ON redundancy_links(state,next_reconcile_at,id);

CREATE TABLE redundancy_backlogs (
    id BINARY(16) PRIMARY KEY NOT NULL,
    max_pending_keys INTEGER NOT NULL,
    queued_keys INTEGER NOT NULL DEFAULT 0,
    FOREIGN KEY (id) REFERENCES redundancy_links(id) ON DELETE CASCADE,
    CONSTRAINT redundancy_queue_capacity CHECK (max_pending_keys BETWEEN 1 AND 1000000),
    CONSTRAINT redundancy_queue_nonnegative CHECK (queued_keys >= 0),
    CONSTRAINT redundancy_queue_full CHECK (queued_keys <= max_pending_keys)
);

CREATE TRIGGER transfer_changes_backlog_insert AFTER INSERT ON transfer_changes FOR EACH ROW
BEGIN
    UPDATE redundancy_backlogs SET queued_keys=queued_keys+1 WHERE id=NEW.task_id;
END;

CREATE TRIGGER transfer_changes_backlog_delete AFTER DELETE ON transfer_changes FOR EACH ROW
BEGIN
    UPDATE redundancy_backlogs SET queued_keys=queued_keys-1 WHERE id=OLD.task_id;
END;

ALTER TABLE bucket_change_gates ADD COLUMN managed INTEGER NOT NULL DEFAULT 0 CHECK (managed>=0);
ALTER TABLE transfer_changes ADD COLUMN reconcile BOOLEAN NOT NULL DEFAULT FALSE;

ALTER TABLE transfer_items ADD COLUMN source_key_bytes VARBINARY(1024) GENERATED ALWAYS AS (CAST(source_key AS BINARY)) VIRTUAL,
    ADD INDEX transfer_items_key_order(task_id,source_key_bytes);
ALTER TABLE transfer_changes ADD COLUMN object_key_bytes VARBINARY(1024) GENERATED ALWAYS AS (CAST(object_key AS BINARY)) VIRTUAL,
    ADD INDEX transfer_changes_key_order(task_id,object_key_bytes);

ALTER TABLE transfer_runs ADD COLUMN redundancy_id BINARY(16),
    ADD CHECK (redundancy_id IS NULL OR redundancy_id=task_id);

UPDATE mokyu_meta SET schema_version=51 WHERE id=1;
