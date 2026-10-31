-- Serialize namespace checks separately from object publication gates.
CREATE TABLE redundancy_catalog (
    singleton INTEGER PRIMARY KEY DEFAULT 1 CHECK (singleton=1)
);
INSERT INTO redundancy_catalog DEFAULT VALUES;

CREATE TABLE redundancy_links (
    id BLOB NOT NULL PRIMARY KEY REFERENCES transfer_runs(task_id),
    source_bucket BLOB NOT NULL REFERENCES buckets(id),
    target_bucket BLOB REFERENCES buckets(id),
    external_bucket BLOB REFERENCES external_buckets(id),
    target_prefix TEXT NOT NULL,
    source_domain BLOB NOT NULL,
    source_space TEXT NOT NULL,
    target_domain BLOB NOT NULL,
    target_space TEXT NOT NULL,
    settings TEXT NOT NULL,
    revision BIGINT NOT NULL DEFAULT 1 CHECK (revision>0),
    scan_credit INTEGER NOT NULL DEFAULT 0 CHECK (scan_credit BETWEEN 0 AND 64),
    state TEXT NOT NULL DEFAULT 'preparing' CHECK (state IN ('preparing','active','reconciling','catchup','idle','pausing','paused','removing')),
    reconcile_prefix TEXT NOT NULL DEFAULT '',
    reconciled_at TEXT,
    next_reconcile_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    CHECK ((target_bucket IS NULL)<>(external_bucket IS NULL)),
    CHECK (target_bucket IS NULL OR source_bucket<>target_bucket),
    CHECK (length(CAST(target_prefix AS BLOB))<=1024 AND length(CAST(reconcile_prefix AS BLOB))<=1024)
);
CREATE INDEX redundancy_links_source ON redundancy_links(source_bucket,id);
CREATE UNIQUE INDEX redundancy_links_destination ON redundancy_links(source_bucket,target_domain);
CREATE INDEX redundancy_links_source_domain ON redundancy_links(source_domain);
CREATE INDEX redundancy_links_target_domain ON redundancy_links(target_domain);
CREATE INDEX redundancy_links_target ON redundancy_links(target_bucket);
CREATE INDEX redundancy_links_external ON redundancy_links(external_bucket);
CREATE INDEX redundancy_links_due ON redundancy_links(state,next_reconcile_at,id);

CREATE TABLE redundancy_backlogs (
    id BLOB PRIMARY KEY NOT NULL,
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
ALTER TABLE transfer_changes ADD COLUMN reconcile INTEGER NOT NULL DEFAULT 0 CHECK (reconcile IN (0,1));

ALTER TABLE transfer_runs ADD COLUMN redundancy_id BLOB CHECK (redundancy_id IS NULL OR redundancy_id=task_id);

UPDATE mokyu_meta SET schema_version=52 WHERE id=1;
