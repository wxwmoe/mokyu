-- Serialize namespace checks separately from object publication gates.
CREATE TABLE redundancy_catalog (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton)
);
INSERT INTO redundancy_catalog DEFAULT VALUES;

CREATE TABLE redundancy_links (
    id uuid PRIMARY KEY REFERENCES transfer_runs(task_id),
    source_bucket uuid NOT NULL REFERENCES buckets(id),
    target_bucket uuid REFERENCES buckets(id),
    external_bucket uuid REFERENCES external_buckets(id),
    target_prefix text COLLATE "C" NOT NULL,
    source_domain bytea NOT NULL,
    source_space text COLLATE "C" NOT NULL,
    target_domain bytea NOT NULL,
    target_space text COLLATE "C" NOT NULL,
    settings jsonb NOT NULL,
    revision bigint NOT NULL DEFAULT 1 CHECK (revision>0),
    scan_credit INTEGER NOT NULL DEFAULT 0 CHECK (scan_credit BETWEEN 0 AND 64),
    state text NOT NULL DEFAULT 'preparing' CHECK (state IN ('preparing','active','reconciling','catchup','idle','pausing','paused','removing')),
    reconcile_prefix text COLLATE "C" NOT NULL DEFAULT '',
    reconciled_at timestamptz,
    next_reconcile_at timestamptz NOT NULL DEFAULT now(),
    CHECK ((target_bucket IS NULL)<>(external_bucket IS NULL)),
    CHECK (target_bucket IS NULL OR source_bucket<>target_bucket),
    CHECK (octet_length(target_prefix)<=1024 AND octet_length(reconcile_prefix)<=1024)
);
CREATE INDEX redundancy_links_source ON redundancy_links(source_bucket,id);
CREATE UNIQUE INDEX redundancy_links_destination ON redundancy_links(source_bucket,target_domain);
CREATE INDEX redundancy_links_source_domain ON redundancy_links(source_domain);
CREATE INDEX redundancy_links_target_domain ON redundancy_links(target_domain);
CREATE INDEX redundancy_links_target ON redundancy_links(target_bucket);
CREATE INDEX redundancy_links_external ON redundancy_links(external_bucket);
CREATE INDEX redundancy_links_due ON redundancy_links(state,next_reconcile_at,id);

CREATE TABLE redundancy_backlogs (
    id uuid PRIMARY KEY NOT NULL,
    max_pending_keys INTEGER NOT NULL,
    queued_keys INTEGER NOT NULL DEFAULT 0,
    FOREIGN KEY (id) REFERENCES redundancy_links(id) ON DELETE CASCADE,
    CONSTRAINT redundancy_queue_capacity CHECK (max_pending_keys BETWEEN 1 AND 1000000),
    CONSTRAINT redundancy_queue_nonnegative CHECK (queued_keys >= 0),
    CONSTRAINT redundancy_queue_full CHECK (queued_keys <= max_pending_keys)
);

CREATE FUNCTION count_redundancy_changes() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        UPDATE redundancy_backlogs SET queued_keys=queued_keys+1 WHERE id=NEW.task_id;
    ELSE
        UPDATE redundancy_backlogs SET queued_keys=queued_keys-1 WHERE id=OLD.task_id;
    END IF;
    RETURN NULL;
END;
$$;
CREATE TRIGGER transfer_changes_backlog AFTER INSERT OR DELETE ON transfer_changes
    FOR EACH ROW EXECUTE FUNCTION count_redundancy_changes();

ALTER TABLE bucket_change_gates ADD COLUMN managed INTEGER NOT NULL DEFAULT 0 CHECK (managed>=0);
ALTER TABLE transfer_items ALTER COLUMN source_key TYPE text COLLATE "C";
ALTER TABLE transfer_changes ADD COLUMN reconcile BOOLEAN NOT NULL DEFAULT FALSE;

ALTER TABLE transfer_runs ADD COLUMN redundancy_id uuid CHECK (redundancy_id IS NULL OR redundancy_id=task_id);

UPDATE mokyu_meta SET schema_version=98;
