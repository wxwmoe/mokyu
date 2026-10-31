ALTER TABLE bucket_change_gates ADD COLUMN required_targets INTEGER NOT NULL DEFAULT 0 CHECK (required_targets>=0);
ALTER TABLE redundancy_links ADD COLUMN synchronous INTEGER NOT NULL DEFAULT 0 CHECK (synchronous IN (0,1)) CHECK (NOT synchronous OR target_bucket IS NOT NULL);
CREATE INDEX redundancy_links_admission ON redundancy_links(source_bucket,synchronous,state,id);

CREATE TRIGGER redundancy_required_insert AFTER INSERT ON redundancy_links WHEN NEW.synchronous AND NEW.state NOT IN ('pausing','paused','removing')
BEGIN
    UPDATE bucket_change_gates SET required_targets=required_targets+1 WHERE bucket_id=NEW.source_bucket;
END;

CREATE TRIGGER redundancy_required_update AFTER UPDATE OF synchronous,state ON redundancy_links WHEN (NEW.synchronous AND NEW.state NOT IN ('pausing','paused','removing')) <> (OLD.synchronous AND OLD.state NOT IN ('pausing','paused','removing'))
BEGIN
    UPDATE bucket_change_gates SET required_targets=required_targets+CASE WHEN NEW.synchronous AND NEW.state NOT IN ('pausing','paused','removing') THEN 1 ELSE -1 END WHERE bucket_id=NEW.source_bucket;
END;

CREATE TRIGGER redundancy_required_delete AFTER DELETE ON redundancy_links WHEN OLD.synchronous AND OLD.state NOT IN ('pausing','paused','removing')
BEGIN
    UPDATE bucket_change_gates SET required_targets=required_targets-1 WHERE bucket_id=OLD.source_bucket;
END;

CREATE TABLE replica_publications (
    id BLOB NOT NULL PRIMARY KEY,
    source_bucket BLOB NOT NULL,
    object_key BLOB NOT NULL,
    source_pin INTEGER,
    phase text NOT NULL DEFAULT 'admitted' CHECK (phase IN ('admitted','preparing','published','aborted')),
    early_ack INTEGER NOT NULL DEFAULT 0 CHECK (early_ack IN (0,1)),
    intent TEXT,
    intent_hash BLOB,
    result TEXT,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    FOREIGN KEY (source_bucket) REFERENCES buckets(id),
    FOREIGN KEY (source_pin) REFERENCES streams(id),
    CHECK (length(object_key) BETWEEN 1 AND 1024),
    CHECK (intent_hash IS NULL OR length(intent_hash)=32)
);
CREATE INDEX replica_publications_pin ON replica_publications(source_pin);
CREATE INDEX replica_publications_recovery ON replica_publications(phase,updated_at,id);

CREATE TABLE replica_targets (
    publication_id BLOB NOT NULL,
    link_id BLOB NOT NULL,
    link_revision BIGINT NOT NULL CHECK (link_revision>0),
    target_bucket BLOB NOT NULL,
    target_key BLOB NOT NULL,
    storage_id BLOB NOT NULL,
    profile_revision BIGINT NOT NULL CHECK (profile_revision>0),
    authority TEXT NOT NULL,
    permissions text NOT NULL,
    expected_version INTEGER,
    candidate INTEGER,
    task_id BLOB,
    phase text NOT NULL DEFAULT 'admitted' CHECK (phase IN ('admitted','preparing','prepared','published','aborted')),
    PRIMARY KEY (publication_id,link_id),
    FOREIGN KEY (publication_id) REFERENCES replica_publications(id) ON DELETE CASCADE,
    FOREIGN KEY (target_bucket) REFERENCES buckets(id),
    FOREIGN KEY (storage_id) REFERENCES storages(id),
    FOREIGN KEY (candidate) REFERENCES streams(id),
    FOREIGN KEY (task_id) REFERENCES tasks(id),
    CHECK (length(target_key) BETWEEN 1 AND 1024)
);
CREATE INDEX replica_targets_link ON replica_targets(link_id,phase,publication_id);
CREATE UNIQUE INDEX replica_targets_task ON replica_targets(task_id);
CREATE INDEX replica_targets_candidate ON replica_targets(candidate);

ALTER TABLE streams ADD COLUMN publication_id BLOB REFERENCES replica_publications(id);
CREATE INDEX streams_publication ON streams(publication_id) WHERE publication_id IS NOT NULL;

UPDATE mokyu_meta SET schema_version=53 WHERE id=1;
