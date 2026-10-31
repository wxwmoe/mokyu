ALTER TABLE bucket_change_gates ADD COLUMN required_targets INTEGER NOT NULL DEFAULT 0 CHECK (required_targets>=0);
ALTER TABLE redundancy_links ADD COLUMN synchronous BOOLEAN NOT NULL DEFAULT 0,
    ADD CONSTRAINT redundancy_synchronous_local CHECK (synchronous=0 OR target_bucket IS NOT NULL);
CREATE INDEX redundancy_links_admission ON redundancy_links(source_bucket,synchronous,state,id);

CREATE TRIGGER redundancy_required_insert AFTER INSERT ON redundancy_links FOR EACH ROW
BEGIN
    IF NEW.synchronous AND NEW.state NOT IN ('pausing','paused','removing') THEN
        UPDATE bucket_change_gates SET required_targets=required_targets+1 WHERE bucket_id=NEW.source_bucket;
    END IF;
END;

CREATE TRIGGER redundancy_required_update AFTER UPDATE ON redundancy_links FOR EACH ROW
BEGIN
    IF (NEW.synchronous AND NEW.state NOT IN ('pausing','paused','removing')) <> (OLD.synchronous AND OLD.state NOT IN ('pausing','paused','removing')) THEN
        UPDATE bucket_change_gates SET required_targets=required_targets+IF(NEW.synchronous AND NEW.state NOT IN ('pausing','paused','removing'),1,-1) WHERE bucket_id=NEW.source_bucket;
    END IF;
END;

CREATE TRIGGER redundancy_required_delete AFTER DELETE ON redundancy_links FOR EACH ROW
BEGIN
    IF OLD.synchronous AND OLD.state NOT IN ('pausing','paused','removing') THEN
        UPDATE bucket_change_gates SET required_targets=required_targets-1 WHERE bucket_id=OLD.source_bucket;
    END IF;
END;

CREATE TABLE replica_publications (
    id BINARY(16) NOT NULL PRIMARY KEY,
    source_bucket BINARY(16) NOT NULL,
    object_key VARBINARY(1024) NOT NULL,
    source_pin BIGINT,
    phase VARCHAR(16) NOT NULL DEFAULT 'admitted' CHECK (phase IN ('admitted','preparing','published','aborted')),
    early_ack BOOLEAN NOT NULL DEFAULT 0,
    intent JSON,
    intent_hash BINARY(32),
    result JSON,
    created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    FOREIGN KEY (source_bucket) REFERENCES buckets(id),
    FOREIGN KEY (source_pin) REFERENCES streams(id),
    CHECK (OCTET_LENGTH(object_key) BETWEEN 1 AND 1024),
    CHECK (intent_hash IS NULL OR OCTET_LENGTH(intent_hash)=32)
);
CREATE INDEX replica_publications_pin ON replica_publications(source_pin);
CREATE INDEX replica_publications_recovery ON replica_publications(phase,updated_at,id);

CREATE TABLE replica_targets (
    publication_id BINARY(16) NOT NULL,
    link_id BINARY(16) NOT NULL,
    link_revision BIGINT NOT NULL CHECK (link_revision>0),
    target_bucket BINARY(16) NOT NULL,
    target_key VARBINARY(1024) NOT NULL,
    storage_id BINARY(16) NOT NULL,
    profile_revision BIGINT NOT NULL CHECK (profile_revision>0),
    authority JSON NOT NULL,
    permissions VARCHAR(16) NOT NULL,
    expected_version BIGINT,
    candidate BIGINT,
    task_id BINARY(16),
    phase VARCHAR(16) NOT NULL DEFAULT 'admitted' CHECK (phase IN ('admitted','preparing','prepared','published','aborted')),
    PRIMARY KEY (publication_id,link_id),
    FOREIGN KEY (publication_id) REFERENCES replica_publications(id) ON DELETE CASCADE,
    FOREIGN KEY (target_bucket) REFERENCES buckets(id),
    FOREIGN KEY (storage_id) REFERENCES storages(id),
    FOREIGN KEY (candidate) REFERENCES streams(id),
    FOREIGN KEY (task_id) REFERENCES tasks(id),
    CHECK (OCTET_LENGTH(target_key) BETWEEN 1 AND 1024)
);
CREATE INDEX replica_targets_link ON replica_targets(link_id,phase,publication_id);
CREATE UNIQUE INDEX replica_targets_task ON replica_targets(task_id);
CREATE INDEX replica_targets_candidate ON replica_targets(candidate);

ALTER TABLE streams ADD COLUMN publication_id BINARY(16);
ALTER TABLE streams ADD FOREIGN KEY (publication_id) REFERENCES replica_publications(id);
CREATE INDEX streams_publication ON streams(publication_id);

UPDATE mokyu_meta SET schema_version=52 WHERE id=1;
