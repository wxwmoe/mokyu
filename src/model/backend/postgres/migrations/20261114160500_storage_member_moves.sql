CREATE TABLE storage_member_moves (
    id UUID NOT NULL PRIMARY KEY,
    cluster_id UUID NOT NULL,
    group_id UUID NOT NULL,
    source_media UUID NOT NULL,
    target_media UUID NOT NULL,
    state TEXT NOT NULL,
    created_at BIGINT NOT NULL,
    checked_at BIGINT NOT NULL DEFAULT 0,
    bytes_per_second BIGINT NOT NULL,
    CONSTRAINT storage_move_cluster FOREIGN KEY(cluster_id) REFERENCES storage_clusters(id),
    CONSTRAINT storage_move_state CHECK(state IN ('moving','paused','reverting','cleaning','undo_cleanup','completed','cancelled')),
    CONSTRAINT storage_move_members CHECK(source_media<>target_media AND bytes_per_second>0)
);

CREATE INDEX storage_move_due ON storage_member_moves(state,checked_at,id);
CREATE INDEX storage_move_source ON storage_member_moves(cluster_id,source_media,state);
CREATE INDEX storage_move_target ON storage_member_moves(cluster_id,target_media,state);

CREATE TABLE storage_backup_moves (
    job_id UUID NOT NULL,
    backup_id UUID NOT NULL,
    sequence INTEGER NOT NULL,
    shard INTEGER NOT NULL,
    receipt JSONB NOT NULL,
    copied BOOLEAN NOT NULL DEFAULT FALSE,
    removed BOOLEAN NOT NULL DEFAULT FALSE,
    PRIMARY KEY(job_id,backup_id,sequence,shard),
    CONSTRAINT storage_backup_move_job FOREIGN KEY(job_id) REFERENCES storage_member_moves(id),
    CONSTRAINT storage_backup_move_slot FOREIGN KEY(backup_id) REFERENCES storage_backups(id)
);

CREATE TABLE storage_cluster_runs (
    id UUID NOT NULL PRIMARY KEY,
    cluster_id UUID NOT NULL,
    media_id UUID,
    revision BIGINT NOT NULL,
    layout JSONB NOT NULL,
    kind TEXT NOT NULL,
    state TEXT NOT NULL,
    cursor_id UUID NOT NULL,
    ceiling UUID NOT NULL,
    check_token UUID,
    lease_until BIGINT NOT NULL DEFAULT 0,
    checked BIGINT NOT NULL DEFAULT 0,
    byte_count BIGINT NOT NULL DEFAULT 0,
    bytes_per_second BIGINT NOT NULL,
    last_error TEXT,
    backup_group TEXT,
    CONSTRAINT storage_run_cluster FOREIGN KEY(cluster_id) REFERENCES storage_clusters(id),
    CONSTRAINT storage_run_kind CHECK(kind IN ('rebuild','scrub','sync')),
    CONSTRAINT storage_run_state CHECK(state IN ('objects','snapshots','completed','cancelled'))
);

CREATE INDEX storage_run_due ON storage_cluster_runs(state,lease_until,id);
CREATE INDEX storage_run_cluster ON storage_cluster_runs(cluster_id,state);

CREATE TABLE storage_run_snapshots (
    run_id UUID NOT NULL,
    storage_id UUID NOT NULL,
    snapshot_id UUID NOT NULL,
    PRIMARY KEY(run_id,storage_id),
    CONSTRAINT storage_run_snapshot_job FOREIGN KEY(run_id) REFERENCES storage_cluster_runs(id)
);

UPDATE mokyu_meta SET schema_version=20261114160500 WHERE singleton;
