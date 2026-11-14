CREATE TABLE storage_member_moves (
    id BLOB NOT NULL PRIMARY KEY,
    cluster_id BLOB NOT NULL,
    group_id BLOB NOT NULL,
    source_media BLOB NOT NULL,
    target_media BLOB NOT NULL,
    state TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    checked_at INTEGER NOT NULL DEFAULT 0,
    bytes_per_second INTEGER NOT NULL,
    CONSTRAINT storage_move_cluster FOREIGN KEY(cluster_id) REFERENCES storage_clusters(id),
    CONSTRAINT storage_move_state CHECK(state IN ('moving','paused','reverting','cleaning','undo_cleanup','completed','cancelled')),
    CONSTRAINT storage_move_members CHECK(source_media<>target_media AND bytes_per_second>0)
) STRICT, WITHOUT ROWID;

CREATE INDEX storage_move_due ON storage_member_moves(state,checked_at,id);
CREATE INDEX storage_move_source ON storage_member_moves(cluster_id,source_media,state);
CREATE INDEX storage_move_target ON storage_member_moves(cluster_id,target_media,state);

CREATE TABLE storage_backup_moves (
    job_id BLOB NOT NULL,
    backup_id BLOB NOT NULL,
    sequence INTEGER NOT NULL,
    shard INTEGER NOT NULL,
    receipt TEXT NOT NULL CHECK(json_valid(receipt)),
    copied INTEGER NOT NULL DEFAULT FALSE,
    removed INTEGER NOT NULL DEFAULT FALSE,
    PRIMARY KEY(job_id,backup_id,sequence,shard),
    CONSTRAINT storage_backup_move_job FOREIGN KEY(job_id) REFERENCES storage_member_moves(id),
    CONSTRAINT storage_backup_move_slot FOREIGN KEY(backup_id) REFERENCES storage_backups(id)
) STRICT, WITHOUT ROWID;

CREATE TABLE storage_cluster_runs (
    id BLOB NOT NULL PRIMARY KEY,
    cluster_id BLOB NOT NULL,
    media_id BLOB,
    revision INTEGER NOT NULL,
    layout TEXT NOT NULL,
    kind TEXT NOT NULL,
    state TEXT NOT NULL,
    cursor_id BLOB NOT NULL,
    ceiling BLOB NOT NULL,
    check_token BLOB,
    lease_until INTEGER NOT NULL DEFAULT 0,
    checked INTEGER NOT NULL DEFAULT 0,
    byte_count INTEGER NOT NULL DEFAULT 0,
    bytes_per_second INTEGER NOT NULL,
    last_error TEXT,
    backup_group TEXT,
    CONSTRAINT storage_run_cluster FOREIGN KEY(cluster_id) REFERENCES storage_clusters(id),
    CONSTRAINT storage_run_kind CHECK(kind IN ('rebuild','scrub','sync')),
    CONSTRAINT storage_run_state CHECK(state IN ('objects','snapshots','completed','cancelled'))
) STRICT, WITHOUT ROWID;

CREATE INDEX storage_run_due ON storage_cluster_runs(state,lease_until,id);
CREATE INDEX storage_run_cluster ON storage_cluster_runs(cluster_id,state);

CREATE TABLE storage_run_snapshots (
    run_id BLOB NOT NULL,
    storage_id BLOB NOT NULL,
    snapshot_id BLOB NOT NULL,
    PRIMARY KEY(run_id,storage_id),
    CONSTRAINT storage_run_snapshot_job FOREIGN KEY(run_id) REFERENCES storage_cluster_runs(id)
) STRICT, WITHOUT ROWID;

UPDATE mokyu_meta SET schema_version=20261114160500 WHERE id=1;
