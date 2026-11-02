ALTER TABLE nodes ADD COLUMN reported_at INTEGER;

CREATE TABLE node_runtime_samples (
    node_id BLOB NOT NULL REFERENCES nodes(id),
    received_at INTEGER NOT NULL,
    sample TEXT NOT NULL,
    PRIMARY KEY (node_id, received_at)
);
CREATE INDEX node_runtime_sample_time ON node_runtime_samples(received_at);
CREATE TABLE cluster_runtime_samples (
    captured_at INTEGER PRIMARY KEY,
    sample TEXT NOT NULL
);

UPDATE mokyu_meta SET schema_version = 20261103033436;
