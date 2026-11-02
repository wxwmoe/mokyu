ALTER TABLE nodes ADD COLUMN reported_at DATETIME(6);

CREATE TABLE node_runtime_samples (
    node_id BINARY(16) NOT NULL,
    received_at DATETIME(6) NOT NULL,
    sample JSON NOT NULL,
    PRIMARY KEY (node_id, received_at),
    FOREIGN KEY (node_id) REFERENCES nodes(id)
);
CREATE INDEX node_runtime_sample_time ON node_runtime_samples(received_at);
CREATE TABLE cluster_runtime_samples (
    captured_at DATETIME(6) PRIMARY KEY,
    sample JSON NOT NULL
);

UPDATE mokyu_meta SET schema_version = 20261103033436;
