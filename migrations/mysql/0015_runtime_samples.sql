CREATE TABLE IF NOT EXISTS runtime_samples (
    sampled_at DATETIME(6) NOT NULL PRIMARY KEY,
    data JSON NOT NULL
) ENGINE=InnoDB;

UPDATE mokyu_meta SET schema_version=15 WHERE id=1;
