UPDATE bucket_tiering
SET observed_since = strftime('%Y-%m-%dT%H:%M:%fZ', (CAST(observed_since AS INTEGER) / 1000) / 1000.0, 'unixepoch')
WHERE length(observed_since) = 16 AND observed_since NOT GLOB '*[^0-9]*';

UPDATE mokyu_meta SET schema_version = 20261105234041;
