-- Retention queries and reference checks must not scan unrelated live records.
CREATE INDEX chunks_deleted ON chunks(deleted_at,id) WHERE state='deleted';
CREATE INDEX uploads_finished ON uploads(touched_at,id) WHERE state IN ('completed','aborted');
CREATE INDEX tasks_completed ON tasks(updated_at,id) WHERE state='completed';
CREATE INDEX objects_empty ON objects(bucket_id,key) WHERE stream_id IS NULL;
CREATE INDEX streams_writing ON streams(bucket_id,object_key) WHERE state='writing';
CREATE INDEX parts_stream ON parts(stream_id) WHERE stream_id IS NOT NULL;
CREATE INDEX uploads_output ON uploads(output_stream) WHERE output_stream IS NOT NULL;
CREATE INDEX fragments_cleanup ON fragments(created_at,id);

-- Keep deleted tuple reuse and planner statistics responsive on growing tables.
ALTER TABLE chunks SET (autovacuum_vacuum_scale_factor=0.05, autovacuum_analyze_scale_factor=0.02);
ALTER TABLE extents SET (autovacuum_vacuum_scale_factor=0.05, autovacuum_analyze_scale_factor=0.02);
ALTER TABLE streams SET (autovacuum_vacuum_scale_factor=0.05, autovacuum_analyze_scale_factor=0.02);
ALTER TABLE objects SET (autovacuum_vacuum_scale_factor=0.05, autovacuum_analyze_scale_factor=0.02);
ALTER TABLE uploads SET (autovacuum_vacuum_scale_factor=0.05, autovacuum_analyze_scale_factor=0.02);
ALTER TABLE parts SET (autovacuum_vacuum_scale_factor=0.05, autovacuum_analyze_scale_factor=0.02);
ALTER TABLE fragments SET (autovacuum_vacuum_scale_factor=0.05, autovacuum_analyze_scale_factor=0.02);
ALTER TABLE sessions SET (autovacuum_vacuum_scale_factor=0.05, autovacuum_analyze_scale_factor=0.02);
ALTER TABLE tasks SET (autovacuum_vacuum_scale_factor=0.05, autovacuum_analyze_scale_factor=0.02);

UPDATE gateway_meta SET schema_version=2;
