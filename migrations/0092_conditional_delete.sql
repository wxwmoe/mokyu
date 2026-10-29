ALTER TABLE node_object_changes DROP CONSTRAINT node_object_changes_outcome_check;
ALTER TABLE node_object_changes ADD CONSTRAINT node_object_changes_outcome_check
    CHECK (outcome IN ('"applied"'::jsonb, '"no_such_key"'::jsonb, '"precondition_failed"'::jsonb));

UPDATE mokyu_meta SET schema_version = 92 WHERE singleton;
