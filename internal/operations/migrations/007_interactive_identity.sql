ALTER TABLE interactive_tasks ADD COLUMN pod_name text NOT NULL DEFAULT '';
ALTER TABLE interactive_tasks ADD COLUMN pod_uid text NOT NULL DEFAULT '';
