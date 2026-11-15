ALTER TABLE workflow_runs ADD COLUMN definition jsonb NOT NULL DEFAULT '{}';
ALTER TABLE tasks ADD COLUMN dependency_policy text NOT NULL DEFAULT 'fail' CHECK(dependency_policy IN ('fail','block'));
