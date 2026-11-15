ALTER TABLE attempts ADD COLUMN token_hash text;
ALTER TABLE attempts ADD COLUMN token_expires timestamptz;
CREATE UNIQUE INDEX attempts_token ON attempts(token_hash) WHERE token_hash IS NOT NULL;
ALTER TABLE tasks ADD COLUMN parent_id text REFERENCES tasks;
ALTER TABLE tasks ADD COLUMN root_id text;
ALTER TABLE tasks ADD COLUMN depth integer NOT NULL DEFAULT 0 CHECK(depth BETWEEN 0 AND 3);
CREATE INDEX tasks_root ON tasks(root_id);
