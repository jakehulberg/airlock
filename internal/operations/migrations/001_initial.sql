CREATE TABLE agents (
 id text PRIMARY KEY, owner text NOT NULL, definition jsonb NOT NULL,
 updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE workflows (
 id text PRIMARY KEY, owner text NOT NULL, definition jsonb NOT NULL,
 updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE workflow_runs (
 id text PRIMARY KEY, owner text NOT NULL, workflow_id text NOT NULL REFERENCES workflows,
 state text NOT NULL DEFAULT 'running', input jsonb NOT NULL DEFAULT '{}',
 created_at timestamptz NOT NULL DEFAULT now(), finished_at timestamptz
);
CREATE TABLE tasks (
 id text PRIMARY KEY, owner text NOT NULL, title text NOT NULL, prompt text NOT NULL,
 agent_id text REFERENCES agents, run_id text REFERENCES workflow_runs,
 state text NOT NULL CHECK(state IN ('draft','queued','blocked','running','awaiting_approval','completed','failed','cancelled','uncertain')),
 priority integer NOT NULL DEFAULT 0 CHECK(priority BETWEEN -10 AND 10),
 max_attempts integer NOT NULL DEFAULT 3 CHECK(max_attempts BETWEEN 1 AND 10),
 timeout_seconds integer NOT NULL DEFAULT 1800 CHECK(timeout_seconds BETWEEN 1 AND 14400),
 attempt_count integer NOT NULL DEFAULT 0, available_at timestamptz NOT NULL DEFAULT now(),
 lease_until timestamptz, worker text, cancel_requested boolean NOT NULL DEFAULT false,
 approval_required boolean NOT NULL DEFAULT false, approved boolean NOT NULL DEFAULT false,
 idempotency_key text, created_at timestamptz NOT NULL DEFAULT now(), finished_at timestamptz,
 UNIQUE(owner,idempotency_key)
);
CREATE INDEX tasks_queue ON tasks(priority DESC,available_at) WHERE state='queued';
CREATE INDEX tasks_owner ON tasks(owner,created_at DESC);
CREATE TABLE dependencies (
 task_id text REFERENCES tasks ON DELETE CASCADE, dependency_id text REFERENCES tasks,
 PRIMARY KEY(task_id,dependency_id), CHECK(task_id <> dependency_id)
);
CREATE TABLE attempts (
 id text PRIMARY KEY, task_id text NOT NULL REFERENCES tasks, number integer NOT NULL,
 state text NOT NULL DEFAULT 'running', claim_name text NOT NULL, pod_name text NOT NULL DEFAULT '',
 pod_uid text NOT NULL DEFAULT '', sandbox_owner text NOT NULL, output text NOT NULL DEFAULT '',
 error text NOT NULL DEFAULT '', exit_code integer, started_at timestamptz NOT NULL DEFAULT now(),
 finished_at timestamptz, cleanup_pending boolean NOT NULL DEFAULT true,
 UNIQUE(task_id,number), CHECK(octet_length(output)<=1048576)
);
CREATE TABLE schedules (
 id text PRIMARY KEY, owner text NOT NULL, workflow_id text NOT NULL REFERENCES workflows,
 definition jsonb NOT NULL, next_run timestamptz, enabled boolean NOT NULL DEFAULT true,
 updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE schedule_fires (
 schedule_id text REFERENCES schedules, due_at timestamptz NOT NULL,
 run_id text REFERENCES workflow_runs, PRIMARY KEY(schedule_id,due_at)
);
CREATE TABLE messages (
 id text PRIMARY KEY, owner text NOT NULL, sender text NOT NULL, recipient text NOT NULL,
 task_id text NOT NULL REFERENCES tasks, run_id text REFERENCES workflow_runs,
 type text NOT NULL, payload jsonb NOT NULL, correlation_id text NOT NULL,
 status text NOT NULL DEFAULT 'pending' CHECK(status IN ('pending','processed')),
 created_at timestamptz NOT NULL DEFAULT now(), CHECK(octet_length(payload::text)<=65536)
);
CREATE TABLE approvals (
 id text PRIMARY KEY, task_id text NOT NULL REFERENCES tasks, owner text NOT NULL,
 decision text NOT NULL CHECK(decision IN ('approved','rejected')), reason text NOT NULL DEFAULT '',
 created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE activity (
 id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, owner text NOT NULL,
 kind text NOT NULL, reference text NOT NULL, detail text NOT NULL DEFAULT '',
 created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX activity_owner ON activity(owner,id DESC);
CREATE TABLE interactive_tasks (
 id text PRIMARY KEY, owner text NOT NULL, claim text NOT NULL, data jsonb NOT NULL,
 active boolean NOT NULL, updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX interactive_active_claim ON interactive_tasks(claim) WHERE active;
