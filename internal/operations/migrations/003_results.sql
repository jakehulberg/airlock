ALTER TABLE attempts ADD COLUMN result jsonb;
CREATE TABLE engine_health (
 component text PRIMARY KEY,
 last_success timestamptz NOT NULL
);
