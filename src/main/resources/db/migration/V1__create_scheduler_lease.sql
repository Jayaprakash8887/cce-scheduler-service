-- Leader heartbeat (single-leader model): a single row keyed by id = 0. The advisory
-- lock is the leadership mechanism; this row is heartbeat/observability bookkeeping
-- updated (upserted) by the current leader.
CREATE TABLE IF NOT EXISTS scheduler_lease (
    id               INTEGER PRIMARY KEY,
    leader_id        VARCHAR,
    last_heartbeat   TIMESTAMPTZ,
    lease_expires_at TIMESTAMPTZ
);
