-- Registry of live scheduler pods (including standbys that own no partitions).
-- Used to compute each pod's fair share of partitions so a single pod that
-- boots first cannot greedily hold every partition.
CREATE TABLE IF NOT EXISTS scheduler_node (
    node_id        VARCHAR PRIMARY KEY,
    last_heartbeat TIMESTAMPTZ NOT NULL,
    owned_count    INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_scheduler_node_heartbeat
    ON scheduler_node (last_heartbeat);
