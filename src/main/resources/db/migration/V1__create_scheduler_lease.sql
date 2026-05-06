CREATE TABLE IF NOT EXISTS scheduler_lease (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    partition_index  INTEGER NOT NULL DEFAULT 0,
    leader_id        VARCHAR,
    last_heartbeat   TIMESTAMPTZ,
    lease_expires_at TIMESTAMPTZ,
    CONSTRAINT uq_scheduler_lease_partition UNIQUE (partition_index)
);

-- Insert default partition row (single-leader mode)
INSERT INTO scheduler_lease (id, partition_index)
VALUES (gen_random_uuid(), 0)
ON CONFLICT (partition_index) DO NOTHING;
