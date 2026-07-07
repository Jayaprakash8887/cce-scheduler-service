-- Schema for integration tests (mimics Compliance Service + Scheduler)

CREATE TABLE IF NOT EXISTS step_instance (
    id                    UUID PRIMARY KEY,
    protocol_instance_id  UUID NOT NULL,
    action_id             VARCHAR(255) NOT NULL,
    repeat_index          INTEGER NOT NULL DEFAULT 0,
    state                 VARCHAR(50) NOT NULL,
    due_date              TIMESTAMPTZ,
    overdue_date          TIMESTAMPTZ,
    missed_date           TIMESTAMPTZ,
    completed_at          TIMESTAMPTZ,
    required_behavior     VARCHAR(50),
    created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS scheduler_lease (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    partition_index  INTEGER NOT NULL DEFAULT 0,
    leader_id        VARCHAR,
    last_heartbeat   TIMESTAMPTZ,
    lease_expires_at TIMESTAMPTZ,
    CONSTRAINT uq_scheduler_lease_partition UNIQUE (partition_index)
);

INSERT INTO scheduler_lease (id, partition_index)
VALUES (gen_random_uuid(), 0)
ON CONFLICT (partition_index) DO NOTHING;

CREATE TABLE IF NOT EXISTS scheduler_partition_cursor (
    partition_index INTEGER PRIMARY KEY,
    watermark       TIMESTAMPTZ NOT NULL,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
