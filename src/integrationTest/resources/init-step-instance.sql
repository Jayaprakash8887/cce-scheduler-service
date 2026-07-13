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
    id               INTEGER PRIMARY KEY,
    leader_id        VARCHAR,
    last_heartbeat   TIMESTAMPTZ,
    lease_expires_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS scheduler_scan_cursor (
    id           INTEGER PRIMARY KEY,
    watermark    TIMESTAMPTZ NOT NULL,
    watermark_id UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000000',
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
