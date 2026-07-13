-- Single-leader schema. One active leader (single advisory lock) scans the whole table.

-- Retire the live-instance registry from the earlier multi-instance model.
DROP TABLE IF EXISTS scheduler_node;

-- The lease is a singleton (one leader). Re-key the legacy column created by V1.
ALTER TABLE scheduler_lease RENAME COLUMN partition_index TO singleton;
ALTER TABLE scheduler_lease RENAME CONSTRAINT uq_scheduler_lease_partition TO uq_scheduler_lease_singleton;

-- Single-row scan cursor: a keyset/seek cursor over the composite key (threshold, step id).
-- The scan considers only step_instance rows that sort strictly after (watermark, watermark_id)
-- — i.e. eff_threshold > watermark, or eff_threshold = watermark AND id > watermark_id.
-- SchedulerLoop advances it to the last emitted (threshold, id) after a fully successful
-- publish cycle. The id (a UUID v7) breaks threshold ties so a same-instant cohort larger
-- than batch-size drains across cycles instead of being truncated. Written only by the
-- leader (serialized by the advisory lock).
--
-- KNOWN GAPS (accepted; addressed elsewhere):
--   (a) A step whose (threshold, id) sorts below the current cursor (backfill / backdated
--       start / clock skew, or a late crossing at exactly the watermark instant with
--       id < watermark_id) is never scanned.
--   (b) A trigger published to Kafka but never applied by the Compliance Service is not
--       re-driven. Transient publish FAILURES are still retried because the cursor only
--       advances when every trigger in the cycle succeeds.
CREATE TABLE IF NOT EXISTS scheduler_scan_cursor (
    id           INTEGER PRIMARY KEY,
    watermark    TIMESTAMPTZ NOT NULL,
    watermark_id UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000000',
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Partial indexes supporting the watermark-bounded scan. Each matches a state branch of
-- findDueSteps, ordered by that state's effective threshold then id, so the (threshold, id)
-- keyset seek is fully index-ordered. step_instance is owned by the Compliance Service:
-- guarded by a table-exists check (and CREATE INDEX IF NOT EXISTS) so this is idempotent and
-- safe where step_instance is not present. Verify after deploy with:
--   EXPLAIN (ANALYZE, BUFFERS) <the findDueSteps query>
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'step_instance') THEN
        CREATE INDEX IF NOT EXISTS idx_step_instance_pending_due
            ON step_instance (due_date, id) WHERE state = 'PENDING';
        CREATE INDEX IF NOT EXISTS idx_step_instance_due_overdue
            ON step_instance (overdue_date, id) WHERE state = 'DUE';
        CREATE INDEX IF NOT EXISTS idx_step_instance_overdue_missed
            ON step_instance (missed_date, id) WHERE state = 'OVERDUE';
    END IF;
END $$;
