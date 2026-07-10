-- Partial indexes supporting the scheduler's watermark-bounded scan. Each matches a
-- state branch of findDueSteps, ordered by that state's effective threshold then id,
-- so the (threshold, id) keyset seek is fully index-ordered.
--
-- step_instance is owned by the Compliance Service. These are guarded by a table-exists
-- check (and CREATE INDEX IF NOT EXISTS) so this migration is idempotent and safe in
-- scheduler databases where step_instance is not present yet, and coexists with any
-- Compliance-side indexes. After deploy, verify with:
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
