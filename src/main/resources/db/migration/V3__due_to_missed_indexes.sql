-- The DUE → OVERDUE transition is removed: a DUE step now goes straight to MISSED (or
-- SKIPPED) when missed_date is reached, and OVERDUE is no longer part of the lifecycle.
-- Realigns the partial indexes with the new findDueSteps branches:
--   PENDING → due_date, DUE → missed_date
-- The old DUE/overdue_date and OVERDUE/missed_date indexes no longer match any branch.
-- step_instance is owned by the Compliance Service: guarded by a table-exists check (and
-- IF EXISTS / IF NOT EXISTS) so this is idempotent and safe where step_instance is absent.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'step_instance') THEN
        DROP INDEX IF EXISTS idx_step_instance_due_overdue;
        DROP INDEX IF EXISTS idx_step_instance_overdue_missed;

        CREATE INDEX IF NOT EXISTS idx_step_instance_due_missed
            ON step_instance (missed_date, id) WHERE state = 'DUE';
    END IF;
END $$;
