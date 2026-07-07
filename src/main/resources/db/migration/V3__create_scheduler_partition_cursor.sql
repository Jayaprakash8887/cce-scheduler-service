-- Per-partition scan watermark (Logstash-style bookmark).
-- One row per partition; the scan considers only step_instance rows whose
-- state-specific threshold is STRICTLY GREATER than this watermark, and
-- SchedulerLoop advances it to the largest threshold emitted after a fully
-- successful publish cycle. Written only by the partition's current owner
-- (serialized by the advisory lock), so no cross-writer contention.
--
-- KNOWN GAPS (accepted here, addressed elsewhere):
--   (a) A step created with a threshold below the current watermark
--       (backfill / backdated start / clock skew) is never scanned.
--   (b) A trigger that is published to Kafka but never applied by the
--       Compliance Service is not re-driven (state stays frozen below the
--       watermark). Transient publish FAILURES are still retried because the
--       watermark only advances when every trigger in the cycle succeeds.
--   (c) If MORE THAN batch-size steps share an identical threshold timestamp,
--       the overflow beyond the batch boundary at that exact instant is
--       skipped. Mitigation: keep batch-size above the largest expected
--       same-instant cohort (relevant when thresholds are calendar dates).
CREATE TABLE IF NOT EXISTS scheduler_partition_cursor (
    partition_index INTEGER PRIMARY KEY,
    watermark       TIMESTAMPTZ NOT NULL,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
