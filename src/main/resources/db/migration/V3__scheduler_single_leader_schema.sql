-- Per-partition scan watermark (Logstash-style bookmark), as a keyset/seek
-- cursor over the composite key (threshold, step_id). The scan considers only
-- step_instance rows that sort strictly after (watermark, watermark_id) —
-- i.e. eff_threshold > watermark, or eff_threshold = watermark AND id > watermark_id.
-- SchedulerLoop advances the cursor to the last emitted (threshold, id) after a
-- fully successful publish cycle. Because id breaks threshold ties, a cohort of
-- steps sharing an identical threshold (e.g. calendar-date schedules, bulk
-- backfills) drains across cycles batch-size at a time — no truncation, no stall.
--
-- step_instance.id is a UUID v7 (time-ordered by creation), so within a
-- same-threshold cohort the id tie-break drains rows in creation order (FIFO),
-- and an index on (threshold, id) stays compact. NOTE: the id's embedded time is
-- CREATION time, not the threshold — so id is only a tie-breaker UNDER threshold,
-- never the primary cursor (creation order != due order would strand steps).
--
-- Written only by the partition's current owner (serialized by the advisory
-- lock), so no cross-writer contention.
--
-- KNOWN GAPS (accepted here, addressed elsewhere):
--   (a) A step whose (threshold, id) sorts below the current cursor (backfill /
--       backdated start / clock skew, or a late crossing at exactly the
--       watermark instant with id < watermark_id) is never scanned.
--   (b) A trigger that is published to Kafka but never applied by the
--       Compliance Service is not re-driven (state stays frozen below the
--       cursor). Transient publish FAILURES are still retried because the
--       cursor only advances when every trigger in the cycle succeeds.
CREATE TABLE IF NOT EXISTS scheduler_partition_cursor (
    partition_index INTEGER PRIMARY KEY,
    watermark       TIMESTAMPTZ NOT NULL,
    watermark_id    UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000000',
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
