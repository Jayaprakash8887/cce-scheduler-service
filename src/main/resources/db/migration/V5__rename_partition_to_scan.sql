-- Single-leader / single logical partition: remove the "partition" concept from names.
-- The cursor becomes a single-row table keyed by a fixed id (0); the lease keeps a
-- singleton marker.

ALTER TABLE scheduler_partition_cursor RENAME TO scheduler_scan_cursor;
ALTER TABLE scheduler_scan_cursor RENAME COLUMN partition_index TO id;

ALTER TABLE scheduler_lease RENAME COLUMN partition_index TO singleton;
ALTER TABLE scheduler_lease RENAME CONSTRAINT uq_scheduler_lease_partition TO uq_scheduler_lease_singleton;
