-- The scheduler_node registry existed only to compute each pod's fair share of
-- multiple logical partitions. The service now runs a single logical partition
-- with single-leader election (one advisory lock), so the registry is unused.
DROP TABLE IF EXISTS scheduler_node;
