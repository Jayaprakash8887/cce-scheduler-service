# Insights Pre-Computation Optimization

> **CCE Scheduler Service** — Pre-computed transition metrics for the Insights Service  
> **Status**: Proposed | **Target**: v1.2.0  
> **Last Updated**: 2026-05-11

---

## 1. Problem Statement

The Scheduler Service drives time-based state transitions (`PENDING → DUE → OVERDUE → MISSED/SKIPPED`) for step instances. The Insights Service queries `step_instance` for state distribution, overdue trends, and missed-step analysis. These queries scan the full `step_instance` table on every dashboard load.

Currently, the Scheduler Service has **no persistent record** of transitions it has triggered — it publishes Kafka events and moves on. This means:

1. **The Insights Service cannot query transition history** — e.g., "how many steps transitioned to OVERDUE in the last 7 days?" requires scanning `step_instance.updated_at` and inferring state changes
2. **Step state distribution** requires `GROUP BY state` on the full `step_instance` table (potentially hundreds of thousands of rows)
3. **Trend analysis** (e.g., overdue rate over time) is impossible without a transition log

---

## 2. Optimizations Owned by Scheduler Service

### 2.1 New Table: `transition_log`

**Problem:** The Insights Service cannot query "when did each step transition to its current state?" without scanning the full table and inferring from timestamps. Deviation trends by date require joining `deviation` with `step_instance` and grouping by `detected_at`.

**Solution:** The Scheduler Service persists a log entry for each state transition it triggers, creating a queryable audit trail.

**Schema:**

```sql
CREATE TABLE transition_log (
    id                   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    step_instance_id     UUID NOT NULL,
    protocol_instance_id UUID NOT NULL,
    action_id            VARCHAR(200) NOT NULL,
    from_state           VARCHAR(20) NOT NULL,  -- PENDING, DUE, OVERDUE
    to_state             VARCHAR(20) NOT NULL,   -- DUE, OVERDUE, MISSED, SKIPPED
    transition_type      VARCHAR(30) NOT NULL,   -- PENDING_TO_DUE, DUE_TO_OVERDUE, OVERDUE_TO_MISSED
    transitioned_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    partition_index      INTEGER NOT NULL        -- scheduler partition that triggered this
);

CREATE INDEX idx_transition_log_step ON transition_log (step_instance_id);
CREATE INDEX idx_transition_log_time ON transition_log (transitioned_at);
CREATE INDEX idx_transition_log_type ON transition_log (transition_type);
CREATE INDEX idx_transition_log_protocol ON transition_log (protocol_instance_id);
```

**Update trigger:** In `SchedulerLoop.executeCycle()`, after successfully publishing the Kafka trigger event for each step, also persist a `transition_log` entry in the same cycle.

**Insights Service query enablement:**

| New Capability | Query |
|----------------|-------|
| Transition trends over time | `SELECT DATE(transitioned_at), transition_type, COUNT(*) FROM transition_log GROUP BY 1, 2` |
| Steps transitioned to OVERDUE in date range | `SELECT COUNT(*) FROM transition_log WHERE transition_type = 'DUE_TO_OVERDUE' AND transitioned_at BETWEEN ? AND ?` |
| Avg time in DUE before OVERDUE | `SELECT AVG(EXTRACT(EPOCH FROM (tl2.transitioned_at - tl1.transitioned_at))/86400) FROM transition_log tl1 JOIN transition_log tl2 ON tl1.step_instance_id = tl2.step_instance_id WHERE tl1.transition_type = 'PENDING_TO_DUE' AND tl2.transition_type = 'DUE_TO_OVERDUE'` |
| Missed step volume by action | `SELECT action_id, COUNT(*) FROM transition_log WHERE to_state = 'MISSED' GROUP BY action_id` |

---

### 2.2 New Table: `step_state_snapshot`

**Problem:** The Insights Service computes step state distribution (`PENDING`, `DUE`, `OVERDUE`, `MISSED`, `COMPLETED`, `SKIPPED` counts) by running `GROUP BY state` on the full `step_instance` table. At scale (100K+ steps), this is expensive and runs on every dashboard load.

**Solution:** The Scheduler Service maintains a periodic snapshot of state distribution, updated at the end of each scan cycle.

**Schema:**

```sql
CREATE TABLE step_state_snapshot (
    id                     UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    protocol_definition_id UUID,               -- NULL for global snapshot
    facility_id            VARCHAR(100),        -- NULL for global/protocol-level snapshot
    pending_count          INTEGER NOT NULL DEFAULT 0,
    due_count              INTEGER NOT NULL DEFAULT 0,
    overdue_count          INTEGER NOT NULL DEFAULT 0,
    missed_count           INTEGER NOT NULL DEFAULT 0,
    completed_count        INTEGER NOT NULL DEFAULT 0,
    skipped_count          INTEGER NOT NULL DEFAULT 0,
    total_count            INTEGER NOT NULL DEFAULT 0,
    snapshot_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (protocol_definition_id, facility_id)
);
```

**Update trigger:** At the end of each `SchedulerLoop.executeCycle()` (after all transitions for the current partition), recalculate the global state counts:

```sql
INSERT INTO step_state_snapshot (protocol_definition_id, facility_id, pending_count, due_count, overdue_count, missed_count, completed_count, skipped_count, total_count, snapshot_at)
SELECT
    NULL, NULL,
    COUNT(*) FILTER (WHERE si.state = 'PENDING'),
    COUNT(*) FILTER (WHERE si.state = 'DUE'),
    COUNT(*) FILTER (WHERE si.state = 'OVERDUE'),
    COUNT(*) FILTER (WHERE si.state = 'MISSED'),
    COUNT(*) FILTER (WHERE si.state = 'COMPLETED'),
    COUNT(*) FILTER (WHERE si.state = 'SKIPPED'),
    COUNT(*),
    now()
FROM step_instance si
ON CONFLICT (protocol_definition_id, facility_id)
DO UPDATE SET
    pending_count = EXCLUDED.pending_count,
    due_count = EXCLUDED.due_count,
    overdue_count = EXCLUDED.overdue_count,
    missed_count = EXCLUDED.missed_count,
    completed_count = EXCLUDED.completed_count,
    skipped_count = EXCLUDED.skipped_count,
    total_count = EXCLUDED.total_count,
    snapshot_at = now();
```

> **Trade-off:** The snapshot is eventually consistent — it reflects state distribution as of the last scan cycle (default: every 5 seconds). For dashboard purposes, this is acceptable. The Insights Service can show `snapshot_at` to indicate data freshness.

**Insights Service query replacement:**

| Current Query | Replacement |
|---------------|-------------|
| Step state distribution (`GROUP BY state` on step_instance) | `SELECT * FROM step_state_snapshot WHERE protocol_definition_id IS NULL AND facility_id IS NULL` |
| State distribution per protocol | `SELECT * FROM step_state_snapshot WHERE protocol_definition_id = ? AND facility_id IS NULL` |

> **Note:** Per-protocol and per-facility snapshots are optional extensions. The initial implementation should focus on the global snapshot only, adding protocol/facility breakdowns if the Insights Service requires them.

---

## 3. Consistency Guarantees

- **`transition_log`** — Persisted in the same database transaction as the Kafka publish success check. If the Kafka publish fails, no log entry is created (the transition didn't happen).
- **`step_state_snapshot`** — Eventually consistent (refreshed every scan cycle). The `snapshot_at` timestamp indicates data freshness.

### 3.1 Backfill Strategy

```sql
-- transition_log: cannot be fully backfilled (historical transitions were not recorded)
-- Partial backfill from step_instance timestamps:
INSERT INTO transition_log (step_instance_id, protocol_instance_id, action_id, from_state, to_state, transition_type, transitioned_at, partition_index)
SELECT
    si.id, si.protocol_instance_id, si.action_id,
    'DUE', 'OVERDUE', 'DUE_TO_OVERDUE',
    si.updated_at, 0
FROM step_instance si
WHERE si.state IN ('OVERDUE', 'MISSED')
  AND si.updated_at IS NOT NULL;

-- step_state_snapshot: full computation
INSERT INTO step_state_snapshot (protocol_definition_id, facility_id, pending_count, due_count, overdue_count, missed_count, completed_count, skipped_count, total_count)
SELECT
    NULL, NULL,
    COUNT(*) FILTER (WHERE state = 'PENDING'),
    COUNT(*) FILTER (WHERE state = 'DUE'),
    COUNT(*) FILTER (WHERE state = 'OVERDUE'),
    COUNT(*) FILTER (WHERE state = 'MISSED'),
    COUNT(*) FILTER (WHERE state = 'COMPLETED'),
    COUNT(*) FILTER (WHERE state = 'SKIPPED'),
    COUNT(*)
FROM step_instance;
```

---

## 4. Summary of Changes

| Change | Type | Table | Updated By | Trigger |
|--------|------|-------|-----------|---------|
| New `transition_log` table | Table | New | Scheduler | Each state transition |
| New `step_state_snapshot` table | Table | New | Scheduler | End of each scan cycle |

### 4.1 Flyway Migration Plan

| Order | Migration | Description |
|-------|-----------|-------------|
| V2 | `V2__create_transition_log.sql` | Create table + indexes + partial backfill |
| V3 | `V3__create_step_state_snapshot.sql` | Create table + initial snapshot |

### 4.2 Configuration Additions

| Property | Default | Description |
|----------|---------|-------------|
| `cce.scheduler.transition-log.enabled` | `true` | Enable/disable transition logging |
| `cce.scheduler.snapshot.enabled` | `true` | Enable/disable state snapshot refresh |
| `cce.scheduler.snapshot.protocol-level` | `false` | Enable per-protocol breakdowns in snapshot |
